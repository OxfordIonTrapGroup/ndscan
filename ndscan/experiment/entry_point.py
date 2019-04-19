r"""
Top-level functions for launching :class:`.ExpFragment`\ s and their scans from the rest
of the ARTIQ ``HasEnvironment`` universe.

The two main entry points into the :class:`.ExpFragment` universe are

 * scans (with axes and overrides set from the dashboard UI) via
   :meth:`make_fragment_scan_exp`, and
 * manually launched fragments from vanilla ARTIQ ``EnvExperiment``\ s using
   :meth:`run_fragment_once` or :meth:`create_and_run_fragment_once`.
"""

from artiq.language import *
from contextlib import suppress
import json
import logging
import random
from typing import Any, Callable, Dict, Iterable, Type

from .default_analysis import AnnotationContext
from .fragment import ExpFragment
from .parameters import type_string_to_param
from .result_channels import (AppendingDatasetSink, LastValueSink, ScalarDatasetSink,
                              ResultChannel)
from .scan_generator import GENERATORS, ScanOptions
from .scan_runner import (ScanAxis, ScanRunner, ScanSpec, describe_scan,
                          filter_default_analyses)
from .utils import is_kernel
from ..utils import PARAMS_ARG_KEY, shorten_to_unambiguous_suffixes

__all__ = [
    "make_fragment_scan_exp", "run_fragment_once", "create_and_run_fragment_once"
]

logger = logging.getLogger(__name__)


class ScanSpecError(Exception):
    """Raised when the scan specification passed in :data:`PARAMS_ARG_KEY` is not valid
    for the given fragment."""
    pass


class FragmentScanExperiment(EnvExperiment):
    """Implements possibly (trivial) scans of an :class:`.ExpFragment`, with overrides
    and scan axes as specified by the :data:`PARAMS_ARG_KEY` dataset, and result
    channels being broadcasted to datasets.

    See :meth:`make_fragment_scan_exp` for a convenience method to create subclasses for
    a specific :class:`.ExpFragment`.
    """
    argument_ui = "ndscan"

    def build(self, fragment_init: Callable[[], ExpFragment]):
        """
        :param fragment_init: Callable to create the top-level :meth:`ExpFragment`
            instance.
        """
        self.setattr_device("ccb")
        self.setattr_device("core")
        self.setattr_device("scheduler")

        self.fragment = fragment_init()

        instances = dict()
        self.schemata = dict()
        self.fragment._collect_params(instances, self.schemata)
        desc = {
            "instances": instances,
            "schemata": self.schemata,
            "always_shown": self.fragment._get_always_shown_params(),
            "overrides": {},
            "scan": {
                "axes": [],
                "num_repeats": 1,
                "continuous_without_axes": True,
                "randomise_order_globally": False
            }
        }
        self._params = self.get_argument(PARAMS_ARG_KEY, PYONValue(default=desc))

        self._scan_desc = None
        self._scan_axis_sinks = None
        self._scan_result_sinks = {}

    def prepare(self):
        """Collect parameters to set from both scan axes and simple overrides, and
        initialise result channels.
        """

        # Create scan and parameter overrides.
        param_stores = {}
        for fqn, specs in self._params.get("overrides", {}).items():
            store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            param_stores[fqn] = [(s["path"], store_type((fqn, s["path"]), s["value"]))
                                 for s in specs]

        scan = self._params.get("scan", {})

        generators = []
        axes = []
        for axspec in scan["axes"]:
            generator_class = GENERATORS.get(axspec["type"], None)
            if not generator_class:
                raise ScanSpecError("Axis type '{}' not implemented".format(
                    axspec["type"]))
            generator = generator_class(**axspec["range"])
            generators.append(generator)

            fqn = axspec["fqn"]
            pathspec = axspec["path"]

            store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            store = store_type((fqn, pathspec),
                               generator.points_for_level(0, random)[0])
            param_stores.setdefault(fqn, []).append((pathspec, store))
            axes.append(ScanAxis(self.schemata[fqn], pathspec, store))

        options = ScanOptions(scan.get("num_repeats", 1),
                              scan.get("continuous_without_axes", True),
                              scan.get("randomise_order_globally", False))
        self._scan = ScanSpec(axes, generators, options)

        self.fragment.init_params(param_stores)

        # Initialise result channels.
        chan_dict = {}
        self.fragment._collect_result_channels(chan_dict)

        chan_name_map = _shorten_result_channel_names(chan_dict.keys())

        self._short_child_channel_names = {}
        for path, channel in chan_dict.items():
            if not channel.save_by_default:
                continue
            name = chan_name_map[path].replace("/", "_")
            self._short_child_channel_names[channel] = name

            if self._scan.axes:
                sink = AppendingDatasetSink(self, "ndscan.points.channel_" + name)
            else:
                sink = ScalarDatasetSink(self, "ndscan.point." + name)
            channel.set_sink(sink)
            self._scan_result_sinks[channel] = sink

        self.fragment.prepare()

    def run(self):
        """Run the (possibly trivial) scan."""
        self._broadcast_metadata()
        self._issue_ccb()

        with suppress(TerminationRequested):
            if not self._scan.axes:
                self._run_single()
            else:
                runner = ScanRunner(self)
                self._scan_axis_sinks = [
                    AppendingDatasetSink(self, "ndscan.points.axis_{}".format(i))
                    for i in range(len(self._scan.axes))
                ]
                runner.run(self.fragment, self._scan, self._scan_axis_sinks)

            self._set_completed()

    def analyze(self):
        if not self._scan_axis_sinks:
            return

        analyses = filter_default_analyses(self.fragment, self._scan)
        if not analyses:
            return

        axis_data = {}
        axis_indices = {}
        for i, (axis, sink) in enumerate(zip(self._scan.axes, self._scan_axis_sinks)):
            identity = (axis.param_schema["fqn"], axis.path)
            axis_data[identity] = sink.get_all()
            axis_indices[identity] = i

        result_data = {
            chan: sink.get_all()
            for chan, sink in self._scan_result_sinks.items()
        }

        context = AnnotationContext(
            lambda handle: axis_indices[handle._store.identity], lambda channel: self.
            _short_child_channel_names[channel])

        annotations = []
        for a in analyses:
            annotations += a.execute(axis_data, result_data, context)

        if annotations:
            # Replace existing (online-fit) annotations if any analysis produced custom
            # ones. This could be made configurable in the future.
            self.set_dataset("ndscan.annotations",
                             json.dumps(annotations),
                             broadcast=True)

    def _run_single(self):
        try:
            with suppress(TerminationRequested):
                while True:
                    self.fragment.host_setup()
                    if is_kernel(self.fragment.run_once):
                        self._run_continuous_kernel()
                        self.core.comm.close()
                    else:
                        self._continuous_loop()
                    if not self._scan.options.continuous_without_axes:
                        return
                    self.scheduler.pause()
        finally:
            self._set_completed()

    @kernel
    def _run_continuous_kernel(self):
        self.core.reset()
        self._continuous_loop()

    @portable
    def _continuous_loop(self):
        while not self.scheduler.check_pause():
            self.fragment.device_setup()
            self.fragment.run_once()
            if not self._scan.options.continuous_without_axes:
                return

    def _set_completed(self):
        self.set_dataset("ndscan.completed", True, broadcast=True)

    def _broadcast_metadata(self):
        def push(name, value):
            self.set_dataset("ndscan." + name, value, broadcast=True)

        push("rid", self.scheduler.rid)
        push("completed", False)

        self._scan_desc = describe_scan(self._scan, self.fragment,
                                        self._short_child_channel_names)
        for name, value in self._scan_desc.items():
            # Flatten arrays/dictionaries to JSON strings for HDF5 compatibility.
            if isinstance(value, str) or isinstance(value, int):
                push(name, value)
            else:
                push(name, json.dumps(value))

    def _issue_ccb(self):
        cmd = ("${python} -m ndscan.applet "
               "--server=${server} "
               "--port=${port_notify} "
               "--port-control=${port_control}")
        cmd += " --rid={}".format(self.scheduler.rid)
        self.ccb.issue("create_applet",
                       "ndscan: " + self.fragment.fqn,
                       cmd,
                       group="ndscan",
                       is_transient=True)


def _shorten_result_channel_names(full_names: Iterable[str]) -> Dict[str, str]:
    return shorten_to_unambiguous_suffixes(
        full_names, lambda fqn, n: "/".join(fqn.split("/")[-n:]))


def make_fragment_scan_exp(fragment_class: Type[ExpFragment]
                           ) -> Type[FragmentScanExperiment]:
    """Create a :class:`FragmentScanExperiment` subclass that scans the given
    :class:`.ExpFragment`, ready to be picked up by the ARTIQ explorer/…

    This is the default way of creating scan experiments::

        class MyExpFragment(ExpFragment):
            def build_fragment(self):
                # ...

            def run_once(self):
                # ...

        MyExpFragmentScan = make_fragment_scan_exp(MyExpFragment)
    """

    class FragmentScanShim(FragmentScanExperiment):
        def build(self):
            super().build(lambda: fragment_class(self, []))

    # Take on the name of the fragment class to keep result file names informative.
    FragmentScanShim.__name__ = fragment_class.__name__

    # Use the fragment class docstring to display in the experiment explorer UI.
    FragmentScanShim.__doc__ = fragment_class.__doc__

    return FragmentScanShim


def run_fragment_once(fragment: ExpFragment) -> Dict[ResultChannel, Any]:
    """Initialise the passed fragment and run it once, capturing and returning the
    values from any result channels.

    :return: A dictionary mapping :class:`ResultChannel` instances to their values
        (or ``None`` if not pushed to).
    """

    channel_dict = {}
    fragment._collect_result_channels(channel_dict)
    sinks = {channel: LastValueSink() for channel in channel_dict.values()}
    for channel, sink in sinks.items():
        channel.set_sink(sink)

    fragment.init_params()
    fragment.prepare()
    fragment.host_setup()
    if is_kernel(fragment.run_once):
        # Run device_setup()/run_once() in a single kernel invocation.
        class FragmentRunner(HasEnvironment):
            def build(self, fragment):
                self.setattr_device("core")
                self.fragment = fragment

            @kernel
            def run(self):
                self.fragment.device_setup()
                self.fragment.run_once()

        FragmentRunner(fragment, fragment).run()
    else:
        fragment.device_setup()
        fragment.run_once()

    return {channel: sink.get_last() for channel, sink in sinks.items()}


def create_and_run_fragment_once(env: HasEnvironment, fragment_class: Type[ExpFragment],
                                 *args, **kwargs) -> Dict[str, Any]:
    """Create an instance of the passed :class:`.ExpFragment` type and runs it once,
    returning the values pushed to any result channels.

    Example::

        class MyExpFragment(ExpFragment):
            def build_fragment(self):
                # ...
                self.setattr_result("foo")

            def run_once(self):
                # ...

        class MyEnvExperiment(EnvExperiment):
            def run(self):
                results = create_and_run_once(self, MyExpFragment)
                print(results["foo"])

    :param env: The ``HasEnvironment`` to use.
    :param fragment_class: The :class:`.ExpFragment` class to instantiate.
    :param args: Any arguments to forward to ``build_fragment()``.
    :param kwargs: Any keyword arguments to forward to ``build_fragment()``.
    :return: A dictionary mapping result channel names to their values (or ``None`` if
        not pushed to).
    """
    results = run_fragment_once(fragment_class(env, [], *args, **kwargs))
    shortened_names = _shorten_result_channel_names(channel.path
                                                    for channel in results.keys())
    return {shortened_names[channel.path]: value for channel, value in results.items()}

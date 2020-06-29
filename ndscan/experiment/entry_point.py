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
from artiq.coredevice.exceptions import RTIOUnderflow
from collections import OrderedDict
from contextlib import suppress
import json
import logging
import random
import time
from typing import Any, Callable, Dict, Iterable, Type

from .default_analysis import AnnotationContext
from .fragment import ExpFragment, RestartKernelTransitoryError, TransitoryError
from .parameters import type_string_to_param
from .result_channels import (AppendingDatasetSink, LastValueSink, ScalarDatasetSink,
                              ResultChannel)
from .scan_generator import GENERATORS, ScanOptions
from .scan_runner import (ScanAxis, ScanRunner, ScanSpec, describe_scan,
                          filter_default_analyses)
from .utils import is_kernel
from ..utils import NoAxesMode, PARAMS_ARG_KEY, shorten_to_unambiguous_suffixes

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

    def build(self,
              fragment_init: Callable[[], ExpFragment],
              max_rtio_underflow_retries: int = 3,
              max_transitory_error_retries: int = 10):
        """
        :param fragment_init: Callable to create the top-level :meth:`ExpFragment`
            instance.
        :param max_rtio_underflow_retries: Number of RTIOUnderflows to tolerate per scan
            point (by simply trying again) before giving up.
        :param max_transitory_error_retries: Number of transitory errors to tolerate per
            scan point (by simply trying again) before giving up.
        """
        self.fragment = fragment_init()
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries

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
                "no_axes_mode": "single",
                "randomise_order_globally": False
            }
        }
        self._params = self.get_argument(PARAMS_ARG_KEY, PYONValue(default=desc))

    def prepare(self):
        """Collect parameters to set from both scan axes and simple overrides, and
        initialise result channels.
        """

        # Create scan and parameter overrides.
        param_stores = {}
        for fqn, specs in self._params.get("overrides", {}).items():
            try:
                store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            except KeyError:
                raise KeyError("Parameter schema not found (likely due to outdated "
                               "argument editor after changes to experiment; "
                               "try Recompute All Arguments)")

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
        self.fragment.init_params(param_stores)

        options = ScanOptions(scan.get("num_repeats", 1),
                              scan.get("randomise_order_globally", False))
        no_axes_mode = NoAxesMode[scan.get("no_axes_mode", "single")]
        spec = ScanSpec(axes, generators, options)
        self.tlr = TopLevelRunner(self, self.fragment, spec, no_axes_mode,
                                  self.max_rtio_underflow_retries,
                                  self.max_transitory_error_retries)

    def run(self):
        self.tlr.create_applet(title="ndscan: " + self.fragment.fqn)
        with suppress(TerminationRequested):
            self.tlr.run()

    def analyze(self):
        self.tlr.analyze()


class TopLevelRunner(HasEnvironment):
    def build(self,
              fragment: ExpFragment,
              spec: ScanSpec,
              no_axes_mode: NoAxesMode = NoAxesMode.single,
              max_rtio_underflow_retries: int = 3,
              max_transitory_error_retries: int = 10,
              dataset_prefix: str = "ndscan."):
        self.fragment = fragment
        self.spec = spec
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.dataset_prefix = dataset_prefix

        self.setattr_device("ccb")
        self.setattr_device("core")
        self.setattr_device("scheduler")

        # FIXME: We save these as individual booleans as enums crash the ARTIQ compiler.
        self._continue_running = False
        self._is_time_series = False

        if not self.spec.axes:
            self._continue_running = no_axes_mode != NoAxesMode.single
            if no_axes_mode == NoAxesMode.time_series:
                self._is_time_series = True
                param_schema = {
                    "type": "float",
                    "fqn": "timestamp",
                    "description": "Elapsed time",
                    "default": "0.0",
                    "spec": {
                        "min": 0.0,
                        "unit": "s",
                        "scale": 1.0
                    },
                }
                self.spec.axes = [ScanAxis(param_schema, "*", None)]

        # Initialise result channels.
        chan_dict = {}
        self.fragment._collect_result_channels(chan_dict)

        chan_name_map = _shorten_result_channel_names(chan_dict.keys())

        self._scan_result_sinks = {}
        self._short_child_channel_names = {}
        for path, channel in chan_dict.items():
            if not channel.save_by_default:
                continue
            name = chan_name_map[path].replace("/", "_")
            self._short_child_channel_names[channel] = name

            if self.spec.axes:
                sink = AppendingDatasetSink(
                    self, self.dataset_prefix + "points.channel_" + name)
            else:
                sink = ScalarDatasetSink(self, self.dataset_prefix + "point." + name)
            channel.set_sink(sink)
            self._scan_result_sinks[channel] = sink

        self.fragment.prepare()

    def run(self):
        """Run the (possibly trivial) scan."""
        self._broadcast_metadata()

        if not self.spec.axes and not self._is_time_series:
            self._run_continuous()
            return None, {c: s.get_last() for c, s in self._scan_result_sinks.items()}

        coordinate_sinks = None
        if self._is_time_series:
            self._timestamp_sink = AppendingDatasetSink(
                self, self.dataset_prefix + "points.axis_0")
            coordinate_sinks = [self._timestamp_sink]
            self._time_series_start = time.monotonic()
            self._run_continuous()
        else:
            runner = ScanRunner(
                self,
                max_rtio_underflow_retries=self.max_rtio_underflow_retries,
                max_transitory_error_retries=self.max_transitory_error_retries)
            coordinate_sinks = [
                AppendingDatasetSink(self,
                                     self.dataset_prefix + "points.axis_{}".format(i))
                for i in range(len(self.spec.axes))
            ]
            runner.run(self.fragment, self.spec, coordinate_sinks)
            self._set_completed()

        self._coordinate_data = OrderedDict(
            ((a.param_schema["fqn"], a.path), s.get_all())
            for a, s in zip(self.spec.axes, coordinate_sinks))
        self._value_data = {c: s.get_all() for c, s in self._scan_result_sinks.items()}
        return self._coordinate_data, self._value_data

    def analyze(self):
        if not self.spec.axes:
            # Return if there are no scan axes - could allow the time series fake axis
            # in the future.
            return

        analyses = filter_default_analyses(self.fragment, self.spec)
        if not analyses:
            return

        axis_indices = {}
        for i, axis in enumerate(self.spec.axes):
            axis_indices[(axis.param_schema["fqn"], axis.path)] = i

        context = AnnotationContext(
            lambda handle: axis_indices[handle._store.identity],
            lambda channel: self._short_child_channel_names[channel])

        annotations = []
        for a in analyses:
            annotations += a.execute(self._coordinate_data, self._value_data, context)

        if annotations:
            # Replace existing (online-fit) annotations if any analysis produced custom
            # ones. This could be made configurable in the future.
            self.set_dataset(self.dataset_prefix + "annotations",
                             json.dumps(annotations),
                             broadcast=True)

    def _run_continuous(self):
        try:
            with suppress(TerminationRequested):
                while True:
                    try:
                        self.fragment.host_setup()
                        self._point_phase = False
                        if is_kernel(self.fragment.run_once):
                            self._run_continuous_kernel()
                            self.core.comm.close()
                        else:
                            self._continuous_loop()
                    finally:
                        self.fragment.host_cleanup()
                    if not self._continue_running:
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
        try:
            while not self.scheduler.check_pause():
                self.fragment.device_setup()
                self.fragment.run_once()
                self._finish_continuous_point()
                if not self._continue_running:
                    return
        finally:
            self.fragment.device_cleanup()

    @rpc(flags={"async"})
    def _finish_continuous_point(self):
        self._point_phase = not self._point_phase
        self.set_dataset(self.dataset_prefix + "point_phase",
                         self._point_phase,
                         broadcast=True)
        if self._is_time_series:
            self._timestamp_sink.push(time.monotonic() - self._time_series_start)

    def _set_completed(self):
        self.set_dataset(self.dataset_prefix + "completed", True, broadcast=True)

    def _broadcast_metadata(self):
        def push(name, value):
            self.set_dataset(self.dataset_prefix + name, value, broadcast=True)

        source_prefix = self.get_dataset("system_id", default="rid")
        push("source_id", "{}_{}".format(source_prefix, self.scheduler.rid))

        push("completed", False)

        self._scan_desc = describe_scan(self.spec, self.fragment,
                                        self._short_child_channel_names)
        for name, value in self._scan_desc.items():
            # Flatten arrays/dictionaries to JSON strings for HDF5 compatibility.
            if isinstance(value, str) or isinstance(value, int):
                push(name, value)
            else:
                push(name, json.dumps(value))

    def create_applet(self, title: str, group: str = "ndscan"):
        cmd = ("${python} -m ndscan.applet "
               "--server=${server} "
               "--port=${port_notify} "
               "--port-control=${port_control}")
        cmd += " --rid={}".format(self.scheduler.rid)
        if self.dataset_prefix != "ndscan.":
            cmd += " --prefix={}".format(self.dataset_prefix)
        self.ccb.issue("create_applet", title, cmd, group=group, is_transient=True)


def _shorten_result_channel_names(full_names: Iterable[str]) -> Dict[str, str]:
    return shorten_to_unambiguous_suffixes(full_names,
                                           lambda fqn, n: "/".join(fqn.split("/")[-n:]))


def make_fragment_scan_exp(
        fragment_class: Type[ExpFragment],
        *args,
        max_rtio_underflow_retries: int = 3,
        max_transitory_error_retries: int = 10) -> Type[FragmentScanExperiment]:
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
            super().build(lambda: fragment_class(self, [], *args),
                          max_rtio_underflow_retries=max_rtio_underflow_retries,
                          max_transitory_error_retries=max_transitory_error_retries)

    # Take on the name of the fragment class to keep result file names informative.
    FragmentScanShim.__name__ = fragment_class.__name__

    # Use the fragment class docstring to display in the experiment explorer UI.
    FragmentScanShim.__doc__ = fragment_class.__doc__

    return FragmentScanShim


class _FragmentRunner(HasEnvironment):
    """Object wrapping fragment execution to be able to execute everything in one kernel
    invocation (no difference for non-kernel fragments).
    """
    def build(self, fragment: ExpFragment, max_rtio_underflow_retries: int,
              max_transitory_error_retries: int):
        self.fragment = fragment
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.num_underflows_caught = 0
        self.num_transitory_errors_caught = 0

    def run(self) -> bool:
        """Execute device_setup()/run_once(), retrying if nececssary.

        :return: ``True`` if execution completed, ``False`` if it should be attempted
            again (RestartKernelTransitoryError).
        """
        if is_kernel(self.fragment.run_once):
            self.setattr_device("core")
            return self._run_on_kernel()
        else:
            return self._run()

    @kernel
    def _run_on_kernel(self):
        """Force the portable _run() to run on the kernel."""
        return self._run()

    @portable
    def _run(self):
        try:
            while True:
                try:
                    self.fragment.device_setup()
                    self.fragment.run_once()
                    return True
                except RTIOUnderflow:
                    self.num_underflows_caught += 1
                    if self.num_underflows_caught > self.max_rtio_underflow_retries:
                        raise
                    print("Ignoring RTIOUnderflow (", self.num_underflows_caught, "/",
                          self.max_rtio_underflow_retries, ")")
                except RestartKernelTransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
                    print("Caught transitory error, restarting kernel")
                    return False
                except TransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
                    print("Caught transitory error (",
                          self.num_transitory_errors_caught, "/",
                          self.max_transitory_error_retries, "), retrying")
        finally:
            self.fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True


def run_fragment_once(
    fragment: ExpFragment,
    max_rtio_underflow_retries: int = 3,
    max_transitory_error_retries: int = 10,
) -> Dict[ResultChannel, Any]:
    """Initialise the passed fragment and run it once, capturing and returning the
    values from any result channels.

    :param max_transitory_error_retries: Number of times to catch transitory error
        exceptions and retry execution. If exceeded, the exception is re-raised for
        the caller to handle. If ``0``, retrying is disabled entirely.

    :return: A dictionary mapping :class:`ResultChannel` instances to their values
        (or ``None`` if not pushed to).
    """

    channel_dict = {}
    fragment._collect_result_channels(channel_dict)
    sinks = {channel: LastValueSink() for channel in channel_dict.values()}
    for channel, sink in sinks.items():
        channel.set_sink(sink)

    runner = _FragmentRunner(fragment, fragment, max_rtio_underflow_retries,
                             max_transitory_error_retries)
    fragment.init_params()
    fragment.prepare()
    try:
        while True:
            fragment.host_setup()
            if runner.run():
                break
    finally:
        fragment.host_cleanup()

    return {channel: sink.get_last() for channel, sink in sinks.items()}


def create_and_run_fragment_once(env: HasEnvironment,
                                 fragment_class: Type[ExpFragment],
                                 max_rtio_underflow_retries: int = 3,
                                 max_transitory_error_retries: int = 10,
                                 *args,
                                 **kwargs) -> Dict[str, Any]:
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
    results = run_fragment_once(
        fragment_class(env, [], *args, **kwargs),
        max_rtio_underflow_retries=max_rtio_underflow_retries,
        max_transitory_error_retries=max_transitory_error_retries)
    shortened_names = _shorten_result_channel_names(channel.path
                                                    for channel in results.keys())
    return {shortened_names[channel.path]: value for channel, value in results.items()}

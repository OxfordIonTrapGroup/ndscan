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
from functools import reduce
import logging
import random
import time
from typing import Any, Callable, Dict, Iterable, List, Tuple, Type

from .default_analysis import AnnotationContext
from .fragment import (ExpFragment, Fragment, RestartKernelTransitoryError,
                       TransitoryError)
from .parameters import ParamStore, type_string_to_param
from .result_channels import (AppendingDatasetSink, LastValueSink, ScalarDatasetSink,
                              ResultChannel)
from .scan_generator import GENERATORS, ScanOptions
from .scan_runner import (ScanAxis, ScanRunner, ScanSpec, describe_scan,
                          describe_analyses, filter_default_analyses)
from .utils import dump_json, is_kernel, to_metadata_broadcast_type
from ..utils import (merge_no_duplicates, NoAxesMode, PARAMS_ARG_KEY, SCHEMA_REVISION,
                     SCHEMA_REVISION_KEY, shorten_to_unambiguous_suffixes)

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

        self.args = ArgumentInterface(self, [self.fragment])

    def prepare(self):
        """Collect parameters to set from both scan axes and simple overrides, and
        initialise result channels.
        """
        param_stores = self.args.make_override_stores()

        spec, no_axes_mode = self.args.make_scan_spec()
        for ax in spec.axes:
            fqn = ax.param_schema["fqn"]
            param_stores.setdefault(fqn, []).append((ax.path, ax.param_store))

        self.fragment.init_params(param_stores)
        self.tlr = TopLevelRunner(self, self.fragment, spec, no_axes_mode,
                                  self.max_rtio_underflow_retries,
                                  self.max_transitory_error_retries)

    def run(self):
        self.tlr.create_applet(title="ndscan: " + self.fragment.fqn)
        with suppress(TerminationRequested):
            self.tlr.run()

    def analyze(self):
        self.tlr.analyze()


class ArgumentInterface(HasEnvironment):
    def build(self, fragments: List[Fragment]) -> None:
        self._fragments = fragments

        instances = dict()
        self._schemata = dict()
        always_shown_params = []
        for fragment in fragments:
            fragment._collect_params(instances, self._schemata)
            always_shown_params += fragment._get_always_shown_params()
        desc = {
            "instances": instances,
            "schemata": self._schemata,
            "always_shown": always_shown_params,
            "overrides": {},
            "scan": {
                "axes": [],
                "num_repeats": 1,
                "no_axes_mode": "single",
                "randomise_order_globally": False
            }
        }
        self._params = self.get_argument(PARAMS_ARG_KEY, PYONValue(default=desc))

    def make_override_stores(self) -> Dict[str, Tuple[str, ParamStore]]:
        stores = {}
        for fqn, specs in self._params.get("overrides", {}).items():
            try:
                store_type = type_string_to_param(self._schemata[fqn]["type"]).StoreType
            except KeyError:
                raise KeyError("Parameter schema not found (likely due to outdated "
                               "argument editor after changes to experiment; "
                               "try Recompute All Arguments)")

            stores[fqn] = [(s["path"], store_type((fqn, s["path"]), s["value"]))
                           for s in specs]
        return stores

    def make_scan_spec(self) -> Tuple[ScanSpec, NoAxesMode]:
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

            store_type = type_string_to_param(self._schemata[fqn]["type"]).StoreType
            store = store_type((fqn, pathspec),
                               generator.points_for_level(0, random)[0])
            axes.append(ScanAxis(self._schemata[fqn], pathspec, store))

        options = ScanOptions(scan.get("num_repeats", 1),
                              scan.get("randomise_order_globally", False))
        no_axes_mode = NoAxesMode[scan.get("no_axes_mode", "single")]
        spec = ScanSpec(axes, generators, options)
        return spec, no_axes_mode


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

        if dataset_prefix and dataset_prefix[-1] != ".":
            # Add trailing dot to dataset prefix if not given – the same bare prefix
            # mushed into all the ndscan datasets isn't what a user with an intact sense
            # of style would intend.
            dataset_prefix += "."
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

        # Filter analyses, set up analysis result channels, and keep track of all the
        # names in the annotation context.
        self._analyses = filter_default_analyses(self.fragment, self.spec.axes)

        self._analysis_results = reduce(
            lambda l, r: merge_no_duplicates(l, r, kind="analysis result"),
            (a.get_analysis_results() for a in self._analyses), {})
        for name, channel in self._analysis_results.items():
            channel.set_sink(
                ScalarDatasetSink(self,
                                  self.dataset_prefix + "analysis_result." + name))

        axis_indices = {}
        for i, axis in enumerate(self.spec.axes):
            axis_indices[(axis.param_schema["fqn"], axis.path)] = i
        self._annotation_context = AnnotationContext(
            lambda handle: axis_indices[handle._store.identity],
            lambda channel: self._short_child_channel_names[channel],
            lambda channel: True)

        self._coordinate_sinks = None

        self.fragment.prepare()

    def run(self):
        """Run the (possibly trivial) scan."""
        self._broadcast_metadata()

        if not self.spec.axes and not self._is_time_series:
            self._run_continuous()
            return None, {c: s.get_last() for c, s in self._scan_result_sinks.items()}

        if self._is_time_series:
            self._timestamp_sink = AppendingDatasetSink(
                self, self.dataset_prefix + "points.axis_0")
            self._coordinate_sinks = [self._timestamp_sink]
            self._time_series_start = time.monotonic()
            self._run_continuous()
        else:
            runner = ScanRunner(
                self,
                max_rtio_underflow_retries=self.max_rtio_underflow_retries,
                max_transitory_error_retries=self.max_transitory_error_retries)
            self._coordinate_sinks = [
                AppendingDatasetSink(self,
                                     self.dataset_prefix + "points.axis_{}".format(i))
                for i in range(len(self.spec.axes))
            ]
            runner.run(self.fragment, self.spec, self._coordinate_sinks)
            self._set_completed()

        return self._make_coordinate_dict(), self._make_value_dict()

    def _make_coordinate_dict(self):
        return OrderedDict(((a.param_schema["fqn"], a.path), s.get_all())
                           for a, s in zip(self.spec.axes, self._coordinate_sinks))

    def _make_value_dict(self):
        return {c: s.get_all() for c, s in self._scan_result_sinks.items()}

    def analyze(self):
        if self._coordinate_sinks is None:
            # Continuous scan or got an exception early on, so there is no data to
            # analyse – gracefully ignore this to keep FragmentScanExperiment
            # implementation simple.
            return
        if not self._analyses:
            return

        annotations = []
        coordinates = self._make_coordinate_dict()
        values = self._make_value_dict()
        for a in self._analyses:
            annotations += a.execute(coordinates, values, self._annotation_context)

        if annotations:
            # Replace existing (online-fit) annotations if any analysis produced custom
            # ones. This could be made configurable in the future.
            self.set_dataset(self.dataset_prefix + "annotations",
                             dump_json(annotations),
                             broadcast=True)

        return {
            name: channel.sink.get_last()
            for name, channel in self._analysis_results.items()
        }

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

        push(SCHEMA_REVISION_KEY, SCHEMA_REVISION)

        source_prefix = self.get_dataset("system_id", default="rid")
        push("source_id", "{}_{}".format(source_prefix, self.scheduler.rid))

        push("completed", False)

        self._scan_desc = describe_scan(self.spec, self.fragment,
                                        self._short_child_channel_names)
        self._scan_desc.update(
            describe_analyses(self._analyses, self._annotation_context))
        self._scan_desc["analysis_results"] = {
            name: channel.describe()
            for name, channel in self._analysis_results.items()
        }

        for name, value in self._scan_desc.items():
            # Flatten arrays/dictionaries to JSON strings for HDF5 compatibility.
            ds_value = to_metadata_broadcast_type(value)
            if ds_value is None:
                push(name, dump_json(value))
            else:
                push(name, ds_value)

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

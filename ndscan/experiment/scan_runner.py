"""Generic scanning loop.

While :mod:`.scan_generator` describes a scan to be run in the abstract, this module
contains the implementation to actually execute one within an ARTIQ experiment. This
will likely be used by end users via
:class:`~ndscan.experiment.entry_point.FragmentScanExperiment` or subscans.
"""

import numpy as np
from artiq.coredevice.exceptions import RTIOUnderflow
from artiq.language import *
from itertools import islice
from typing import Any, Dict, List, Iterable, Iterator, Tuple
from .default_analysis import AnnotationContext, DefaultAnalysis
from .fragment import ExpFragment, TransitoryError, RestartKernelTransitoryError
from .parameters import ParamStore, type_string_to_param
from .result_channels import ResultChannel, ResultSink
from .scan_generator import generate_points, ScanGenerator, ScanOptions
from .utils import is_kernel, make_coordinate_dict, make_value_dict

__all__ = [
    "ScanAxis",
    "ScanSpec",
    "ScanRunner",
    "filter_default_analyses",
    "describe_scan",
    "describe_analyses",
]


class ScanAxis:
    """Describes a single axis that is being scanned.

    Apart from the metadata, this also includes the necessary information to execute the
    scan at runtime; i.e. the :class:`.ParamStore` to modify in order to set the
    parameter.
    """

    def __init__(
        self, param_schema: Dict[str, Any], path: str, param_store: ParamStore
    ):
        self.param_schema = param_schema
        self.path = path
        self.param_store = param_store


class ScanSpec:
    """Describes a single scan.

    :param axes: The list of parameters that are scanned.
    :param generators: Generators that give the points for each of the specified axes.
    :param options: Applicable :class:`.ScanOptions`.
    """

    def __init__(
        self,
        axes: List[ScanAxis],
        generators: List[ScanGenerator],
        options: ScanOptions,
    ):
        self.axes = axes
        self.generators = generators
        self.options = options


class ScanRunner(HasEnvironment):
    """Runs the actual loop that executes an :class:`.ExpFragment` for a specified list
    of scan axes (on either the host or core device, as appropriate).

    Integrates with the ARTIQ scheduler to pause/terminate execution as requested.

    Conceptually, this is only a single function (``run()``), but is wrapped in a class
    to follow the idiomatic ARTIQ kernel/``HasEnvironment`` integration style.
    """

    # Note: ARTIQ Python is currently severely limited in its support for generics or
    # metaprogramming. While the interface for this class is effortlessly generic, the
    # implementation might well be a long-forgotten ritual for invoking Cthulhu.

    def build(
        self,
        max_rtio_underflow_retries: int = 3,
        max_transitory_error_retries: int = 10,
    ):
        """
        :param max_rtio_underflow_retries: Number of RTIOUnderflows to tolerate per scan
            point (by simply trying again) before giving up.
        :param max_transitory_error_retries: Number of transitory errors to tolerate per
            scan point (by simply trying again) before giving up.
        """
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.setattr_device("core")
        self.setattr_device("scheduler")

    def run(
        self, fragment: ExpFragment, spec: ScanSpec, axis_sinks: List[ResultSink], scan_result_sinks: dict,
    ) -> None:
        """Run a scan of the given fragment, with axes as specified.

        :param fragment: The fragment to iterate.
        :param options: The options for the scan generator.
        :param axis_sinks: A list of :class:`.ResultSink` instances to push the
            coordinates for each scan point to, matching ``scan.axes``.
        """

        points = generate_points(spec.generators, spec.options)

        # TODO: Support parameters which require host_setup() when changed.
        run_impl = (
            self._run_scan_on_core_device
            if is_kernel(fragment.run_once)
            else self._run_scan_on_host
        )
        self.live_analysis_runner = LiveAnalysisRunner(
            fragment, spec.axes, axis_sinks, scan_result_sinks
        )
        run_impl(fragment, points, spec.axes, axis_sinks)

    def _run_scan_on_host(
        self,
        fragment: ExpFragment,
        points: Iterator[Tuple],
        axes: List[ScanAxis],
        axis_sinks: List[ResultSink],
    ) -> None:
        while True:
            # After every pause(), pull in dataset changes (immediately as well to catch
            # changes between the time the experiment is prepared and when it is run, to
            # keep the semantics uniform).
            fragment.recompute_param_defaults()
            try:
                fragment.host_setup()
                try:
                    while True:
                        axis_values = next(points, None)
                        if axis_values is None:
                            return
                        for (axis, value, sink) in zip(axes, axis_values, axis_sinks):
                            axis.param_store.set_value(value)
                            sink.push(value)
                        fragment.device_setup()
                        fragment.run_once()
                        self.live_analysis_runner.live_analyses()
                        if self.scheduler.check_pause():
                            break
                finally:
                    fragment.device_cleanup()
            finally:
                fragment.host_cleanup()
            # Host-only scans don't necessarily make a connection to the core device,
            # but we close any connection that was made here anyway in case the user
            # forgot to do so elsewhere. Otherwise, the connection might fail after the
            # pause. artiq.sim.devices.Core and other dummy core devices that could very
            # reasonably be used with a host-only scan don't have a close() method,
            # though, so check for its existence first.
            if hasattr(self.core, "close"):
                self.core.close()
            self.scheduler.pause()

    def _run_scan_on_core_device(
        self,
        fragment: ExpFragment,
        points: list,
        axes: List[ScanAxis],
        axis_sinks: List[ResultSink],
    ) -> None:
        # Stash away fragment in member variable to pacify ARTIQ compiler; there is no
        # reason this shouldn't just be passed along and materialised as a global.
        self._kscan_fragment = fragment

        # Set up members to be accessed from the kernel through the
        # _kscan_param_values_chunk RPC call later.
        self._kscan_points = points
        self._kscan_axes = axes
        self._kscan_axis_sinks = axis_sinks

        # Stash away points in current kernel chunk until they have been marked
        # complete so we can resume from interruptions.
        self._kscan_current_chunk = []

        # Interval between scheduler.check_pause() calls on the core device (or rather,
        # the minimum interval; calls are only made after a point has been completed).
        self._kscan_pause_check_interval_mu = self.core.seconds_to_mu(0.2)
        self._kscan_last_pause_check_mu = np.int64(0)

        # _kscan_param_values_chunk returns a tuple of lists of values, one for each
        # scan axis. Synthesize a return type annotation (`def foo(self): -> …`) with
        # the concrete type for this scan so the compiler can infer the types in
        # run_chunk() correctly.
        self._kscan_param_values_chunk.__func__.__annotations__ = {
            "return": TTuple(
                [
                    TList(type_string_to_param(a.param_schema["type"]).CompilerType)
                    for a in axes
                ]
            )
        }

        # Build kernel function that calls _kscan_param_values_chunk() and iterates over
        # the returned values, assigning them to the respective parameter stores and
        # calling _kscan_run_point() for each.
        #
        # Currently, this can't be expressed as generic code, as there is no way to
        # express indexing or deconstructing a tuple of values of inhomogeneous types
        # without actually writing it out as an assignment from a tuple value.
        for i, axis in enumerate(axes):
            setattr(
                self, "_kscan_param_setter_{}".format(i), axis.param_store.set_value
            )
        run_chunk = self._build_kscan_run_chunk(len(axes))

        self._kscan_update_host_param_stores()
        while True:
            # After every pause(), pull in dataset changes (immediately as well to catch
            # changes between the time the experiment is prepared and when it is run, to
            # keep the semantics uniform).
            self._kscan_fragment.recompute_param_defaults()
            try:
                self._kscan_fragment.host_setup()
                self._kscan_run_loop(run_chunk)
                if self._kscan_is_out_of_points():
                    # No more points; finished successfully.
                    return
            finally:
                self._kscan_fragment.host_cleanup()
                self.core.comm.close()
            self.scheduler.pause()

    def _build_kscan_run_chunk(self, num_axes):
        param_decl = " ".join("p{0},".format(idx) for idx in range(num_axes))
        code = ""
        code += "({}) = self._kscan_param_values_chunk()\n".format(param_decl)
        code += "if not p0:\n"  # No more points
        code += "    return True\n"
        code += "for i in range(len(p0)):\n"
        for idx in range(num_axes):
            code += "    self._kscan_param_setter_{0}(p{0}[i])\n".format(idx)
        code += "    if self._kscan_run_point():\n"
        code += "        return True\n"
        code += "return False"
        return kernel_from_string(["self"], code)

    @kernel
    def _kscan_run_loop(self, run_chunk):
        try:
            self._kscan_last_pause_check_mu = self.core.get_rtio_counter_mu()
            while True:
                # Fetch chunk in separate function to make sure stack memory is released
                # every time.
                if run_chunk(self):
                    return
        finally:
            self._kscan_fragment.device_cleanup()

    @kernel
    def _kscan_run_point(self) -> TBool:
        """Execute the fragment for a single point (with the currently set parameters).

        :return: Whether the kernel should be exited/experiment should be paused before
            continuing (``True`` to pause, ``False`` to continue immediately).
        """
        num_underflows = 0
        num_transitory_errors = 0
        while True:
            if self._kscan_should_pause():
                return True
            try:
                self._fragment.device_setup()
                self._fragment.run_once()
                self.live_analysis_runner.live_analyses()
                break
            except RTIOUnderflow:
                # For the first two underflows per point, just print a warning and carry
                # on (3 is a pretty arbitrary limit – we don't want to block forever in
                # case the experiment is faulty, but also want to tolerate ~1% underflow
                # chance for experiments where timing is critical).
                if num_underflows >= self.max_rtio_underflow_retries:
                    raise
                num_underflows += 1
                print(
                    "Ignoring RTIOUnderflow (",
                    num_underflows,
                    "/",
                    self.max_rtio_underflow_retries,
                    ")",
                )
                self._kscan_retry_point()
            except RestartKernelTransitoryError:
                print("Caught transitory error, restarting kernel")
                self._kscan_retry_point()
                return True
            except TransitoryError:
                if num_transitory_errors >= self.max_transitory_error_retries:
                    raise
                num_transitory_errors += 1
                print(
                    "Caught transitory error (",
                    num_transitory_errors,
                    "/",
                    self.max_transitory_error_retries,
                    "), retrying",
                )
                self._kscan_retry_point()
        self._kscan_point_completed()
        return False

    @kernel
    def _kscan_should_pause(self) -> TBool:
        current_time_mu = self.core.get_rtio_counter_mu()
        if (
            current_time_mu - self._kscan_last_pause_check_mu
            > self._kscan_pause_check_interval_mu
        ):
            self._kscan_last_pause_check_mu = current_time_mu
            if self.scheduler.check_pause():
                return True
        return False

    def _kscan_param_values_chunk(self):
        # Number of scan points to send at once. After each chunk, the kernel needs to
        # execute a blocking RPC to fetch new points, so this should be chosen such
        # that latency/constant overhead and throughput are balanced. 10 is an arbitrary
        # choice based on the observation that even for fast experiments, 10 points take
        # a good fraction of a second, while it is still low enough not to run into any
        # memory management issues on the kernel.
        CHUNK_SIZE = 10

        self._kscan_current_chunk.extend(
            islice(self._kscan_points, CHUNK_SIZE - len(self._kscan_current_chunk))
        )

        values = tuple([] for _ in self._kscan_axes)
        for p in self._kscan_current_chunk:
            for i, (value, axis) in enumerate(zip(p, self._kscan_axes)):
                # KLUDGE: Explicitly coerce value to the target type here so we can use
                # the regular (float) scans for integers until proper support for int
                # scans is implemented.
                values[i].append(axis.param_store.coerce(value))
        return values

    @rpc(flags={"async"})
    def _kscan_retry_point(self):
        # TODO: Ensure any values pushed to result channels in this iteration are
        # discarded. For this, we'll need to make ScanRunner aware of the result channel
        # sinks (not just the axis sinks), or "rebind" the fragment's result channels
        # to intercept values locally and only forward them to the real sinks in
        # _kscan_point_completed.
        pass

    @rpc(flags={"async"})
    def _kscan_point_completed(self):
        values = self._kscan_current_chunk.pop(0)
        for value, sink in zip(values, self._kscan_axis_sinks):
            sink.push(value)

        # TODO: Warn if some result channels have not been pushed to.

        self._kscan_update_host_param_stores()

    @host_only
    def _kscan_update_host_param_stores(self):
        """Set host-side parameter stores for the scan axes to their current values,
        i.e. as specified by the next point in the current scan chunk.

        This ensures that if a parameter is scanned from a kernel scan that requires
        a host RPC to update (e.g. a non-@kernel device_setup()), the RPC'd code will
        execute using the expected values.
        """

        if self._kscan_is_out_of_points():
            return
        # Set the host-side parameter stores.
        next_values = self._kscan_current_chunk[0]
        for value, axis in zip(next_values, self._kscan_axes):
            axis.param_store.set_value(value)

    @host_only
    def _kscan_is_out_of_points(self):
        if self._kscan_current_chunk:
            return False
        # Current chunk is empty, but we might be at a chunk boundary.
        self._kscan_param_values_chunk()
        return not self._kscan_current_chunk


def match_default_analysis(analysis: DefaultAnalysis, axes: Iterable[ScanAxis]) -> bool:
    """Return whether the given default analysis can be executed for the given scan
    axes.

    The implementation is currently a bit more convoluted than necessary, as we want to
    catch cases where the parameter specified by the analysis is scanned indirectly
    through overrides. (TODO: Do we really, though? This matches the behaviour prior to
    the refactoring towards exposing a set of required axis handles from
    DefaultAnalysis, but we should revisit this.)
    """
    stores = set(a.param_store for a in axes)
    assert None not in stores, "Can only match analyses after stores have been created"
    return set(a._store for a in analysis.required_axes()) == stores


def filter_default_analyses(
    fragment: ExpFragment, axes: Iterable[ScanAxis]
) -> List[DefaultAnalysis]:
    """Return the default analyses of the given fragment that can be executed for the
    given scan spec.

    See :func:`match_default_analysis`.
    """
    ax = list(axes)  # Don't exhaust an arbitrary iterable.
    result = []
    for analysis in fragment.get_default_analyses():
        if not isinstance(analysis, DefaultAnalysis):
            raise ValueError(
                f"Unexpected get_default_analyses() return value for {fragment}: "
                "Expected list of ndscan.experiment.DefaultAnalysis instances, got "
                f"element of type '{analysis}'"
            )
        if match_default_analysis(analysis, ax):
            result.append(analysis)
    return result


class LiveAnalysisRunner:
    def __init__(
        self,
        fragment,
        axes: List[ScanAxis] = [],
        coordinate_sinks: List[ResultSink] = [],
        scan_result_sinks: dict = {},
    ):
        """Filter analyses for specific scan and store"""
        self._analyses = filter_default_analyses(fragment.get_live_analyses(), axes)
        self._axes = axes
        self._coordinate_sinks = coordinate_sinks
        self._scan_result_sinks = scan_result_sinks

    @rpc(flags={"async"})
    def live_analyses(self):
        """Minimal implementation of entry_point/TopLevelRunner.analyze
        Purely for data processing, does not handle annotations (yet!)"""
        coordinates = make_coordinate_dict(self._axes, self._coordinate_sinks)
        values = make_value_dict(self._scan_result_sinks)
        for a in self._analyses:
            a.execute(coordinates, values)


def describe_scan(
    spec: ScanSpec, fragment: ExpFragment, short_result_names: Dict[ResultChannel, str]
) -> Dict[str, Any]:
    """Return metadata for the given spec in stringly typed dictionary form.

    :param spec: :class:`.ScanSpec` describing the scan.
    :param fragment: Fragment being scanned.
    :param short_result_names: Map from result channel objects to shortened names.
    """
    desc = {}

    desc["fragment_fqn"] = fragment.fqn
    axis_specs = [
        {
            "param": ax.param_schema,
            "path": ax.path,
        }
        for ax in spec.axes
    ]
    for ax, gen in zip(axis_specs, spec.generators):
        gen.describe_limits(ax)

    desc["axes"] = axis_specs
    desc["seed"] = spec.options.seed

    # KLUDGE: Skip non-saved channels to make sure the UI doesn't attempt to display
    # them; they should possibly just be ignored there.
    desc["channels"] = {
        name: channel.describe()
        for (channel, name) in short_result_names.items()
        if channel.save_by_default
    }

    return desc


def describe_analyses(
    analyses: Iterable[DefaultAnalysis], context: AnnotationContext
) -> Dict[str, Any]:
    """Return metadata for the given analyses in stringly typed dictionary form.

    :param analyses: The :class:`.DefaultAnalysis` objects to describe (already filtered
        to those that apply to the scan, and thus are describable by the context).
    :param context: Used to resolve any references to scanned parameters/results
        channels/analysis results.

    :return: The analysis metadata (``annotations``/``online_analyses``), with all
        references to fragment tree objects resolved, and ready for JSON/…
        serialisation.
    """
    desc = {}
    desc["annotations"] = []
    desc["online_analyses"] = {}
    for analysis in analyses:
        annotations, online_analyses = analysis.describe_online_analyses(context)
        desc["annotations"].extend(annotations)
        for name, spec in online_analyses.items():
            if name in desc["online_analyses"]:
                raise ValueError(
                    "An online analysis with name '{}' already exists".format(name)
                )
            desc["online_analyses"][name] = spec
    return desc

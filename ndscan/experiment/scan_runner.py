"""Generic scanning loop.

While :mod:`.scan_generator` describes a scan to be run in the abstract, this module
contains the implementation to actually execute one within an ARTIQ experiment. This
will likely be used by end users via
:class:`~ndscan.experiment.entry_point.FragmentScanExperiment` or subscans.
"""

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import islice
from typing import Any

import numpy as np
from artiq.coredevice.exceptions import RTIOUnderflow
from artiq.language import HasEnvironment, host_only, kernel, kernel_from_string, rpc

from .default_analysis import AnnotationContext, DefaultAnalysis
from .fragment import ExpFragment, RestartKernelTransitoryError, TransitoryError
from .parameters import ParamStore
from .result_channels import ResultChannel, ResultSink, SingleUseSink
from .scan_generator import ScanGenerator, ScanOptions, generate_points
from .utils import is_kernel

__all__ = [
    "ScanAxis",
    "ScanSpec",
    "ScanRunner",
    "select_runner_class",
    "match_default_analysis",
    "filter_default_analyses",
    "describe_scan",
    "describe_analyses",
]

logger = logging.getLogger(__name__)


@dataclass
class ScanAxis:
    """Describes a single axis that is being scanned.

    Apart from the metadata, this also includes the necessary information to execute the
    scan at runtime; i.e. the :class:`.ParamStore` to modify in order to set the
    parameter.
    """

    param_schema: dict[str, Any]
    path: str
    param_store: ParamStore


@dataclass
class ScanSpec:
    """Describes a single scan."""

    #: The list of parameters that are scanned.
    axes: list[ScanAxis]

    #: Generators that give the points for each of the specified axes.
    generators: list[ScanGenerator]

    #: Applicable :class:`.ScanOptions`.
    options: ScanOptions


class ScanRunner(HasEnvironment):
    """Runs the actual loop that executes an :class:`.ExpFragment` for a specified list
    of scan axes (on either the host or core device, as appropriate).
    """

    def build(
        self,
        max_rtio_underflow_retries: int = 3,
        max_transitory_error_retries: int = 10,
        skip_on_persistent_transitory_error: bool = False,
    ):
        """
        :param max_rtio_underflow_retries: Number of RTIOUnderflows to tolerate per scan
            point (by simply trying again) before giving up. Three is a pretty arbitrary
            default – we don't want to block forever in case the experiment is faulty,
            but also want to tolerate ~1% underflow chance for experiments where tight
            timing is critical.
        :param max_transitory_error_retries: Number of transitory errors to tolerate per
            scan point (by simply trying again) before giving up.
        :param skip_on_persistent_transitory_error: By default, transitory errors above
            the configured limit are raised for the calling code to handle (possibly
            terminating the experiment). If ``True``, points with too many transitory
            errors will be skipped instead after logging an error. Consequences for
            overall system robustness should be considered before using this in
            automated code.
        """
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.skip_on_persistent_transitory_error = skip_on_persistent_transitory_error
        self.setattr_device("core")
        self.setattr_device("scheduler")

    def run(
        self, fragment: ExpFragment, spec: ScanSpec, axis_sinks: list[ResultSink]
    ) -> None:
        """Run a scan of the given fragment, with axes as specified.

        Integrates with the ARTIQ scheduler to pause/terminate execution as requested.

        :param fragment: The fragment to iterate.
        :param options: The options for the scan generator.
        :param axis_sinks: A list of :class:`.ResultSink` instances to push the
            coordinates for each scan point to, matching ``scan.axes``.
        """
        # TODO: Support parameters which require host_setup() when changed.
        self.setup(fragment, spec.axes, axis_sinks)
        self.set_points(generate_points(spec.generators, spec.options))
        while True:
            # After every pause(), pull in dataset changes (immediately as well to catch
            # changes between the time the experiment is prepared and when it is run, to
            # keep the semantics uniform).
            fragment.recompute_param_defaults()
            try:
                # FIXME: Need to handle transitory errors here.
                fragment.host_setup()

                # For on-core-device scans, we'll spawn a kernel here.
                if self.acquire(device_cleanup=True):
                    return
            finally:
                fragment.host_cleanup()
                # For host-only scans, self.core might be artiq.sim.devices.Core or
                # similar without a close() method.
                if hasattr(self.core, "close"):
                    self.core.close()
            self.scheduler.pause()

    def setup(
        self, fragment: ExpFragment, axes: list[ScanAxis], axis_sinks: list[ResultSink]
    ) -> None:
        raise NotImplementedError

    def set_points(self, points: Iterator[tuple]) -> None:
        raise NotImplementedError

    def acquire(self, device_cleanup: bool) -> bool:
        """
        :param device_cleanup: Whether to execute :meth:`.ExpFragment.device_cleanup` at
            the end of the scan (e.g. for use in subscans which may not actually leave
            the device).
        :return: ``true`` if scan is complete, ``false`` if the scan has been
            interrupted and ``acquire()`` should be called again to complete it.
        """
        raise NotImplementedError


class ResultBatcher:
    """Intercepts all result channel sinks of the given fragment, making sure that every
    channel has seen exactly one ``push()`` before forwarding the results to whatever
    sinks might have been set originally in one batch.

    This makes sure that buggy ``ExpFragment`` implementations that do not always push
    a result, or points that failed halfway through, do not lead to "desynchronised"
    datasets/… (where the indices in the struct-of-arrays construction no longer match
    up).
    """

    def __init__(self, fragment: ExpFragment) -> None:
        self._fragment = fragment
        self._orig_sinks = dict[ResultChannel, ResultSink]()

    def install(self) -> None:
        """Start intercepting results."""
        channels = dict[str, ResultChannel]()
        self._fragment._collect_result_channels(channels)
        for channel in channels.values():
            if channel.sink is None:
                continue
            self._orig_sinks[channel] = channel.sink
            channel.sink = SingleUseSink()

    def discard_current(self) -> None:
        """Discard any results that may have been pushed already (e.g. if a point was
        interrupted.)
        """
        for channel in self._orig_sinks.keys():
            if channel.sink.is_set():
                # This is normal, e.g. when a transitory error interrupts a point.
                logger.debug("Discarding result for '%s'", channel)
            channel.sink.reset()

    def ensure_complete_and_push(self) -> None:
        """Make sure each result channel has been pushed to (failing if not), and then
        forward the results to the original sinks.
        """
        # First check whether we have all the values.
        for channel in self._orig_sinks.keys():
            if not channel.sink.is_set():
                raise ValueError(
                    f"Missing value for result channel '{channel}' "
                    + "(push() not called for current point)"
                )
        # Only then forward them.
        for channel, orig_sink in self._orig_sinks.items():
            orig_sink.push(channel.sink.get())
            channel.sink.reset()

    def remove(self) -> None:
        """Stop intercepting results, restoring the original sinks."""
        self.discard_current()

        # Restore direct access to original sinks for future use.
        for channel, original_sink in self._orig_sinks.items():
            channel.set_sink(original_sink)
        self._orig_sinks.clear()

    def __enter__(self) -> "ResultBatcher":
        self.install()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.remove()


class HostScanRunner(ScanRunner):
    def setup(
        self, fragment: ExpFragment, axes: list[ScanAxis], axis_sinks: list[ResultSink]
    ) -> None:
        self._fragment = fragment
        self._axes = axes
        self._axis_sinks = axis_sinks

    def set_points(self, points: Iterator[tuple]) -> None:
        self._points = points

    def acquire(self, device_cleanup: bool) -> bool:
        with ResultBatcher(self._fragment) as result_batcher:
            try:
                # FIXME: Need to handle transitory errors here (or possibly, would be
                # enough to do so in ScanRunner.run(), which we want anyway for
                # host_setup(), etc.).
                while True:
                    axis_values = next(self._points, None)
                    if axis_values is None:
                        return True
                    for axis, value in zip(self._axes, axis_values):
                        axis.param_store.set_value(value)
                    self._fragment.device_setup()
                    self._fragment.run_once()

                    result_batcher.ensure_complete_and_push()
                    for sink, value in zip(self._axis_sinks, axis_values):
                        # Now that we know self._fragment successfully produced a
                        # complete point, also record the axis coordinates.
                        sink.push(value)

                    if self.scheduler.check_pause():
                        return False
            finally:
                if device_cleanup:
                    self._fragment.device_cleanup()


class KernelScanRunner(ScanRunner):
    # Note: ARTIQ Python is currently severely limited in its support for generics or
    # metaprogramming. While the interface for this class is effortlessly generic, the
    # implementation might well be a long-forgotten ritual for invoking Cthulhu.

    def setup(
        self, fragment: ExpFragment, axes: list[ScanAxis], axis_sinks: list[ResultSink]
    ) -> None:
        self._fragment = fragment

        # Set up members to be accessed from the kernel through the
        # _get_param_values_chunk RPC call later.
        self._axes = axes
        self._axis_sinks = axis_sinks

        # Interval between scheduler.check_pause() calls on the core device (or rather,
        # the minimum interval; calls are only made after a point has been completed).
        self._pause_check_interval_mu = self.core.seconds_to_mu(0.2)
        self._last_pause_check_mu = np.int64(0)

        # _get_param_values_chunk returns a tuple of lists of values, one for each
        # scan axis. Synthesize a return type annotation (`def foo(self): -> …`) with
        # the concrete type for this scan so the compiler can infer the types in
        # run_chunk() correctly.
        self._get_param_values_chunk.__func__.__annotations__ = {
            "return": tuple.__class_getitem__(
                tuple(list[a.param_store.RpcType] for a in axes)
            )
        }

        # Build kernel function that calls _get_param_values_chunk() and iterates over
        # the returned values, assigning them to the respective parameter stores and
        # calling _run_point() for each.
        #
        # Currently, this can't be expressed as generic code, as there is no way to
        # express indexing or deconstructing a tuple of values of inhomogeneous types
        # without actually writing it out as an assignment from a tuple value.
        for i, axis in enumerate(axes):
            setattr(self, f"_param_setter_{i}", axis.param_store.set_from_rpc)
        self._run_chunk = self._build_run_chunk(len(axes))

        # We'll have to set up the ResultBatcher on the host during the scan to
        # appropriately handle the results streaming in via async RPCs, so unfortunately
        # cannot use the context manager API.
        self._result_batcher: ResultBatcher | None = None

    def set_points(self, points: Iterator[tuple]) -> None:
        self._points = points
        # Stash away points in current kernel chunk until they have been marked
        # complete so we can resume from interruptions.
        self._current_chunk = []
        self._update_host_param_stores()

    _RUN_CHUNK_PROCEED = 0
    _RUN_CHUNK_INTERRUPTED = 1
    _RUN_CHUNK_SCAN_COMPLETE = 2

    def _build_run_chunk(self, num_axes):
        param_decl = " ".join(f"p{idx}," for idx in range(num_axes))
        code = ""
        code += f"({param_decl}) = self._get_param_values_chunk()\n"
        code += "if not p0:\n"  # No more points
        code += "    return self._RUN_CHUNK_SCAN_COMPLETE\n"
        code += "for i in range(len(p0)):\n"
        for idx in range(num_axes):
            code += "    self._param_setter_{0}(p{0}[i])\n".format(idx)
        code += "    if self._run_point():\n"
        code += "        return self._RUN_CHUNK_INTERRUPTED\n"
        code += "return self._RUN_CHUNK_PROCEED"
        return kernel_from_string(["self"], code)

    @rpc(flags={"async"})
    def _install_result_batcher(self):
        self._result_batcher = ResultBatcher(self._fragment)
        self._result_batcher.install()

    @rpc(flags={"async"})
    def _remove_result_batcher(self):
        self._result_batcher.remove()
        self._result_batcher = None

    @kernel
    def acquire(self, device_cleanup: bool) -> bool:
        self._install_result_batcher()
        try:
            self._last_pause_check_mu = self.core.get_rtio_counter_mu()
            while True:
                # Fetch chunk in separate function to make sure stack memory is released
                # every time. (The ARTIQ compiler effectively uses alloca() to provision
                # memory for RPC return values.)
                result = self._run_chunk(self)
                if result == self._RUN_CHUNK_INTERRUPTED:
                    return False
                if result == self._RUN_CHUNK_SCAN_COMPLETE:
                    return True
                assert result == self._RUN_CHUNK_PROCEED
        finally:
            self._remove_result_batcher()
            if device_cleanup:
                self._fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True

    @kernel
    def _run_point(self) -> bool:
        """Execute the fragment for a single point (with the currently set parameters).

        :return: Whether the kernel should be exited/experiment should be paused before
            continuing (``True`` to pause, ``False`` to continue immediately).
        """
        num_underflows = 0
        num_transitory_errors = 0
        while True:
            if self._should_pause():
                return True
            try:
                self._fragment.device_setup()
                self._fragment.run_once()
                break
            except RTIOUnderflow:
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
                self._retry_point()
            except RestartKernelTransitoryError:
                print("Caught transitory error, restarting kernel")
                self._retry_point()
                return True
            except TransitoryError:
                if num_transitory_errors >= self.max_transitory_error_retries:
                    if self.skip_on_persistent_transitory_error:
                        self._skip_point()
                        return False
                    raise
                num_transitory_errors += 1
                print(
                    "Caught transitory error (",
                    num_transitory_errors,
                    "/",
                    self.max_transitory_error_retries,
                    "), retrying",
                )
                self._retry_point()
        self._point_completed()
        return False

    @kernel
    def _should_pause(self) -> bool:
        current_time_mu = self.core.get_rtio_counter_mu()
        if current_time_mu - self._last_pause_check_mu > self._pause_check_interval_mu:
            self._last_pause_check_mu = current_time_mu
            if self.scheduler.check_pause():
                return True
        return False

    @rpc
    def _get_param_values_chunk(self):
        # Number of scan points to send at once. After each chunk, the kernel needs to
        # execute a blocking RPC to fetch new points, so this should be chosen such
        # that latency/constant overhead and throughput are balanced. 10 is an arbitrary
        # choice based on the observation that even for fast experiments, 10 points take
        # a good fraction of a second, while it is still low enough not to run into any
        # memory management issues on the kernel.
        CHUNK_SIZE = 10

        self._current_chunk.extend(
            islice(self._points, CHUNK_SIZE - len(self._current_chunk))
        )

        values = tuple([] for _ in self._axes)
        for p in self._current_chunk:
            for i, (value, axis) in enumerate(zip(p, self._axes)):
                # KLUDGE: Explicitly coerce value to the target type here so we can use
                # the regular (float) scans for integers until proper support for int
                # scans is implemented.
                values[i].append(
                    axis.param_store.to_rpc_type(
                        axis.param_store.coerce(axis.param_store.value_from_pyon(value))
                    )
                )
        return values

    @rpc(flags={"async"})
    def _retry_point(self):
        self._result_batcher.discard_current()

    @rpc(flags={"async"})
    def _skip_point(self):
        self._result_batcher.discard_current()
        values = self._current_chunk.pop(0)
        logger.error("Skipping point: %s", values)
        self._update_host_param_stores()

    @rpc(flags={"async"})
    def _point_completed(self):
        # This might raise an exception, which will only bubble up to the user during
        # the next synchronous RPC request. As this only occurs when the user code
        # contains a logic error (failure to call push() on a result channel), this
        # should be acceptable, however.
        self._result_batcher.ensure_complete_and_push()

        # Now that we know that a complete point was successfully produced, also record
        # the axis coordinates.
        values = self._current_chunk.pop(0)
        for value, sink in zip(values, self._axis_sinks):
            sink.push(value)

        # Prepare for the next point.
        self._update_host_param_stores()

    @host_only
    def _update_host_param_stores(self):
        """Set host-side parameter stores for the scan axes to their current values,
        i.e. as specified by the next point in the current scan chunk.

        This ensures that if a parameter is scanned from a kernel scan that requires
        a host RPC to update (e.g. a non-@kernel device_setup()), the RPC'd code will
        execute using the expected values.
        """
        if self._is_out_of_points():
            return
        # Set the host-side parameter stores.
        next_values = self._current_chunk[0]
        for value, axis in zip(next_values, self._axes):
            axis.param_store.set_value(axis.param_store.value_from_pyon(value))

    @host_only
    def _is_out_of_points(self):
        if self._current_chunk:
            return False
        # Current chunk is empty, but we might be at a chunk boundary.
        self._get_param_values_chunk()
        return not self._current_chunk


def select_runner_class(fragment: ExpFragment) -> type[ScanRunner]:
    if is_kernel(fragment.run_once):
        return KernelScanRunner
    else:
        return HostScanRunner


def match_default_analysis(analysis: DefaultAnalysis, axes: Iterable[ScanAxis]) -> bool:
    """Return whether the given default analysis can be executed for the given scan
    axes.

    The implementation is currently a bit more convoluted than necessary, as we want to
    catch cases where the parameter specified by the analysis is scanned indirectly
    through overrides. (TODO: Do we really, though? This matches the behaviour prior to
    the refactoring towards exposing a set of required axis handles from
    DefaultAnalysis, but we should revisit this.)
    """
    stores = {a.param_store for a in axes}
    assert None not in stores, "Can only match analyses after stores have been created"
    return {a._store for a in analysis.required_axes()} == stores


def filter_default_analyses(
    fragment: ExpFragment, axes: Iterable[ScanAxis]
) -> list[DefaultAnalysis]:
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


def describe_scan(
    spec: ScanSpec, fragment: ExpFragment, short_result_names: dict[ResultChannel, str]
) -> dict[str, Any]:
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
) -> dict[str, Any]:
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
                    f"An online analysis with name '{name}' already exists"
                )
            desc["online_analyses"][name] = spec
    return desc

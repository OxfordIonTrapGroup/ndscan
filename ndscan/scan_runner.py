from artiq.language import *
from contextlib import suppress
from itertools import islice
from typing import Any, Dict, List, Iterator, Tuple
from .fragment import ExpFragment
from .parameters import ParamStore, type_string_to_param
from .result_channels import ResultChannel, ResultSink
from .scan_generator import generate_points, ScanGenerator, ScanOptions
from .utils import is_kernel


class ScanFinished(Exception):
    """Used internally to signal that a scan has been successfully completed (points
    exhausted).

    This is a kludge to work around a bug where tuples of empty lists crash the ARTIQ
    RPC code (kernel aborts), and should never be visible to the user.
    """
    pass


class ScanAxis:
    def __init__(self, param_schema: Dict[str, Any], path: str,
                 param_store: ParamStore):
        self.param_schema = param_schema
        self.path = path
        self.param_store = param_store


class ScanSpec:
    def __init__(self, axes: List[ScanAxis], generators: List[ScanGenerator],
                 options: ScanOptions):
        self.axes = axes
        self.generators = generators
        self.options = options


class ScanRunner(HasEnvironment):
    """Runs the actual loop that executes an :class:`ExpFragment` for a specified list
    of scan axes (on either the host or core device, as appropriate).

    Integrates with the ARTIQ scheduler to pause/terminate execution as requested.

    Conceptually, this is only a single function (``run()``), but is wrapped in a class
    to follow the idiomatic ARTIQ kernel/``HasEnvironment`` integration style.
    """

    # Note: ARTIQ Python is currently severely limited in its support for generics or
    # metaprogramming. While the interface for this class is effortlessly generic, the
    # implementation might well be a long-forgotten ritual for invoking Cthulhu, and is
    # special-cased for a number of low dimensions.

    def build(self):
        ""
        self.setattr_device("core")
        self.setattr_device("scheduler")

    def run(self, fragment: ExpFragment, spec: ScanSpec,
            axis_sinks: List[ResultSink]) -> None:
        """Run a scan of the given fragment, with axes as specified.

        :param fragment: The fragment to iterate.
        :param options: The options for the scan generator.
        :param axis_sinks: A list of :class:`ResultSink` instances to push the
            coordinates for each scan point to, matching ``scan.axes``.
        """

        # Stash away _fragment in member variable to pacify ARTIQ compiler; there is no
        # reason this shouldn't just be passed along and materialised as a global.
        self._fragment = fragment

        # TODO: Handle parameters requiring host setup.
        self._fragment.host_setup()

        points = generate_points(spec.generators, spec.options)

        run_impl = self._run_scan_on_core_device if is_kernel(
            self._fragment.run_once) else self._run_scan_on_host
        run_impl(points, spec.axes, axis_sinks)

    def _run_scan_on_host(self, points: Iterator[Tuple], axes: List[ScanAxis],
                          axis_sinks: List[ResultSink]) -> None:
        while True:
            axis_values = next(points, None)
            if axis_values is None:
                break
            for (axis, value, sink) in zip(axes, axis_values, axis_sinks):
                axis.param_store.set_value(value)
                sink.push(value)

            self._fragment.device_setup()
            self._fragment.run_once()
            self.scheduler.pause()

    def _run_scan_on_core_device(self, points: list, axes: List[ScanAxis],
                                 axis_sinks: List[ResultSink]) -> None:
        # Set up members to be accessed from the kernel through the
        # _kscan_param_values_chunk RPC call later.
        self._kscan_points = points
        self._kscan_axis_sinks = axis_sinks
        self._kscan_axis_coerce_fns = [a.param_store.coerce for a in axes]

        # Stash away points in current kernel chunk until they have been marked
        # completed as a quick shortcut to be able to resume from interruptions. This
        # should be cleaned up a bit later. Alternatively, if we use an (async, but
        # still) RPC to keep track of points completed, we might as well use it to send
        # back all the result channel values from the core device in one go.
        self._kscan_current_chunk = []

        for i, axis in enumerate(axes):
            setattr(self, "_kscan_param_setter_{}".format(i),
                    axis.param_store.set_value)

        # _kscan_param_values_chunk returns a tuple of lists of values, one for each
        # scan axis. Synthesize a return type annotation (`def foo(self): -> …`) with
        # the concrete type for this scan so the compiler can infer the types in
        # _kscan_impl() correctly.
        self._kscan_param_values_chunk.__func__.__annotations__ = {
            "return":
            TTuple([
                TList(type_string_to_param(a.param_schema["type"]).CompilerType)
                for a in axes
            ])
        }

        # TODO: Implement pausing logic.
        # FIXME: Replace this with generated code once eval_kernel() is implemented.
        num_dims = len(axes)
        scan_impl = getattr(self, "_kscan_impl_{}".format(num_dims), None)
        if scan_impl is None:
            raise NotImplementedError(
                "{}-dimensional scans not supported yet".format(num_dims))

        with suppress(ScanFinished):
            while True:
                scan_impl()
                self.core.comm.close()
                self.scheduler.pause()

    @kernel
    def _kscan_impl_1(self):
        while True:
            (param_values_0, ) = self._kscan_param_values_chunk()
            for i in range(len(param_values_0)):
                self._kscan_param_setter_0(param_values_0[i])
                self._kscan_run_fragment_once()
                self._kscan_point_completed()
            if self.scheduler.check_pause():
                return

    @kernel
    def _kscan_impl_2(self):
        while True:
            param_values_0, param_values_1 = self._kscan_param_values_chunk()
            for i in range(len(param_values_0)):
                self._kscan_param_setter_0(param_values_0[i])
                self._kscan_param_setter_1(param_values_1[i])
                self._kscan_run_fragment_once()
                self._kscan_point_completed()
            if self.scheduler.check_pause():
                return

    @kernel
    def _kscan_impl_3(self):
        while True:
            param_values_0, param_values_1, param_values_2 =\
                self._kscan_param_values_chunk()
            for i in range(len(param_values_0)):
                self._kscan_param_setter_0(param_values_0[i])
                self._kscan_param_setter_1(param_values_1[i])
                self._kscan_param_setter_2(param_values_2[i])
                self._kscan_run_fragment_once()
                self._kscan_point_completed()
            if self.scheduler.check_pause():
                return

    @kernel
    def _kscan_run_fragment_once(self):
        self._fragment.device_setup()
        self._fragment.run_once()

    def _kscan_param_values_chunk(self):
        # Chunk size could be chosen adaptively in the future based on wall clock time
        # per point to provide good responsitivity to pause/terminate requests while
        # keeping RPC latency overhead low.
        CHUNK_SIZE = 10

        self._kscan_current_chunk.extend(
            islice(self._kscan_points, CHUNK_SIZE - len(self._kscan_current_chunk)))

        values = tuple([] for _ in self._kscan_axis_coerce_fns)
        for p in self._kscan_current_chunk:
            for i, (value, coerce) in enumerate(zip(p, self._kscan_axis_coerce_fns)):
                # KLUDGE: Explicitly coerce value to the target type here so we can use
                # the regular (float) scans for integers until proper support for int
                # scans is implemented.
                values[i].append(coerce(value))
        if not values[0]:
            raise ScanFinished
        return values

    @rpc(flags={"async"})
    def _kscan_point_completed(self):
        values = self._kscan_current_chunk.pop(0)
        for value, sink in zip(values, self._kscan_axis_sinks):
            sink.push(value)


def describe_scan(spec: ScanSpec, fragment: ExpFragment,
                  results_by_short_name: Dict[str, ResultChannel],
                  result_key_names_by_path: Dict[str, str]):
    """Return metadata for the given spec in stringly typed dictionary form.

    :param spec: :class:`ScanSpec` describing the scan.
    :param fragment: Fragment being scanned.
    :param results_by_short_name: Map from short result names to channel objects.
    :param result_key_names_by_path: Map from result channel path to name of result key
        ("channel_…").
    """
    desc = {}

    desc["fragment_fqn"] = fragment.fqn
    axis_specs = [{
        "param": ax.param_schema,
        "path": ax.path,
    } for ax in spec.axes]
    for ax, gen in zip(axis_specs, spec.generators):
        gen.describe_limits(ax)

    desc["axes"] = axis_specs
    desc["seed"] = spec.options.seed
    desc["channels"] = {
        name: channel.describe()
        for (name, channel) in results_by_short_name.items()
    }

    desc["auto_fit"] = []
    axis_identities = [(s.param_schema["fqn"], s.path) for s in spec.axes]
    for f in fragment.get_default_fits():
        if f.has_data(axis_identities):
            desc["auto_fit"].append(
                f.describe(
                    lambda identity: "axis_{}".format(axis_identities.index(identity)),
                    lambda path: result_key_names_by_path[path]))

    return desc

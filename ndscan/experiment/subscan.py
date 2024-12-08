"""
Implements subscans, that is, the ability for an :class:`.ExpFragment` to scan
another child fragment as part of its execution.
"""

from collections import OrderedDict
from copy import copy
from functools import reduce
from artiq.language import kernel, portable, rpc
from .default_analysis import AnnotationContext, DefaultAnalysis
from .fragment import ExpFragment, Fragment, RestartKernelTransitoryError
from .parameters import ParamHandle
from .result_channels import (ArraySink, LastValueSink, OpaqueChannel, ResultChannel,
                              SubscanChannel)
from .scan_generator import ScanGenerator, ScanOptions, generate_points
from .scan_runner import (ScanAxis, ScanRunner, ScanSpec, describe_analyses,
                          describe_scan, filter_default_analyses, select_runner_class)
from .utils import is_kernel
from ..utils import merge_no_duplicates, shorten_to_unambiguous_suffixes

__all__ = ["setattr_subscan", "Subscan", "SubscanExpFragment"]


class Subscan:
    """Handle returned by :meth:`setattr_subscan`, allowing the subscan to actually be
    executed.
    """
    def __init__(
        self,
        runner: ScanRunner,
        fragment: ExpFragment,
        possible_axes: dict[ParamHandle, ScanAxis],
        schema_channel: SubscanChannel,
        coordinate_channels: list[ResultChannel],
        child_result_sinks: dict[ResultChannel, ArraySink],
        aggregate_result_channels: dict[ResultChannel, ResultChannel],
        short_child_channel_names: dict[str, ResultChannel],
        analyses: list[DefaultAnalysis],
        parent_analysis_result_channels: dict[str, ResultChannel],
    ):
        self._runner = runner
        self._fragment = fragment
        self._possible_axes = possible_axes
        self._schema_channel = schema_channel
        self._coordinate_channels = coordinate_channels
        self._child_result_sinks = child_result_sinks
        self._aggregate_result_channels = aggregate_result_channels
        self._short_child_channel_names = short_child_channel_names
        self._analyses = analyses
        self._parent_analysis_result_channels = parent_analysis_result_channels

    def run(
        self,
        axis_generators: list[tuple[ParamHandle, ScanGenerator]],
        options: ScanOptions = ScanOptions(),
        execute_default_analyses: bool = True
    ) -> tuple[dict[ParamHandle, list], dict[ResultChannel, list]]:
        """Run the subscan with the given axis iteration specifications, and return the
        data point coordinates/result channel values.

        :param axis_generators: The list of scan axes (dimensions). Each element is a
            tuple of parameter to scan (handle must have been passed to
            :func:`setattr_subscan` to set up), and the :class:`ScanGenerator` to use
            to generate the points.
        :param options: :class:`ScanOptions` to control scan execution.
        :param execute_default_analyses: Whether to run any default analyses associated
            with the subfragment after the scan is complete, even if they are not
            exposed as owning fragment channels.

        :return: A tuple ``(coordinates, values, analysis_results)``, each a dictionary
            mapping parameter handles, result channels and analysis channel names to
            lists of their values.
        """

        # TODO: This is probably superfluous (or if not, the on-kernel-friendly
        # implementation might also need something similar).
        for sink in self._child_result_sinks.values():
            sink.clear()
        self.set_scan_spec(axis_generators, options)
        self._fragment.prepare()
        self._runner.run(self._fragment, self._spec,
                         list(self._coordinate_sinks.values()))
        return self._push_results(execute_default_analyses)

    def set_scan_spec(self,
                      axis_generators: list[tuple[ParamHandle, ScanGenerator]],
                      options: ScanOptions = ScanOptions()):
        axes: list[ScanAxis] = []
        generators: list[ScanGenerator] = []
        self._coordinate_sinks = OrderedDict[ParamHandle, ArraySink]()

        for param_handle, generator in axis_generators:
            axis = self._possible_axes.get(param_handle, None)
            assert axis is not None, "Axis not registered in setattr_subscan()"
            axes.append(axis)
            generators.append(generator)
            self._coordinate_sinks[param_handle] = ArraySink()

        self._spec = ScanSpec(axes, generators, options)
        self._runner.setup(self._fragment, axes, list(self._coordinate_sinks.values()))
        self._regenerate_points()

    def _regenerate_points(self):
        self._runner.set_points(
            generate_points(self._spec.generators, self._spec.options))

    @portable
    def acquire(self, execute_default_analyses=False):
        if not self._runner.acquire():
            raise RestartKernelTransitoryError("Subscan interrupted by pause request")
        self._finalize(execute_default_analyses)

    @rpc(flags={"async"})
    def _finalize(self, execute_default_analyses):
        # Return is ignored for on-kernel-friendly scans.
        self._push_results(execute_default_analyses)
        # Prepare for next subscan.
        self._regenerate_points()

    def _push_results(self, execute_default_analyses):
        analysis_schema, analysis_results = self._handle_default_analyses(
            self._spec.axes, self._coordinate_sinks, execute_default_analyses)
        self._push_schema(analysis_schema)
        coordinates = self._push_coordinates()
        values = self._push_values()
        return coordinates, values, analysis_results

    def _push_schema(self, analysis_schema):
        scan_schema = describe_scan(self._spec, self._fragment,
                                    self._short_child_channel_names)
        scan_schema.update(analysis_schema)
        self._schema_channel.push(scan_schema)

    def _push_coordinates(self):
        coordinates = OrderedDict()
        for channel, (param, sink) in zip(self._coordinate_channels,
                                          self._coordinate_sinks.items()):
            v = sink.get_all()
            coordinates[param] = v
            channel.push(v)
            # Prepare for next iteration.
            sink.clear()

        return coordinates

    def _push_values(self):
        values = {}
        for chan, sink in self._child_result_sinks.items():
            v = sink.get_all()
            values[chan] = v
            self._aggregate_result_channels[chan].push(v)
            # Prepare for next iteration.
            sink.clear()
        return values

    def _handle_default_analyses(
        self,
        axes: list[ScanAxis],
        coordinate_sinks: dict[ParamHandle, ArraySink],
        always_run: bool,
    ):
        # Re-filter analyses based on actual scan axes to support slightly dodgy use
        # case where a lower-dimensional scan is actually taken than originally
        # announced – should revisit this design.
        analyses = filter_default_analyses(self._fragment, axes)
        if not analyses:
            return {}, {}

        axis_data = {
            handle._store.identity: sink.get_all()
            for handle, sink in coordinate_sinks.items()
        }

        result_data = {
            chan: sink.get_all()
            for chan, sink in self._child_result_sinks.items()
        }

        def get_axis_index(handle):
            for i, h in enumerate(coordinate_sinks.keys()):
                if handle._store == h._store:
                    return i
            assert False

        context = AnnotationContext(
            get_axis_index, lambda channel: self._short_child_channel_names[channel],
            lambda channel: channel.path in self._parent_analysis_result_channels)
        schema = describe_analyses(analyses, context)
        schema["analysis_results"] = {
            name: parent.path
            for name, parent in self._parent_analysis_result_channels.items()
        }

        analysis_sinks = {}

        if len(self._parent_analysis_result_channels) > 0 or always_run:
            for a in analyses:
                for name, channel in a.get_analysis_results().items():
                    sink = LastValueSink()
                    channel.set_sink(sink)
                    analysis_sinks[name] = sink
            annotations = []
            for a in analyses:
                annotations += a.execute(axis_data, result_data, context)
            if annotations:
                # Replace existing (online-fit) annotations if any analysis produced
                # custom ones. This could be made configurable in the future.
                schema["annotations"] = annotations

        analysis_results = {
            name: sink.get_last()
            for name, sink in analysis_sinks.items()
        }
        # FIXME: Check for None (not-set) values to produce better error message?
        for name, value in analysis_results.items():
            channel = self._parent_analysis_result_channels.get(name, None)
            if channel is not None:
                channel.push(value)

        return schema, analysis_results


def setattr_subscan(owner: Fragment,
                    scan_name: str,
                    fragment: ExpFragment,
                    axis_params: list[tuple[Fragment, str]],
                    save_results_by_default: bool = True,
                    expose_analysis_results: bool = True) -> Subscan:
    """Set up a scan for the given subfragment.

    Result channels are set up in the owning fragment to expose the scan data, such that
    scan results can be inspected after the fact.

    This is the legacy subscan interface, and is geared primarily towards executing the
    scan loop on the host by calling :meth:`Subscan.run` on the returned handle, which
    takes care of setup/results management/etc. all at once. To be able to execute scans
    on-kernel, :class:`.SubscanExpFragment` is preferred, as it directly integrates the
    lifecycle management with the usual setup/cleanup methods, which is more convenient
    in that case.

    :param owner: The fragment to add the subscan to.
    :param scan_name: Name of the scan; appears in result channel names, and the
        :class:`Subscan` instance will be available as ``owner.<scan_name>``.
    :param fragment: The runnable fragment to iterate over in the scan. Must be a
        subfragment of ``owner``.
    :param axis_params: List of `(fragment, param_name)` tuples defining the axes to be
        scanned. It is possible to specify more axes than are actually used; they will
        be overridden and set to their default values.
    :param save_results_by_default: Passed on to all derived result channels.
    :param expose_analysis_results: Whether to add result channels to ``owner`` that
        contain the results of default analyses set for the fragment. Note that for
        this, all results must be known when this function is called (that is, all
        ``axis_params`` should actually be scanned, and the analysis must not fail to
        produce results).

    :return: A :class:`Subscan` instance to use to actually execute the scan.
    """

    assert owner._building, "Can only create a subscan during build_fragment()"
    assert not hasattr(owner, scan_name), f"Field '{scan_name}' already exists"

    # Our own ScanRunner takes care of the fragment lifecycle.
    owner.detach_fragment(fragment)

    subscan = setup_subscan(owner, f"{scan_name}_", fragment, axis_params,
                            save_results_by_default, expose_analysis_results)
    setattr(owner, scan_name, subscan)
    return subscan


def setup_subscan(result_target: Fragment,
                  name_prefix: str,
                  scanned_fragment: ExpFragment,
                  axis_params: list[tuple[Fragment, str]],
                  save_results_by_default: bool = True,
                  expose_analysis_results: bool = True) -> Subscan:
    # Override target parameter stores with newly created stores.
    # TODO: Potentially make handles have identity and accept them directly.
    axes = {}
    coordinate_channels = []
    for i, (param_owner, name) in enumerate(axis_params):
        handle = getattr(param_owner, name)
        param, store = param_owner.override_param(name)

        axes[handle] = ScanAxis(param.describe(), "/".join(param_owner._fragment_path),
                                store)

        # We simply generate sequential result channels to be sure we have enough.
        # Alternatives:
        #  - Require the actually used axes to be given in axis_params (which will be
        #    the most common use case anyway).
        #  - Serialise the scan point coordinates into the scan spec.
        coordinate_channels.append(
            result_target.setattr_result(name_prefix + f"axis_{i}",
                                         OpaqueChannel,
                                         save_by_default=save_results_by_default))

    # Instead of letting our parent directly manage the subfragment result channels,
    # we redirect the results to ArraySinks…
    original_channels = {}
    scanned_fragment._collect_result_channels(original_channels)

    child_result_sinks = {}
    for channel in original_channels.values():
        sink = ArraySink()
        channel.set_sink(sink)
        child_result_sinks[channel] = sink

    # … and re-export result channels that the collected data will be pushed to.
    channel_name_map = shorten_to_unambiguous_suffixes(
        original_channels.keys(), lambda fqn, n: "/".join(fqn.split("/")[-n:]))
    aggregate_result_channels = {}
    short_child_channel_names = {}
    for full_name, short_name in channel_name_map.items():
        short_identifier = short_name.replace("/", "_")
        channel = original_channels[full_name]
        short_child_channel_names[channel] = short_identifier

        # TODO: Implement ArrayChannel to represent a variable number of dimensions
        # around a scalar channel so we can keep the schema information here instead of
        # throwing our hands up in the air helplessly (i.e. using OpaqueChannel).
        aggregate_result_channels[channel] = result_target.setattr_result(
            name_prefix + "channel_" + short_identifier,
            OpaqueChannel,
            save_by_default=save_results_by_default and channel.save_by_default)

    spec_channel = result_target.setattr_result(name_prefix + "spec", SubscanChannel)

    analyses = filter_default_analyses(scanned_fragment, axes.values())
    parent_analysis_result_channels = {}
    if expose_analysis_results:
        analysis_results = reduce(
            lambda x, y: merge_no_duplicates(x, y, kind="analysis result"),
            (a.get_analysis_results() for a in analyses), {})
        for name, channel in analysis_results.items():
            # Just clone results channels and directly register them as channels of the
            # owning fragment – perhaps not the cleanest design…
            #
            # TODO: Include "analysis_result" in the full name? Seemed a bit verbose
            # just to avoid collisions in the unlikely case of an analysis result named
            # "spec", "axis_0" or similar.
            full_name = name_prefix + name
            new_channel = copy(channel)
            new_channel.path = "/".join(result_target._fragment_path + [full_name])
            result_target._register_result_channel(full_name, new_channel.path,
                                                   new_channel)
            parent_analysis_result_channels[name] = new_channel

    # KLUDGE: If we end up running on the kernel, the ARTIQ compiler needs to treat the
    # "inner" (subscan) and "outer" (TopLevelRunner/…) ScanRunner instances differently
    # in terms of types.
    class RunnerInstance(select_runner_class(scanned_fragment)):
        # KLUDGE: In particular, when we manually set the return type annotations for
        # the parameter value fetching RPC, this should only affect this instance, so
        # override the function. (Would just cloning the function/wrapping it in
        # ScanRunner work?)
        def _get_param_values_chunk(self):
            return super()._get_param_values_chunk()

    runner = RunnerInstance(result_target)

    class SubscanInstance(Subscan):
        # ARTIQ compiler needs a different type for each RunnerInstance.
        pass

    return SubscanInstance(runner, scanned_fragment, axes, spec_channel,
                           coordinate_channels, child_result_sinks,
                           aggregate_result_channels, short_child_channel_names,
                           analyses, parent_analysis_result_channels)


class SubscanExpFragment(ExpFragment):
    """An :class:`.ExpFragment` that scans another :class:`.ExpFragment` when it
    executes ("subscan").

    Compared to the legacy way of creating subscans, :func:`setattr_subscan`, this
    seamlessly supports the execution of ``@kernel`` subscans: not only can the scanned
    fragment be run on the core device (which the legacy interface supported as well),
    but the :meth:`run_once` method driving the scan itself can also be ``@kernel``.
    This means that :class:`SubscanExpFragment` can be used as part of bigger on-device
    experiments, and that frequent recompilation overhead for repeated subscans can be
    avoided.

    The API of this fragment supports use through composition, which is the natural and
    more flexible way (compared to inheritance). However, when using such a fragment as
    part of a larger code base, be aware of the general restrictions of the ARTIQ
    Python compiler, in particular the fact that all instances of a class must share
    the same type (including attributes, etc.). For this reason, you might want to
    create a separate subtype of this class for each use, such that multiple pieces of
    client code remain composable (can be combined into yet another bigger on-kernel
    program). One way to achieve this is by just creating an "empty" subclass:

    .. code-block:: python

        class Foo(ExpFragment):
            "The fragment to be scanned."
            def build_fragment(self) -> None:
                self.setattr_param("param_a", FloatParam, "a value", default=0.0)
                # […]

            @kernel
            def run_once(self):
                # […]

        class FooSubscan(SubscanExpFragment):
            pass

        class Parent(ExpFragment):
            def build_fragment(self) -> None:
                self.setattr_fragment("foo", Foo)
                self.setattr_fragment("scan", FooSubscan, self, "foo",
                    [(self.foo, "param_a")])
                self.setattr_param("num_scan_points",
                                   IntParam,
                                   "Number of scan points",
                                   default=21,
                                   min=2)

            @rpc(flags={"async"})
            def configure_scan(self):
                if self.num_scan_points.changed_after_use():
                    self.scan.configure([(self.foo.param_a,
                        LinearGenerator(0.0, 0.1, self.num_scan_points.use()))])

            def host_setup(self):
                # Run at least once before kernel starts such that all the fields
                # are initialised (required for the ARTIQ compiler).
                self.configure_scan()
                super().host_setup()

            @kernel
            def device_setup(self):
                # Update scan if num_scan_points was changed (can be left out if
                # there are no scannable parameters influencing the scan settings).
                self.configure_scan()
                self.device_setup_subfragments()

            @kernel
            def run_once(self):
                # Execute the subscan (and anything else that the fragment might
                # need to do).
                self.scan.run_once()

    Another way is to just make the :class:`.ExpFragment` performing the subscan a
    subclass of :class:`SubscanExpFragment`:

    .. code-block:: python

        class Parent(SubscanExpFragment):
            def build_fragment(self) -> None:
                self.setattr_fragment("foo", Foo)
                super().build_fragment(self, "foo", [(self.foo, "param_a")])
                self.setattr_param("num_scan_points",
                                   IntParam,
                                   "Number of scan points",
                                   default=21,
                                   min=2)

            # configure_scan(), host_setup() and device_setup() as above.
    """
    def build_fragment(self,
                       scanned_fragment_parent: Fragment,
                       scanned_fragment: ExpFragment | str,
                       axis_params: list[tuple[Fragment, str]],
                       save_results_by_default: bool = True,
                       expose_analysis_results: bool = True) -> None:
        """
        :param scanned_fragment_parent: The fragment that owns the scanned fragment.
        :param scanned_fragment: The fragment to scan. Can either be passed as a string
            (the name of the fragment in the parent) or directly as the
            :class:`.ExpFragment` reference.
        :param axis_params: List of `(fragment, param_name)` tuples defining the axes
            to be scanned.
        :param save_results_by_default: Passed on to all derived result channels.
        :param expose_analysis_results: Whether to add result channels to this fragment
            that contain the results of default analyses set for the fragment. Note that
            for this to work, all results must be known when this function is called
            (that is, all ``axis_params`` should actually be scanned, and any analyses
            must not fail to produce results).
        """
        if isinstance(scanned_fragment, str):
            scanned_fragment = getattr(scanned_fragment_parent, scanned_fragment)
        scanned_fragment_parent.detach_fragment(scanned_fragment)
        self._scanned_fragment = scanned_fragment
        # FIXME: Fix subscan model name inference code, remove "_".
        self._subscan = setup_subscan(self, "_", scanned_fragment, axis_params,
                                      save_results_by_default, expose_analysis_results)
        if is_kernel(scanned_fragment.run_once):
            self.run_once = self._kernel_run_once

    def configure(
        self,
        axis_generators: list[tuple[ParamHandle, ScanGenerator]],
        options: ScanOptions = ScanOptions()
    ) -> None:
        """Configure point generators for each scan axis, and scan options.

        This only needs to be called once (but can be called multiple times to change
        settings between ``run_once()`` invocations, e.g. from a parent fragment
        ``{host, device}_setup()``).

        For on-core-device scans, this has to be called at least once before the kernel
        is first entered (e.g. from ``host_setup()``) such that the types of all the
        fields can be known.

        :param axis_generators: The list of scan axes (dimensions). Each element is a
            tuple of parameter to scan (must correspond to one of the axes specified
            in the constructor; see :meth:`build_fragment`), and the
            :class:`.ScanGenerator` to use to generate the points.
        :param options: :class:`.ScanOptions` to control scan execution.
        """
        self._subscan.set_scan_spec(axis_generators, options)

    # We don't forward prepare(), as there will be a top-level ExpFragment to own the
    # scanned fragment anyway, which can then take care of this directly.

    def host_setup(self):
        """"""
        super().host_setup()
        self._scanned_fragment.host_setup()

    def host_cleanup(self):
        """"""
        self._scanned_fragment.host_cleanup()
        super().host_cleanup()

    def run_once(self) -> None:
        """Execute the subscan as previously configured.

        This has the usual semantics of a fragment ``run_once()`` method, i.e. calling
        it will acquire one set of results for the fragment (here, a complete scan) and
        write them to the result channels. If the scanned fragment has an ``@kernel``
        ``run_once()`` method, this will automatically be made a ``@kernel`` method as
        well.
        """
        self._subscan.acquire()

    @kernel
    def _kernel_run_once(self):
        self._subscan.acquire()

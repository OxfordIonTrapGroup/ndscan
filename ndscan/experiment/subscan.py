"""
Implements subscans, that is, the ability for an :class:`.ExpFragment` to scan
another child fragment as part of its execution.
"""

from collections import OrderedDict
from copy import copy
from functools import reduce
from typing import Callable, Dict, List, Tuple
from .default_analysis import AnnotationContext, DefaultAnalysis
from .fragment import ExpFragment, Fragment
from .parameters import ParamHandle
from .result_channels import (ArraySink, LastValueSink, OpaqueChannel, ResultChannel,
                              SubscanChannel)
from .scan_generator import ScanGenerator, ScanOptions
from .scan_runner import (ScanAxis, ScanRunner, ScanSpec, describe_analyses,
                          describe_scan, filter_default_analyses)
from ..utils import merge_no_duplicates, shorten_to_unambiguous_suffixes

__all__ = ["setattr_subscan", "Subscan"]


class Subscan:
    """Handle returned by :meth:`setattr_subscan`, allowing the subscan to actually be
    executed.
    """
    def __init__(
        self,
        run_fn: Callable[[ExpFragment, ScanSpec, List[ArraySink]], None],
        fragment: ExpFragment,
        possible_axes: Dict[ParamHandle, ScanAxis],
        schema_channel: SubscanChannel,
        coordinate_channels: List[ResultChannel],
        child_result_sinks: Dict[ResultChannel, ArraySink],
        aggregate_result_channels: Dict[ResultChannel, ResultChannel],
        short_child_channel_names: Dict[str, ResultChannel],
        analyses: List[DefaultAnalysis],
        parent_analysis_result_channels: Dict[str, ResultChannel],
    ):
        self._run_fn = run_fn
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
        axis_generators: List[Tuple[ParamHandle, ScanGenerator]],
        options: ScanOptions = ScanOptions(),
        execute_default_analyses: bool = True
    ) -> Tuple[Dict[ParamHandle, list], Dict[ResultChannel, list]]:
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

        for sink in self._child_result_sinks.values():
            sink.clear()

        axes = []
        generators = []
        coordinate_sinks = OrderedDict()

        for param_handle, generator in axis_generators:
            axis = self._possible_axes.get(param_handle, None)
            assert axis is not None, "Axis not registered in setattr_subscan()"
            axes.append(axis)
            generators.append(generator)
            coordinate_sinks[param_handle] = ArraySink()

        spec = ScanSpec(axes, generators, options)
        self._fragment.prepare()
        self._run_fn(self._fragment, spec, list(coordinate_sinks.values()))

        scan_schema = describe_scan(spec, self._fragment,
                                    self._short_child_channel_names)

        analysis_schema, analysis_results = self._handle_default_analyses(
            axes, coordinate_sinks, execute_default_analyses)
        scan_schema.update(analysis_schema)
        self._schema_channel.push(scan_schema)

        for channel, sink in zip(self._coordinate_channels, coordinate_sinks.values()):
            channel.push(sink.get_all())

        values = {}
        for chan, sink in self._child_result_sinks.items():
            v = sink.get_all()
            values[chan] = v
            self._aggregate_result_channels[chan].push(v)

        coordinates = OrderedDict((p, s.get_all()) for p, s in coordinate_sinks.items())
        return coordinates, values, analysis_results

    def _handle_default_analyses(
        self,
        axes: List[ScanAxis],
        coordinate_sinks: Dict[ParamHandle, ArraySink],
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
                    axis_params: List[Tuple[Fragment, str]],
                    save_results_by_default: bool = True,
                    expose_analysis_results: bool = True) -> Subscan:
    """Set up a scan for the given subfragment.

    Result channels are set up to expose the scan data in the owning fragment for
    introspectability.

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

    assert owner._building, "Can only call install_subscan() during build_fragment()"
    assert not hasattr(owner, scan_name), "Field '{}' already exists".format(scan_name)
    assert fragment in owner._subfragments, "Can only scan immediate subfragments"
    assert fragment not in owner._absorbed_results_subfragments, \
        "Subfragment result channels already used (is there already another scan?)"

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
            owner.setattr_result(scan_name + "_axis_{}".format(i),
                                 OpaqueChannel,
                                 save_by_default=save_results_by_default))

    # Instead of letting our parent directly manage the subfragment result channels,
    # we redirect the results to ArraySinks…
    original_channels = {}
    fragment._collect_result_channels(original_channels)
    owner._absorbed_results_subfragments.add(fragment)

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
        aggregate_result_channels[channel] = owner.setattr_result(
            scan_name + "_channel_" + short_identifier,
            OpaqueChannel,
            save_by_default=save_results_by_default and channel.save_by_default)

    spec_channel = owner.setattr_result(scan_name + "_spec", SubscanChannel)

    analyses = filter_default_analyses(fragment, axes.values())
    parent_analysis_result_channels = {}
    if expose_analysis_results:
        analysis_results = reduce(
            lambda l, r: merge_no_duplicates(l, r, kind="analysis result"),
            (a.get_analysis_results() for a in analyses), {})
        for name, channel in analysis_results.items():
            # Just clone results channels and directly register them as channels of the
            # owning fragment – perhaps not the cleanest design…
            #
            # TODO: Include "analysis_result" in the full name? Seemed a bit verbose
            # just to avoid collisions in the unlikely case of an analysis result named
            # "spec", "axis_0" or similar.
            full_name = scan_name + "_" + name
            new_channel = copy(channel)
            new_channel.path = "/".join(owner._fragment_path + [full_name])
            owner._register_result_channel(full_name, new_channel.path, new_channel)
            parent_analysis_result_channels[name] = new_channel

    subscan = Subscan(
        ScanRunner(owner).run, fragment, axes, spec_channel, coordinate_channels,
        child_result_sinks, aggregate_result_channels, short_child_channel_names,
        analyses, parent_analysis_result_channels)
    setattr(owner, scan_name, subscan)
    return subscan

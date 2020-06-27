"""
Implements subscans, that is, the ability for an :class:`.ExpFragment` to scan
another child fragment as part of its execution.
"""

from collections import OrderedDict
from typing import Callable, Dict, List, Tuple
from .default_analysis import AnnotationContext
from .fragment import ExpFragment, Fragment
from .parameters import ParamHandle
from .result_channels import ArraySink, OpaqueChannel, ResultChannel, SubscanChannel
from .scan_generator import ScanGenerator, ScanOptions
from .scan_runner import (ScanAxis, ScanRunner, ScanSpec, describe_scan,
                          filter_default_analyses)
from ..utils import shorten_to_unambiguous_suffixes

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
    ):
        self._run_fn = run_fn
        self._fragment = fragment
        self._possible_axes = possible_axes
        self._schema_channel = schema_channel
        self._coordinate_channels = coordinate_channels
        self._child_result_sinks = child_result_sinks
        self._aggregate_result_channels = aggregate_result_channels
        self._short_child_channel_names = short_child_channel_names

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
            with the subfragment after the scan is complete.

        :return: A tuple ``(coordinates, values)``, each a dictionary mapping parameter
            handles resp. result channels to lists of their values.
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

        if execute_default_analyses:
            annotations = self._run_default_analyses(spec, coordinate_sinks)
            if annotations:
                # Replace existing (online-fit) annotations if any analysis produced
                # custom ones. This could be made configurable in the future.
                scan_schema["annotations"] = annotations

        self._schema_channel.push(scan_schema)

        for channel, sink in zip(self._coordinate_channels, coordinate_sinks.values()):
            channel.push(sink.get_all())

        values = {}
        for chan, sink in self._child_result_sinks.items():
            v = sink.get_all()
            values[chan] = v
            self._aggregate_result_channels[chan].push(v)

        coordinates = OrderedDict((p, s.get_all()) for p, s in coordinate_sinks.items())
        return coordinates, values

    def _run_default_analyses(self, spec, coordinate_sinks):
        analyses = filter_default_analyses(self._fragment, spec)
        if not analyses:
            return []

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
                if handle == h:
                    return i
            assert False

        context = AnnotationContext(
            get_axis_index, lambda channel: self._short_child_channel_names[channel])

        annotations = []
        for a in analyses:
            annotations += a.execute(axis_data, result_data, context)
        return annotations


def setattr_subscan(owner: Fragment,
                    scan_name: str,
                    fragment: ExpFragment,
                    axis_params: List[Tuple[Fragment, str]],
                    save_results_by_default: bool = True) -> Subscan:
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
        # around a scalar channel so we can keep the schema information here.
        aggregate_result_channels[channel] = owner.setattr_result(
            scan_name + "_channel_" + short_identifier,
            OpaqueChannel,
            save_by_default=save_results_by_default and channel.save_by_default)

    spec_channel = owner.setattr_result(scan_name + "_spec", SubscanChannel)

    subscan = Subscan(
        ScanRunner(owner).run, fragment, axes, spec_channel, coordinate_channels,
        child_result_sinks, aggregate_result_channels, short_child_channel_names)
    setattr(owner, scan_name, subscan)
    return subscan

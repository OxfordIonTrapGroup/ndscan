"""
Implements subscans, that is, the ability for an :class:`.fragment.ExpFragment` to scan
another child fragment as part of its execution.
"""

from collections import OrderedDict
from functools import partial
from typing import Callable, Dict, List, Tuple
from .fragment import ExpFragment, Fragment
from .parameters import ParamHandle
from .result_channels import ArraySink, OpaqueChannel, ResultChannel
from .scan_generator import ScanAxis, ScanGenerator, ScanSpec
from .scan_runner import ScanRunner
from .utils import shorten_to_unambiguous_suffixes


class Subscan:
    """Handle returned by :meth:`setattr_subscan`, allowing the subscan to actually be
    executed.
    """

    def __init__(self, run_fn: Callable[[ScanSpec, List[ArraySink]], None],
                 possible_axes: Dict[ParamHandle, Tuple[ScanAxis]],
                 child_result_sinks: Dict[ResultChannel, ArraySink],
                 aggregate_result_channels: Dict[ResultChannel, ResultChannel]):
        self._run_fn = run_fn
        self._child_result_sinks = child_result_sinks
        self._aggregate_result_channels = aggregate_result_channels
        self._possible_axes = possible_axes

    def run(self, generators: List[Tuple[ParamHandle, ScanGenerator]], **scan_options
            ) -> Tuple[Dict[ParamHandle, list], Dict[ResultChannel, list]]:
        """Run the subscan with the given axis iteration specifications, and return the
        data point coordinates/result channel values.

        :param generators: The list of scan axes (dimensions). Each element is a tuple
            of parameter to scan (handle must have been passed to
            :func:`setattr_subscan` to set up), and the :class:`ScanGenerator` to use
            to generate the points.
        :param scan_options: Any extra options to pass to :class:`ScanSpec`.

        :return: A tuple ``(coordinates, values)``, each a dictionary mapping parameter
            handles resp. result channels to their values.
        """

        for sink in self._child_result_sinks.values():
            sink.clear()

        scan_axes = []
        coordinate_sinks = OrderedDict()

        for param_handle, generator in generators:
            axis = self._possible_axes.get(param_handle, None)
            assert axis is not None, "Axis not registered in setattr_subscan()"
            axis.generator = generator
            scan_axes.append(axis)
            coordinate_sinks[param_handle] = ArraySink()

        # FIXME: Find a better abstraction for this, e.g. a ScanOptions class.
        options = {
            "num_repeats": 1,
            "continuous_without_axes": False,
            "randomise_order_globally": False,
            "seed": None
        }
        options.update(scan_options)
        spec = ScanSpec(scan_axes, **options)
        self._run_fn(spec, list(coordinate_sinks.values()))

        values = {}
        for chan, sink in self._child_result_sinks.items():
            v = sink.get_all()
            values[chan] = v
            self._aggregate_result_channels[chan].push(v)

        coordinates = OrderedDict((p, s.get_all()) for p, s in coordinate_sinks.items())
        return coordinates, values


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
    for i, (param_owner, name) in enumerate(axis_params):
        handle = getattr(param_owner, name)
        param, store = param_owner.override_param(name)

        # FIXME: Refactor this so we don't needlessly pass None here.
        axes[handle] = ScanAxis(param.describe(), param_owner._fragment_path, store,
                                None)

        # We simply generate sequential result channels to be sure we have enough.
        # Alternatives:
        #  - Require the actually used axes to be given in axis_params (which will be
        #    the most common use case anyway).
        #  - Serialise the scan point coordinates into the scan spec.
        owner.setattr_result(
            scan_name + "axis_{}".format(i),
            OpaqueChannel,
            save_by_default=save_results_by_default)

    # Instead of letting our parent directly manage the subfragment result channels,
    # we redirect the results to ArraySinks…
    original_channels = {}
    fragment._collect_result_channels(original_channels)
    owner._absorbed_results_subfragments.add(fragment)

    result_array_sinks = {}
    for channel in original_channels.values():
        sink = ArraySink()
        channel.set_sink(sink)
        result_array_sinks[channel] = sink

    # … and re-export result channels that the collected data will be pushed to.
    SCAN_SPEC_NAME = "spec"
    channel_name_map = shorten_to_unambiguous_suffixes(
        list(original_channels.keys()) +
        [SCAN_SPEC_NAME], lambda fqn, n: "/".join(fqn.split("/")[-n:]))
    del channel_name_map[SCAN_SPEC_NAME]
    result_array_channels = {}
    for full_name, short_name in channel_name_map.items():
        channel = original_channels[full_name]

        # TODO: Implement ArrayChannel to represent a variable number of dimensions
        # around a scalar channel so we can keep the schema information here.
        result_array_channels[channel] = owner.setattr_result(
            scan_name + "_" + short_name,
            OpaqueChannel,
            save_by_default=save_results_by_default and channel.save_by_default)

    # TODO: Actually write scan metadata to this.
    owner.setattr_result(scan_name + "_" + SCAN_SPEC_NAME, OpaqueChannel)

    run_fn = partial(ScanRunner(owner).run, fragment)
    subscan = Subscan(run_fn, axes, result_array_sinks, result_array_channels)
    setattr(owner, scan_name, subscan)

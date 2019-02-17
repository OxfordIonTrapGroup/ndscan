r"""
Top-level functions for launching :class:`.ExpFragment`\ s and their scans from the rest
of the ARTIQ ``HasEnvironment`` universe.
"""

from artiq.language import *
from contextlib import suppress
import json
import logging
import random
from typing import Callable, Type

from .fragment import ExpFragment
from .parameters import type_string_to_param
from .result_channels import AppendingDatasetSink, ScalarDatasetSink
from .scan_generator import GENERATORS, ScanOptions
from .scan_runner import ScanAxis, ScanRunner, ScanSpec
from .utils import shorten_to_unambiguous_suffixes, is_kernel

# We don't want to export FragmentScanExperiment to hide it from experiment
# class discovery.
__all__ = ["make_fragment_scan_exp", "PARAMS_ARG_KEY"]

#: Name of the ``artiq.language.HasEnvironment`` argument that is used to confer the
#: list of available parameters to the dashboard plugin, and to pass the information
#: about scanned and overriden parameters to the :class:`FragmentScanExperiment`
#: when it is launched.
#:
#: Users should not need to directly interface with this.
PARAMS_ARG_KEY = "ndscan_params"

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

    def prepare(self):
        """Collect parameters to set from both scan axes and simple overrides, and
        initialise result channels.
        """

        # Create scan and parameter overrides.
        param_stores = {}
        for fqn, specs in self._params.get("overrides", {}).items():
            store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            param_stores[fqn] = [{
                "path": s["path"],
                "store": store_type((fqn, s["path"]), s["value"])
            } for s in specs]

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
            param_stores.setdefault(fqn, []).append({"path": pathspec, "store": store})
            axes.append(ScanAxis(self.schemata[fqn], pathspec, store))

        options = ScanOptions(
            scan.get("num_repeats", 1), scan.get("continuous_without_axes", True),
            scan.get("randomise_order_globally", False))
        self._scan = ScanSpec(axes, generators, options)

        self.fragment.init_params(param_stores)

        # Initialise result channels.
        chan_dict = {}
        self.fragment._collect_result_channels(chan_dict)

        chan_name_map = shorten_to_unambiguous_suffixes(
            chan_dict.keys(), lambda fqn, n: "/".join(fqn.split("/")[-n:]))

        self.channels = {}
        self._channel_dataset_names = {}
        for path, channel in chan_dict.items():
            if not channel.save_by_default:
                continue
            name = chan_name_map[path].replace("/", "_")
            self.channels[name] = channel

            if self._scan.axes:
                dataset = "channel_{}".format(name)
                self._channel_dataset_names[path] = dataset
                sink = AppendingDatasetSink(self, "ndscan.points." + dataset)
            else:
                self._channel_dataset_names[path] = name
                sink = ScalarDatasetSink(self, "ndscan.point." + name)
            channel.set_sink(sink)

    def run(self):
        """Run the (possibly trivial) scan."""
        self._broadcast_metadata()
        self._issue_ccb()

        with suppress(TerminationRequested):
            if not self._scan.axes:
                self._run_single()
            else:
                runner = ScanRunner(self)
                axis_sinks = [
                    AppendingDatasetSink(self, "ndscan.points.axis_{}".format(i))
                    for i in range(len(self._scan.axes))
                ]
                runner.run(self.fragment, self._scan, axis_sinks)
            self._set_completed()

    def _run_single(self):
        try:
            with suppress(TerminationRequested):
                while True:
                    self.fragment.host_setup()
                    self._point_phase = False
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
        first = True
        while not self.scheduler.check_pause():
            if first:
                self.fragment.device_setup()
                first = False
            else:
                self.fragment.device_reset()
            self.fragment.run_once()
            self._broadcast_point_phase()
            if not self._scan.options.continuous_without_axes:
                return

    def _set_completed(self):
        self.set_dataset("ndscan.completed", True, broadcast=True)

    def _broadcast_metadata(self):
        def set(name, value):
            self.set_dataset("ndscan." + name, value, broadcast=True)

        set("fragment_fqn", self.fragment.fqn)
        set("rid", self.scheduler.rid)
        set("completed", False)

        axis_specs = [{
            "param": ax.param_schema,
            "path": ax.path,
        } for ax in self._scan.axes]
        for ax, gen in zip(axis_specs, self._scan.generators):
            gen.describe_limits(ax)

        set("axes", json.dumps(axis_specs))

        set("seed", self._scan.options.seed)

        # KLDUGE: Broadcast auto_fit before channels to allow simpler implementation
        # in current fit applet. As the applet implementation grows more sophisticated
        # (hiding axes, etc.), it should be easy to relax this requirement.

        fits = []
        axis_identities = [(s.param_schema["fqn"], s.path) for s in self._scan.axes]
        for f in self.fragment.get_default_fits():
            if f.has_data(axis_identities):
                fits.append(
                    f.describe(
                        lambda identity: "axis_{}".format(
                            axis_identities.index(identity)), lambda path: self.
                        _channel_dataset_names[path]))
        set("auto_fit", json.dumps(fits))

        channels = {
            name: channel.describe()
            for (name, channel) in self.channels.items()
        }
        set("channels", json.dumps(channels))

    @rpc(flags={"async"})
    def _broadcast_point_phase(self):
        self._point_phase = not self._point_phase
        self.set_dataset("ndscan.point_phase", self._point_phase, broadcast=True)

    def _issue_ccb(self):
        cmd = ("${python} -m ndscan.applet "
               "--server=${server} "
               "--port=${port_notify} "
               "--port-control=${port_control}")
        cmd += " --rid={}".format(self.scheduler.rid)
        self.ccb.issue(
            "create_applet",
            "ndscan: " + self.fragment.fqn,
            cmd,
            group="ndscan",
            is_transient=True)


def make_fragment_scan_exp(fragment_class: Type[ExpFragment]):
    """Create a :class:`FragmentScanExperiment` subclass that scans the given
    :class:`.ExpFragment`, ready to be picked up by the ARTIQ explorer/…

    This is the default way of creating scan experiments::

        class MyExperiment(ExpFragment):
            def build_fragment(self):
                # ...

            def run_once(self):
                # ...

        MyExperimentScan = make_fragment_scan_exp(MyExperiment)
    """

    class FragmentScanShim(FragmentScanExperiment):
        def build(self):
            super().build(lambda: fragment_class(self, []))

    # Take on the name of the fragment class to keep result file names informative.
    FragmentScanShim.__name__ = fragment_class.__name__

    # Use the fragment class docstring to display in the experiment explorer UI.
    FragmentScanShim.__doc__ = fragment_class.__doc__

    return FragmentScanShim

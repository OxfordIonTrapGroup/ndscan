import json
import logging

from artiq.language import *
from artiq.protocols import pyon
from collections import OrderedDict
from contextlib import suppress
from typing import Callable, Dict, List, Type
from .fragment import Fragment, ExpFragment, ParamStore
from .utils import shorten_to_unambiguous_suffixes, will_spawn_kernel


# We don't want to export FragmentScanExperiment to hide it from experiment
# class discovery.
__all__ = ["make_fragment_scan_exp", "PARAMS_ARG_KEY"]

PARAMS_ARG_KEY = "ndscan_params"

logger = logging.getLogger(__name__)


class ScanSpec:
    def __init__(self, axes):
        self.axes = []

    def is_continuous(self) -> bool:
        return not self.axes


class FragmentScanExperiment(EnvExperiment):
    argument_ui = "ndscan"

    def build(self, fragment_init: Callable[[], ExpFragment]):
        self.setattr_device("core")
        self.setattr_device("scheduler")

        self.fragment = fragment_init()

        instances = dict()
        schemata = dict()
        self.fragment._build_param_tree(instances, schemata)
        desc = {
            "instances": instances,
            "schemata": schemata,
            "always_shown": self.fragment._get_always_shown_params(),
            "overrides": {},
            "scan": {
                "axes": []
            }
        }
        self._params = self.get_argument(PARAMS_ARG_KEY, PYONValue(default=desc))

    def prepare(self):
        param_stores = {}
        for fqn, specs in self._params.get("overrides", {}).items():
            param_stores[fqn] = [{"path": s["path"], "store": ParamStore(s["value"])} for s in specs]

        scan = self._params.get("scan", {})

        # Validate things, etc.z
        # Set up scan spec from arguments.
        self._scan = ScanSpec([])

        for ax in self._scan.axes:
            # Add to param_stores.
            pass

        self.fragment._apply_param_overrides(param_stores)

        chan_dict = OrderedDict()
        self.fragment._collect_result_channels(chan_dict)

        chan_name_map = shorten_to_unambiguous_suffixes(
            chan_dict.keys(),
            lambda fqn, n: "/".join(fqn.split("/")[-n:]))

        self.channels = OrderedDict()
        for path, channel in chan_dict.items():
            name = chan_name_map[path].replace("/", "_")
            self.channels[name] = channel
            def make_cb(name):
                return lambda v: self._broadcast_result(name, v)
            channel.set_result_callback(make_cb(name))

    def run(self):
        self._broadcast_metadata()

        if self._scan.is_continuous():
            self._run_continuous()
        else:
            self._run_scan()

    def analyze(self):
        # See whether there are any default fits set up for the chosen
        # parameter(s), otherwise call:
        self.fragment.analyze()

    def _run_continuous(self):
        # Issue CBC for applet display.
        with suppress(TerminationRequested):
            while True:
                self.fragment.host_setup()
                if will_spawn_kernel(self.fragment.run_once):
                    self._krun_continuous()
                    self.core.comm.close()
                else:
                    self._continuous_loop()
                self.scheduler.pause()
        self._set_completed()

    @kernel
    def _krun_continuous(self):
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
                self.fragment.device_reset([])
            self.fragment.run_once()
            # TODO: Broadcast result channels, or is this done explicitly before?

    def _run_scan(self):
         # Issue CBC for applet display.

        # For each scan level, …

        self._set_completed()

    def _set_completed(self):
        self.set_dataset("ndscan.completed", True, broadcast=True)

    def _broadcast_metadata(self):
        def set(name, value):
            self.set_dataset("ndscan." + name, value, broadcast=True)

        set("rid", self.scheduler.rid)
        set("completed", False)

        # TODO: Describe axes, …
        channels = {name: channel.describe() for (name, channel) in self.channels.items()}
        set("channels", json.dumps(channels))

    def _broadcast_result(self, channel_name, value):
        if self._scan.is_continuous():
            self.set_dataset("ndscan.point.{}".format(channel_name), value, broadcast=True)
        else:
            self.append_to_dataset("ndscan.points.channel_{}".format(channel_name), value)


def make_fragment_scan_exp(fragment_class: Type[ExpFragment]):
    class FragmentScanShim(FragmentScanExperiment):
        def build(self):
            super().build(lambda: fragment_class(self, []))

    doc = fragment_class.__doc__
    if not doc:
        doc = fragment_class.__name__
    FragmentScanShim.__doc__ = __doc__

    return FragmentScanShim
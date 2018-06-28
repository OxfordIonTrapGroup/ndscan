import json
import logging
import numpy as np
import random

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


class RefiningGenerator:
    def __init__(self, lower, upper):
        self.lower = float(min(lower, upper))
        self.upper = float(max(lower, upper))

    def has_level(self, level: int):
        return True

    def points_for_level(self, level: int):
        if level == 0:
            return [self.lower, self.upper]

        d = self.upper - self.lower
        num = 2**(level - 1)
        return np.arange(num) * d / num + d / (2 * num)

    def describe_limits(self, target: dict):
        target["min"] = self.lower
        target["max"] = self.upper


class ScanAxis:
    def __init__(self, param_schema: str, path: str, param_store, generator):
        self.param_schema = param_schema
        self.path = path
        self.generator = generator
        self.param_store = param_store

    def describe(self) -> Dict[str, any]:
        result = {
            "param": self.param_schema,
            "path": self.path,
        }
        self.generator.describe_limits(result)
        return result


class ScanSpec:
    def __init__(self, axes):
        self.axes = axes

    def is_continuous(self) -> bool:
        return not self.axes


class ScanSpecError(Exception):
    pass


class FragmentScanExperiment(EnvExperiment):
    argument_ui = "ndscan"

    def build(self, fragment_init: Callable[[], ExpFragment]):
        self.setattr_device("ccb")
        self.setattr_device("core")
        self.setattr_device("scheduler")

        self.fragment = fragment_init()

        instances = dict()
        self.schemata = dict()
        self.fragment._build_param_tree(instances, self.schemata)
        desc = {
            "instances": instances,
            "schemata": self.schemata,
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

        axes = []
        for axspec in scan["axes"]:
            if axspec["type"] == "refining":
                generator = RefiningGenerator(**axspec["range"])
            else:
                raise ScanSpecError("Axis type '{}' not implemented".format(axspec["type"]))

            fqn = axspec["fqn"]
            pathspec = axspec["path"]
            store = ParamStore(generator.points_for_level(0)[0])
            param_stores.setdefault(fqn, []).append({"path": pathspec, "store": store})
            axes.append(ScanAxis(self.schemata[fqn], pathspec, store, generator))
        self._scan = ScanSpec(axes)

        self.fragment._apply_param_overrides(param_stores)

        chan_dict = {}
        self.fragment._collect_result_channels(chan_dict)

        chan_name_map = shorten_to_unambiguous_suffixes(
            chan_dict.keys(),
            lambda fqn, n: "/".join(fqn.split("/")[-n:]))

        self.channels = {}
        for path, channel in chan_dict.items():
            name = chan_name_map[path].replace("/", "_")
            self.channels[name] = channel
            def make_cb(name):
                return lambda v: self._broadcast_result(name, v)
            channel.set_result_callback(make_cb(name))

    def run(self):
        self._broadcast_metadata()
        self._issue_ccb()

        if self._scan.is_continuous():
            self._run_continuous()
        else:
            self._run_scan()

        self._set_completed()

    def analyze(self):
        # See whether there are any default fits set up for the chosen
        # parameter(s), otherwise call:
        self.fragment.analyze()

    def _run_continuous(self):
        with suppress(TerminationRequested):
            while True:
                self.fragment.host_setup()
                self._point_phase = False
                if will_spawn_kernel(self.fragment.run_once):
                    self._krun_continuous()
                    self.core.comm.close()
                else:
                    self._continuous_loop()
                self.scheduler.pause()

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
            self._broadcast_point_phase()

    def _run_scan(self):
        # TODO: This is the most simple implementation possible to get a 1D PoC working;
        # the interesting bits (scheduling multidimensional scans on core device) are still
        # to be implemented.

        if len(self._scan.axes) > 1:
            raise ScanSpecError("Multidimensional scans not yet implemented")

        axis = self._scan.axes[0]
        level = 0

        with suppress(TerminationRequested):
            self.fragment.host_setup()
            while axis.generator.has_level(level):
                points = axis.generator.points_for_level(level)

                # TODO: Make configurable, use defined random generator with saved seed.
                random.shuffle(points)

                for p in points:
                    axis.param_store.set_value(p)
                    self.append_to_dataset("ndscan.points.axis_0", p)

                    # TODO: Use device_reset after first run.
                    self.fragment.device_setup()
                    self.fragment.run_once()
                    self.scheduler.pause()

                level += 1

    def _set_completed(self):
        self.set_dataset("ndscan.completed", True, broadcast=True)

    def _broadcast_metadata(self):
        def set(name, value):
            self.set_dataset("ndscan." + name, value, broadcast=True)

        set("fragment_fqn", self.fragment.fqn)
        set("rid", self.scheduler.rid)
        set("completed", False)

        axes = [ax.describe() for ax in self._scan.axes]
        set("axes", json.dumps(axes))

        channels = {name: channel.describe() for (name, channel) in self.channels.items()}
        set("channels", json.dumps(channels))

    def _broadcast_result(self, channel_name, value):
        if self._scan.is_continuous():
            self.set_dataset("ndscan.point.{}".format(channel_name), value, broadcast=True)
        else:
            self.append_to_dataset("ndscan.points.channel_{}".format(channel_name), value)

    @rpc(flags={"async"})
    def _broadcast_point_phase(self):
        self._point_phase = not self._point_phase
        self.set_dataset("ndscan.point_phase", self._point_phase, broadcast=True)

    def _issue_ccb(self):
        cmd = "${python} -m ndscan.applet --server=${server} --port=${port_notify}"
        cmd += " --rid={}".format(self.scheduler.rid)
        self.ccb.issue("create_applet", "ndscan: " + self.fragment.fqn, cmd,
            group="ndscan", is_transient=True)


def make_fragment_scan_exp(fragment_class: Type[ExpFragment]):
    class FragmentScanShim(FragmentScanExperiment):
        def build(self):
            super().build(lambda: fragment_class(self, []))

    doc = fragment_class.__doc__
    if not doc:
        doc = fragment_class.__name__
    FragmentScanShim.__doc__ = __doc__

    return FragmentScanShim
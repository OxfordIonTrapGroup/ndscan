import itertools
import json
import logging
import numpy as np
import random

from artiq.language import *
from artiq.protocols import pyon
from collections import OrderedDict
from contextlib import suppress
from typing import Callable, Dict, List, Type
from .fragment import Fragment, ExpFragment, type_string_to_param
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

    def points_for_level(self, level: int, rng=None):
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
    def __init__(self, axes, randomise_order_globally=True, seed=None):
        self.axes = axes
        self.randomise_order_globally = randomise_order_globally

        if seed is None:
            seed = random.getrandbits(32)
        self.seed = seed

    def is_continuous(self) -> bool:
        return not self.axes


def generate_points(scan: ScanSpec):
    rng = np.random.RandomState(scan.seed)

    # Stores computed coordinates for each axis, indexed first by
    # axis order, then by level.
    points_for_axes = [[]] * len(scan.axes)

    max_level = 0
    while True:
        new_axes = False
        for i, a in enumerate(scan.axes):
            if a.generator.has_level(max_level):
                points_for_axes[i].append(a.generator.points_for_level(max_level, rng))
                new_axes = True

        if not new_axes:
            # No levels left to exhaust, done.
            return

        points = []

        for axis_levels in itertools.product(*(range(0, len(p)) for p in points_for_axes)):
            if all(l < max_level for l in axis_levels):
                # Previously visited this combination already.
                continue
            tp = itertools.product(*(p[l] for (l, p) in zip(axis_levels, points_for_axes)))
            points.extend(tp)

        if scan.randomise_order_globally:
            rng.shuffle(points)

        for p in points:
            yield p

        max_level += 1


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
        # Collect parameters to set from both scan axes and simple overrides.
        param_stores = {}
        for fqn, specs in self._params.get("overrides", {}).items():
            store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            param_stores[fqn] = [{"path": s["path"], "store": store_type(s["value"])} for s in specs]

        scan = self._params.get("scan", {})

        axes = []
        for axspec in scan["axes"]:
            if axspec["type"] == "refining":
                generator = RefiningGenerator(**axspec["range"])
            else:
                raise ScanSpecError("Axis type '{}' not implemented".format(axspec["type"]))

            fqn = axspec["fqn"]
            pathspec = axspec["path"]

            store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            store = store_type(generator.points_for_level(0)[0])
            param_stores.setdefault(fqn, []).append({"path": pathspec, "store": store})
            axes.append(ScanAxis(self.schemata[fqn], pathspec, store, generator))

        self._scan = ScanSpec(axes)

        self.fragment._apply_param_overrides(param_stores)

        # Initialise result channels.
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
                    self._run_continuous_kernel()
                    self.core.comm.close()
                else:
                    self._continuous_loop()
                self.scheduler.pause()

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

    def _run_scan(self):
        # TODO: Handle parameters requiring host setup.
        self.fragment.host_setup()

        points = generate_points(self._scan)

        if will_spawn_kernel(self.fragment.run_once):
            self._run_scan_on_kernel(points)
        else:
            self._run_scan_on_host(points)

    def _run_scan_on_host(self, points):
        with suppress(TerminationRequested):
            while True:
                axis_values = next(points, None)
                if axis_values is None:
                    break
                for i, (a, p) in enumerate(zip(self._scan.axes, axis_values)):
                    a.param_store.set_value(p)
                    self.append_to_dataset("ndscan.points.axis_{}".format(i), p)

                self.fragment.device_setup()
                self.fragment.run_once()
                self.scheduler.pause()
            self._set_completed()

    def _run_scan_on_kernel(self, points):
        # Set up members to be accessed from the kernel through the
        # _kscan_param_values_chunk RPC call later.
        self._kscan_points = points

        # Stash away points in current kernel chunk until they have been marked completed
        # as a quick shortcut to be able to resume from interruptions. This should be cleaned
        # up a bit later. Alternatively, if we use an (async, but still) RPC to keep track of
        # points completed, we might as well use it to send back all the result channel values
        # from the core device in one go.
        self._kscan_current_chunk = []

        initial_chunk = self._kscan_param_values_chunk()
        for i, values in enumerate(initial_chunk):
            setattr(self, "_kscan_param_values_{}".format(i), values)

        for i, axis in enumerate(self._scan.axes):
            setattr(self, "_kscan_param_setter_{}".format(i), axis.param_store.set_value)

        # _kscan_param_values_chunk returns a tuple of lists of values, one for each scan
        # axis. Synthesize a return type annotation (`def foo(self): -> …`) with the concrete
        # type for this scan so the compiler can infer the types in _kscan_impl() correctly.
        self._kscan_param_values_chunk.__func__.__annotations__ = {
            "return": TTuple([TList(type_string_to_param(a.param_schema["type"]).CompilerType) for a in self._scan.axes])
        }

        # TODO: Implement pausing logic.
        # FIXME: Replace this with generated code once eval_kernel() is implemented.
        num_dims = len(self._scan.axes)
        scan_impl = getattr(self, "_kscan_impl_{}".format(num_dims), None)
        if scan_impl is None:
            raise NotImplementedError("{}-dimensional scans not supported yet".format(num_dims))
        scan_impl()

    @kernel
    def _kscan_impl_1(self):
        while True:
            (param_values_0,) = self._kscan_param_values_chunk()
            for i in range(len(param_values_0)):
                self._kscan_param_setter_0(param_values_0[i])
                self.fragment.device_setup()
                self.fragment.run_once()
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
                self.fragment.device_setup()
                self.fragment.run_once()
                self._kscan_point_completed()
            if self.scheduler.check_pause():
                return

    def _kscan_param_values_chunk(self):
        # Chunk size could be chosen adaptively in the future based on wall clock time
        # per point to provide good responsitivity to pause/terminate requests while
        # keeping RPC latency overhead low.
        CHUNK_SIZE = 10

        self._kscan_current_chunk.extend(itertools.islice(self._kscan_points,
            CHUNK_SIZE - len(self._kscan_current_chunk)))

        values = ([],) * len(self._scan.axes)
        for p in self._kscan_current_chunk:
            for i, v in enumerate(p):
                values[i].append(v)
        return values

    @rpc(flags={"async"})
    def _kscan_point_completed(self):
        self._kscan_current_chunk.pop(0)

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

        set("seed", self._scan.seed)

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
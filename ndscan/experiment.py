import itertools
import json
import logging
import numpy as np
import random

from artiq.coredevice.exceptions import RTIOUnderflow
from artiq.language import *
from collections import OrderedDict
from contextlib import suppress
from typing import Callable, Dict, List, Type
from .fragment import Fragment, ExpFragment, type_string_to_param
from .result_channels import AppendingDatasetSink, ScalarDatasetSink
from .scan_generator import *
from .utils import shorten_to_unambiguous_suffixes, will_spawn_kernel

# We don't want to export FragmentScanExperiment to hide it from experiment
# class discovery.
__all__ = ["make_fragment_scan_exp", "PARAMS_ARG_KEY"]

PARAMS_ARG_KEY = "ndscan_params"

logger = logging.getLogger(__name__)


class ScanSpecError(Exception):
    pass


class ScanFinished(Exception):
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
        # Collect parameters to set from both scan axes and simple overrides.
        param_stores = {}
        for fqn, specs in self._params.get("overrides", {}).items():
            store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            param_stores[fqn] = [{
                "path": s["path"],
                "store": store_type((fqn, s["path"]), s["value"])
            } for s in specs]

        scan = self._params.get("scan", {})

        axes = []
        for axspec in scan["axes"]:
            generator_class = GENERATORS.get(axspec["type"], None)
            if not generator_class:
                raise ScanSpecError("Axis type '{}' not implemented".format(
                    axspec["type"]))
            generator = generator_class(**axspec["range"])

            fqn = axspec["fqn"]
            pathspec = axspec["path"]

            store_type = type_string_to_param(self.schemata[fqn]["type"]).StoreType
            store = store_type((fqn, pathspec),
                               generator.points_for_level(0, random)[0])
            param_stores.setdefault(fqn, []).append({"path": pathspec, "store": store})
            axes.append(ScanAxis(self.schemata[fqn], pathspec, store, generator))

        num_repeats = scan.get("num_repeats", 1)
        continuous_without_axes = scan.get("continuous_without_axes", True)
        randomise_order_globally = scan.get("randomise_order_globally", False)

        self._scan = ScanSpec(axes, num_repeats, continuous_without_axes,
                              randomise_order_globally)

        self.fragment.init_params(param_stores)

        # Initialise result channels.
        chan_dict = {}
        self.fragment._collect_result_channels(chan_dict)

        chan_name_map = shorten_to_unambiguous_suffixes(
            chan_dict.keys(), lambda fqn, n: "/".join(fqn.split("/")[-n:]))

        self.channels = {}
        self._channel_dataset_names = {}
        for path, channel in chan_dict.items():
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
        self._broadcast_metadata()
        self._issue_ccb()

        if not self._scan.axes:
            self._run_single()
        else:
            self._run_scan()

        self._set_completed()

    def analyze(self):
        pass

    def _run_single(self):
        try:
            with suppress(TerminationRequested):
                while True:
                    self.fragment.host_setup()
                    self._point_phase = False
                    if will_spawn_kernel(self.fragment.run_once):
                        self._run_continuous_kernel()
                        self.core.comm.close()
                    else:
                        self._continuous_loop()
                    if not self._scan.continuous_without_axes:
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
            if not self._scan.continuous_without_axes:
                return

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

        for i, axis in enumerate(self._scan.axes):
            setattr(self, "_kscan_param_setter_{}".format(i),
                    axis.param_store.set_value)

        # _kscan_param_values_chunk returns a tuple of lists of values, one for each scan
        # axis. Synthesize a return type annotation (`def foo(self): -> …`) with the concrete
        # type for this scan so the compiler can infer the types in _kscan_impl() correctly.
        self._kscan_param_values_chunk.__func__.__annotations__ = {
            "return":
            TTuple([
                TList(type_string_to_param(a.param_schema["type"]).CompilerType)
                for a in self._scan.axes
            ])
        }

        # TODO: Implement pausing logic.
        # FIXME: Replace this with generated code once eval_kernel() is implemented.
        num_dims = len(self._scan.axes)
        scan_impl = getattr(self, "_kscan_impl_{}".format(num_dims), None)
        if scan_impl is None:
            raise NotImplementedError(
                "{}-dimensional scans not supported yet".format(num_dims))

        # KLUDGE: Returning tuples of empty lists triggers bug in ARTIQ RPC code (kernel
        # aborts), so use an exception to signal end of scan.
        with suppress(ScanFinished, TerminationRequested):
            while True:
                scan_impl()
                self.core.comm.close()
                self.scheduler.pause()
        self._set_completed()

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
        self.fragment.device_setup()
        self.fragment.run_once()

    def _kscan_param_values_chunk(self):
        # Chunk size could be chosen adaptively in the future based on wall clock time
        # per point to provide good responsitivity to pause/terminate requests while
        # keeping RPC latency overhead low.
        CHUNK_SIZE = 10

        self._kscan_current_chunk.extend(
            itertools.islice(self._kscan_points,
                             CHUNK_SIZE - len(self._kscan_current_chunk)))

        values = tuple([] for _ in self._scan.axes)
        for p in self._kscan_current_chunk:
            for i, (value, axis) in enumerate(zip(p, self._scan.axes)):
                # KLUDGE: Explicitly coerce value to the target type here so we can use
                # the regular (float) scans for integers until proper support for int
                # scans is implemented.
                v = axis.param_store.coerce(value)
                self.append_to_dataset("ndscan.points.axis_{}".format(i), v)
                values[i].append(v)
        if not values[0]:
            raise ScanFinished
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

        # KLDUGE: Broadcast auto_fit before channels to allow simpler implementation
        # in current fit applet. As the applet implementation grows more sophisticated
        # (hiding axes, etc.), it should be easy to relax this requirement.

        fits = []
        axis_identities = [(s.param_schema["fqn"], s.path) for s in self._scan.axes]
        for f in self.fragment.get_default_fits():
            if f.has_data(axis_identities):
                fits.append(f.describe(
                    lambda identity: "axis_{}".format(axis_identities.index(identity)),
                    lambda path: self._channel_dataset_names[path]))
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
        cmd = "${python} -m ndscan.applet --server=${server} --port=${port_notify} --port-control=${port_control}"
        cmd += " --rid={}".format(self.scheduler.rid)
        self.ccb.issue(
            "create_applet",
            "ndscan: " + self.fragment.fqn,
            cmd,
            group="ndscan",
            is_transient=True)


def make_fragment_scan_exp(fragment_class: Type[ExpFragment]):
    class FragmentScanShim(FragmentScanExperiment):
        def build(self):
            super().build(lambda: fragment_class(self, []))

    doc = fragment_class.__doc__
    if not doc:
        doc = fragment_class.__name__
    FragmentScanShim.__doc__ = doc

    return FragmentScanShim

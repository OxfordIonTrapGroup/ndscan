"""
Tests that actually run against a core device (by default, the emulator).

Collecting them all in a single module might or might not turn out to be a good idea;
could also keep them inline with the other test_experiment_* unit test modules.
"""

import math
from collections import Counter
from dataclasses import dataclass
from enum import Enum, unique

import numpy as np
from artiq.language import kernel, rpc
from emulator_environment import KernelEmulatorCase
from fixtures import TrivialKernelFragment

from ndscan.experiment.entry_point import make_fragment_scan_exp, run_fragment_once
from ndscan.experiment.fragment import AggregateExpFragment, ExpFragment
from ndscan.experiment.parameters import (
    BoolParam,
    EnumParam,
    FloatParam,
    IntParam,
    StringParam,
)
from ndscan.experiment.result_channels import FloatChannel, IntChannel, OpaqueChannel
from ndscan.experiment.scan_generator import LinearGenerator, ListGenerator
from ndscan.experiment.subscan import SubscanExpFragment, setattr_subscan
from ndscan.utils import SCHEMA_REVISION, SCHEMA_REVISION_KEY


class RunOneKernelCase(KernelEmulatorCase):
    def test_run_once_kernel(self):
        fragment = self.create(TrivialKernelFragment, [])
        run_fragment_once(fragment)


@unique
class Colors(Enum):
    red = "a bright red"
    blue = "a deep blue"


@unique
class Numbers(Enum):
    one = 1
    two = 2


class SmorgasbordKernelFragment(ExpFragment):
    """Contains a mix of different parameter types to exercise kernel-side parameter
    handling with multiple different scanned types.

    For good measure, we call changed_after_use() on each type as well, and test that
    we can assign the values to member variables initialised with the host-side
    parameter type before.
    """

    def build_fragment(self) -> None:
        self.setattr_param("float", FloatParam, "Float", default=0.1)
        self.setattr_param("int", IntParam, "Int", default=42)
        self.setattr_param("string", StringParam, "String", default="'foo'")
        self.setattr_param("bool", BoolParam, "Bool", default=True)
        self.setattr_param("color", EnumParam, "Color", default=Colors.red)
        self.setattr_param("number", EnumParam, "Number", default=Numbers.one)

        self.setattr_result("float_result", FloatChannel)
        self.setattr_result("int_result", IntChannel)
        self.setattr_result("string_result", OpaqueChannel)
        self.setattr_result("bool_result", OpaqueChannel)
        self.setattr_result("color_result", OpaqueChannel)
        self.setattr_result("number_result", OpaqueChannel)

    def host_setup(self):
        self.float_val = self.float.get()
        self.int_val = self.int.get()
        self.string_val = self.string.get()
        self.bool_val = self.bool.get()
        self.color_val = self.color.get()
        self.number_val = self.number.get()

    @kernel
    def device_setup(self) -> None:
        if self.float.changed_after_use():
            self.float_val = self.float.use()
        if self.int.changed_after_use():
            self.int_val = self.int.use()
        if self.string.changed_after_use():
            self.string_val = self.string.use()
        if self.bool.changed_after_use():
            self.bool_val = self.bool.use()
        if self.color.changed_after_use():
            self.color_val = self.color.use()
        if self.number.changed_after_use():
            self.number_val = self.number.use()

    @kernel
    def run_once(self) -> None:
        self.float_result.push(self.float_val)
        self.int_result.push(self.int_val)
        self.string_result.push(self.string_val)
        self.bool_result.push(self.bool_val)
        self.color_result.push(self.color_val.value)
        self.number_result.push(self.number_val.value)


ScanSmorgasbordKernelFragment = make_fragment_scan_exp(SmorgasbordKernelFragment)


@dataclass
class ListScanDef:
    param_name: str
    schema_values: list
    result_values: list


class TestSmorgasbordKernelCase(KernelEmulatorCase):
    def test_smorgasbord_scan(self):
        exp = self.create(ScanSmorgasbordKernelFragment)
        scan_defs = [
            ListScanDef("float", [0.1, 0.2], [0.1, 0.2]),
            ListScanDef("int", [0, 1], [0, 1]),
            # FIXME: String scans currently cause memory corruption during attribute
            # writeback (unavoidable as escaping an array, not caught due to
            # https://github.com/m-labs/artiq/issues/1394).
            # ListScanDef("string", ["'foo'", "'bar'"], ["foo", "bar"]),
            ListScanDef("bool", [True, False], [True, False]),
            ListScanDef("color", ["red", "blue"], ["a bright red", "a deep blue"]),
            ListScanDef("number", ["one", "two"], [1, 2]),
        ]
        fragment_fqn = "test_experiment_kernel.SmorgasbordKernelFragment"

        def fqn(name):
            return f"{fragment_fqn}.{name}"

        for scan_def in scan_defs:
            exp.args._params["scan"]["axes"].append(
                {
                    "fqn": fqn(scan_def.param_name),
                    "path": "*",
                    "type": "list",
                    "range": {
                        "values": scan_def.schema_values,
                        "randomise_order": True,
                    },
                }
            )
        exp.prepare()
        exp.run()

        def d(key):
            return self.dataset_db.get("ndscan.rid_0." + key)

        self.assertEqual(d(SCHEMA_REVISION_KEY), SCHEMA_REVISION)
        self.assertEqual(d("completed"), True)
        self.assertEqual(d("fragment_fqn"), fragment_fqn)
        self.assertEqual(d("source_id"), "rid_0")
        num_points = math.prod(len(scan_def.schema_values) for scan_def in scan_defs)
        for i, scan_def in enumerate(scan_defs):
            num_repeats = num_points // len(scan_def.schema_values)
            self.assertEqual(
                Counter(d(f"points.axis_{i}")),
                Counter({k: num_repeats for k in scan_def.schema_values}),
            )
            self.assertEqual(
                Counter(d(f"points.channel_{scan_def.param_name}_result")),
                Counter({k: num_repeats for k in scan_def.result_values}),
            )


# # #


class OneTrivial(AggregateExpFragment):
    def build_fragment(self):
        return super().build_fragment(
            [self.setattr_fragment("a", TrivialKernelFragment)]
        )


class TwoTrivial(AggregateExpFragment):
    def build_fragment(self):
        return super().build_fragment(
            [
                self.setattr_fragment("a", TrivialKernelFragment),
                self.setattr_fragment("b", TrivialKernelFragment),
            ]
        )


class TwoPlusOneTrivial(AggregateExpFragment):
    def build_fragment(self):
        return super().build_fragment(
            [
                self.setattr_fragment("a", TwoTrivial),
                self.setattr_fragment("b", OneTrivial),
            ]
        )


TwoPlusOneTrivialScan = make_fragment_scan_exp(TwoPlusOneTrivial)


class TestAggregateCase(KernelEmulatorCase):
    def test_two_plus_one(self):
        exp = self.create(TwoPlusOneTrivialScan)
        exp.prepare()
        exp.run()


# # #


class Inner(ExpFragment):
    def build_fragment(self) -> None:
        self.setattr_device("core")
        self.setattr_param("param_float", FloatParam, "Float param", default=0.0)
        self.setattr_param("param_int", IntParam, "Int param", default=0.0)
        self.setattr_result("result", FloatChannel)

    @kernel
    def run_once(self) -> None:
        self.result.push(self.param_float.get() + self.param_int.get())


INT_GEN = ListGenerator([-1, 0, 1], randomise_order=False)
FLOAT_GEN = LinearGenerator(-1.0, 1.0, 11, randomise_order=False)


class FloatSetattrSubscan(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("inner", Inner)
        setattr_subscan(self, "float_scan", self.inner, [(self.inner, "param_float")])

    def host_setup(self):
        self.float_scan.set_scan_spec([(self.inner.param_float, FLOAT_GEN)])
        super().host_setup()

    @kernel
    def run_once(self) -> None:
        self.float_scan.acquire()


class IntSetattrSubscan(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("inner", Inner)
        setattr_subscan(self, "int_scan", self.inner, [(self.inner, "param_int")])

    def host_setup(self):
        self.int_scan.set_scan_spec([(self.inner.param_int, INT_GEN)])

    @kernel
    def run_once(self) -> None:
        self.int_scan.acquire()


class SetattrParent(AggregateExpFragment):
    def build_fragment(self) -> None:
        self.setattr_fragment("int_frag", IntSetattrSubscan)
        self.setattr_fragment("float_frag", FloatSetattrSubscan)
        super().build_fragment([self.int_frag, self.float_frag])


SetattrParentScan = make_fragment_scan_exp(SetattrParent)


class FloatSubscanExpFragment(SubscanExpFragment):
    pass


class FloatFragmentSubscan(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("inner", Inner)
        self.setattr_fragment(
            "scan",
            FloatSubscanExpFragment,
            self,
            self.inner,
            [(self.inner, "param_float")],
        )

    def host_setup(self):
        self.scan.configure([(self.inner.param_float, FLOAT_GEN)])
        super().host_setup()

    @kernel
    def run_once(self) -> None:
        self.scan.run_once()


class IntSubscanExpFragment(SubscanExpFragment):
    pass


class IntFragmentSubscan(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("inner", Inner)
        self.setattr_fragment(
            "scan",
            IntSubscanExpFragment,
            self,
            self.inner,
            [(self.inner, "param_int")],
        )

    def host_setup(self):
        self.scan.configure([(self.inner.param_int, INT_GEN)])

    @kernel
    def run_once(self) -> None:
        self.scan.run_once()


class FragmentSubscanParent(AggregateExpFragment):
    def build_fragment(self) -> None:
        self.setattr_fragment("int_frag", IntFragmentSubscan)
        self.setattr_fragment("float_frag", FloatFragmentSubscan)
        super().build_fragment([self.int_frag, self.float_frag])


FragmentSubscanParentScan = make_fragment_scan_exp(FragmentSubscanParent)


class FloatSubclassSubscan(SubscanExpFragment):
    def build_fragment(self):
        self.setattr_fragment("inner", Inner)
        super().build_fragment(
            self,
            self.inner,
            [(self.inner, "param_float")],
        )

    def host_setup(self):
        self.configure([(self.inner.param_float, FLOAT_GEN)])
        super().host_setup()


class IntSubclassSubscan(SubscanExpFragment):
    def build_fragment(self):
        self.setattr_fragment("inner", Inner)
        super().build_fragment(
            self,
            self.inner,
            [(self.inner, "param_int")],
        )

    def host_setup(self):
        self.configure([(self.inner.param_int, INT_GEN)])
        super().host_setup()


class SubclassSubscanParent(AggregateExpFragment):
    def build_fragment(self) -> None:
        self.setattr_fragment("int_frag", IntSubclassSubscan)
        self.setattr_fragment("float_frag", FloatSubclassSubscan)
        super().build_fragment([self.int_frag, self.float_frag])


SubclassSubscanParentScan = make_fragment_scan_exp(SubclassSubscanParent)


class TestSubscanKernelCase(KernelEmulatorCase):
    def _test_subscan(self, cls, fragment_fqn, float_channel_name, int_channel_name):
        exp = self.create(cls)
        exp.prepare()
        exp.run()

        def d(key):
            return self.dataset_db.get("ndscan.rid_0." + key)

        self.assertEqual(d(SCHEMA_REVISION_KEY), SCHEMA_REVISION)
        self.assertEqual(d("completed"), True)
        self.assertEqual(d("fragment_fqn"), fragment_fqn)
        self.assertEqual(d("source_id"), "rid_0")

        np.testing.assert_array_max_ulp(
            d(f"point.{float_channel_name}"), FLOAT_GEN.points_for_level(0)
        )
        np.testing.assert_array_max_ulp(
            d(f"point.{int_channel_name}"), INT_GEN.points_for_level(0)
        )

    def test_setattr_subscan(self):
        self._test_subscan(
            SetattrParentScan,
            "test_experiment_kernel.SetattrParent",
            "float_scan_channel_result",
            "int_scan_channel_result",
        )

    def test_fragment_subscan(self):
        self._test_subscan(
            FragmentSubscanParentScan,
            "test_experiment_kernel.FragmentSubscanParent",
            "float_frag_scan__channel_result",
            "int_frag_scan__channel_result",
        )

    def test_subclass_subscan(self):
        self._test_subscan(
            SubclassSubscanParentScan,
            "test_experiment_kernel.SubclassSubscanParent",
            "float_frag__channel_result",
            "int_frag__channel_result",
        )


# # #


class KernelAddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("value", FloatParam, "Value to return", 0.0)
        self.setattr_result("result", FloatChannel)

        self.num_prepare_calls = 0
        self.num_host_setup_calls = 0
        self.num_device_setup_calls = 0
        self.num_host_cleanup_calls = 0
        self.num_device_cleanup_calls = 0

    def prepare(self):
        self.num_prepare_calls += 1

    def host_setup(self):
        self.num_host_setup_calls += 1

    @kernel
    def device_setup(self):
        self.num_device_setup_calls += 1

    def host_cleanup(self):
        self.num_host_cleanup_calls += 1

    @kernel
    def device_cleanup(self):
        self.num_device_cleanup_calls += 1

    @kernel
    def run_once(self):
        self.result.push(self.value.get() + 1)


KernelAddOneFragmentScan = make_fragment_scan_exp(KernelAddOneFragment)


class KernelAddOneFragmentSubscan(SubscanExpFragment):
    def build_fragment(self):
        self.setattr_fragment("add_one", KernelAddOneFragment)
        self.setattr_param(
            "num_scan_points", IntParam, "Number of subscan points", default=3
        )
        super().build_fragment(self, self.add_one, [(self.add_one, "value")])

    @rpc(flags={"async"})
    def configure_scan(self):
        if self.num_scan_points.changed_after_use():
            self.configure(
                [
                    (
                        self.add_one.value,
                        LinearGenerator(
                            0.0,
                            1.0,
                            num_points=self.num_scan_points.use(),
                            randomise_order=True,
                        ),
                    )
                ]
            )

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


KernelAddOneFragmentSubscanScan = make_fragment_scan_exp(KernelAddOneFragmentSubscan)


class TestLifetimeCountsCase(KernelEmulatorCase):
    def test_direct_counts(self):
        exp = self.create(KernelAddOneFragmentScan)
        values = [0.0, 1.0, 2.0]
        fragment_fqn = "test_experiment_kernel.KernelAddOneFragment"

        exp.args._params["scan"]["axes"].append(
            {
                "fqn": f"{fragment_fqn}.value",
                "path": "*",
                "type": "list",
                "range": {
                    "values": values,
                    "randomise_order": True,
                },
            }
        )

        exp.prepare()
        exp.run()

        f: KernelAddOneFragment = exp.fragment

        self.assertEqual(f.num_prepare_calls, 1)
        self.assertEqual(f.num_host_setup_calls, 1)
        self.assertEqual(f.num_device_setup_calls, len(values))
        self.assertEqual(f.num_device_cleanup_calls, 1)
        self.assertEqual(f.num_host_cleanup_calls, 1)

    def test_subscan_counts(self):
        exp = self.create(KernelAddOneFragmentSubscanScan)
        exp.prepare()
        exp.run()

        f: KernelAddOneFragment = exp.fragment.add_one

        # FIXME: prepare() is currently not forwarded (but probably should be?)
        # self.assertEqual(f.num_prepare_calls, 1)
        self.assertEqual(f.num_host_setup_calls, 1)
        self.assertEqual(f.num_device_setup_calls, 3)
        self.assertEqual(f.num_device_cleanup_calls, 1)
        self.assertEqual(f.num_host_cleanup_calls, 1)

    def test_subscan_scan_counts(self):
        exp = self.create(KernelAddOneFragmentSubscanScan)
        fragment_fqn = "test_experiment_kernel.KernelAddOneFragmentSubscan"

        num_pointss = [2, 3, 4]
        exp.args._params["scan"]["axes"].append(
            {
                "fqn": f"{fragment_fqn}.num_scan_points",
                "path": "*",
                "type": "list",
                "range": {
                    "values": num_pointss,
                    "randomise_order": True,
                },
            }
        )

        exp.prepare()
        exp.run()

        f: KernelAddOneFragment = exp.fragment.add_one
        # FIXME: prepare() is currently not forwarded (but probably should be?)
        # self.assertEqual(f.num_prepare_calls, 1)
        self.assertEqual(f.num_host_setup_calls, 1)
        self.assertEqual(f.num_device_setup_calls, sum(num_pointss))
        self.assertEqual(f.num_device_cleanup_calls, 1)
        self.assertEqual(f.num_host_cleanup_calls, 1)

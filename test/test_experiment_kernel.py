"""
Tests that actually run against a core device (by default, the emulator).

Collecting them all in a single module might or might not turn out to be a good idea;
could also keep them inline with the other test_experiment_* unit test modules.
"""

import math
from collections import Counter
from dataclasses import dataclass
from enum import Enum, unique

from artiq.language import kernel
from emulator_environment import KernelEmulatorCase
from fixtures import TrivialKernelFragment

from ndscan.experiment.entry_point import make_fragment_scan_exp, run_fragment_once
from ndscan.experiment.fragment import ExpFragment
from ndscan.experiment.parameters import (
    BoolParam,
    EnumParam,
    FloatParam,
    IntParam,
    StringParam,
)
from ndscan.experiment.result_channels import FloatChannel, IntChannel, OpaqueChannel
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

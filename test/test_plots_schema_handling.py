import json

from mock_environment import *

from ndscan.experiment import *
from ndscan.plots.utils import *


class NestedFragment(Fragment):
    def build_fragment(self):
        self.setattr_result("g", FloatChannel)
        self.setattr_result(
            "h", FloatChannel, display_hints={"share_axis_with": self.g.path}
        )

    def run_once(self):
        self.g.push(0.0)
        self.h.push(0.0)


class TestFragment(ExpFragment):
    def build_fragment(self):
        # Manually specify share target path to exercise different orders in
        # grouping code.
        self.setattr_result("a", FloatChannel, display_hints={"share_axis_with": "d"})
        self.setattr_result("b", IntChannel, display_hints={"priority": 1})
        self.setattr_result("c", OpaqueChannel)
        self.setattr_result("d", FloatChannel, display_hints={"share_axis_with": "e"})
        self.setattr_result("e", FloatChannel)
        self.setattr_result(
            "f", FloatChannel, display_hints={"share_axis_with": self.b.path}
        )

        self.setattr_result(
            "a_err", FloatChannel, display_hints={"error_bar_for": self.a.path}
        )

        self.setattr_fragment("n0", NestedFragment)
        self.setattr_fragment("n1", NestedFragment)

        # Also try manually adding display hints to channels from subfragments.
        self.setattr_fragment("n2", NestedFragment)
        self.setattr_fragment("n3", NestedFragment)
        self.n3.g.display_hints["share_axis_with"] = self.n2.g.path

    def run_once(self):
        self.a.push(0.0)
        self.b.push(0)
        self.c.push(0)
        self.d.push(0.0)
        self.e.push(0.0)
        self.f.push(0.0)
        self.a_err.push(0.0)
        self.n0.run_once()
        self.n1.run_once()
        self.n2.run_once()
        self.n3.run_once()


TestExp = make_fragment_scan_exp(TestFragment)


class FragmentScanExpCase(HasEnvironmentCase):
    def test_scalar_channels(self):
        exp = self.create(TestExp)
        exp.prepare()
        exp.run()

        channels = json.loads(self.dataset_db.get("ndscan.rid_0.channels"))

        data_names, error_bar_names = extract_scalar_channels(channels)
        self.assertEqual(
            data_names,
            [
                "b",
                "a",
                "d",
                "e",
                "f",
                "n0_g",
                "n0_h",
                "n1_g",
                "n1_h",
                "n2_g",
                "n2_h",
                "n3_g",
                "n3_h",
            ],
        )
        self.assertEqual(error_bar_names, {"a": "a_err"})

        groups = group_channels_into_axes(channels, data_names)
        self.assertEqual(
            groups,
            [
                ["b", "f"],
                ["a", "d", "e"],
                ["n0_g", "n0_h"],
                ["n1_g", "n1_h"],
                ["n2_g", "n2_h", "n3_g", "n3_h"],
            ],
        )

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


class TestFragment(ExpFragment):
    def build_fragment(self):
        # Manually specify root path to exercise different orders in .
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

import json
from ndscan.experiment import *
from ndscan.plots.utils import *
from mock_environment import *


class TestFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_result("a", FloatChannel)
        self.setattr_result("b", IntChannel)
        self.setattr_result("c", OpaqueChannel)

        self.setattr_result("a_err",
                            FloatChannel,
                            display_hints={"error_bar_for": self.a.path})


TestExp = make_fragment_scan_exp(TestFragment)


class FragmentScanExpCase(HasEnvironmentCase):
    def test_extract_scalar_channels(self):
        exp = self.create(TestExp)
        exp._params["scan"]["continuous_without_axes"] = False
        exp.prepare()
        exp.run()

        channels = json.loads(self.dataset_db.get("ndscan.channels"))
        self.assertEqual(extract_scalar_channels(channels), (["a", "b"], {
            "a": "a_err"
        }))

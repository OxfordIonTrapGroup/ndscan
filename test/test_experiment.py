"""
Tests for ndscan.experiment top-level runners.
"""

import json
from ndscan.experiment import make_fragment_scan_exp
from fixtures import EchoFragment
from mock_environment import HasEnvironmentCase

ScanEchoExp = make_fragment_scan_exp(EchoFragment)


class FragmentScanExpCase(HasEnvironmentCase):
    def test_run_trivial_scan(self):
        exp = self.create(ScanEchoExp)
        exp._params["scan"]["continuous_without_axes"] = False
        exp.prepare()
        exp.run()

        def d(key):
            return self.dataset_db.get("ndscan." + key)

        self.assertEqual(json.loads(d("axes")), [])
        self.assertEqual(d("fragment_fqn"), "fixtures.EchoFragment")
        self.assertEqual(d("rid"), 0)

    def test_run_1d_scan(self):
        exp = self.create(ScanEchoExp)
        fqn = "fixtures.EchoFragment.value"
        exp._params["scan"]["axes"].append({
            "type": "linear",
            "range": {
                "start": 0,
                "stop": 2,
                "num_points": 3,
                "randomise_order": False
            },
            "fqn": fqn,
            "path": "*"
        })
        exp.prepare()
        exp.run()

        def d(key):
            return self.dataset_db.get("ndscan." + key)

        self.assertEqual(
            json.loads(d("axes")), [{
                "increment": 1.0,
                "max": 2,
                "min": 0,
                "param": {
                    "default": "0.0",
                    "description": "Value to return",
                    "fqn": fqn,
                    "spec": {
                        "scale": 1.0,
                        "step": 0.1
                    },
                    "type": "float"
                },
                "path": "*"
            }])
        self.assertEqual(d("points.axis_0"), [0, 1, 2])
        self.assertEqual(d("points.channel_result"), [1, 2, 3])
        self.assertEqual(d("fragment_fqn"), "fixtures.EchoFragment")
        self.assertEqual(d("rid"), 0)

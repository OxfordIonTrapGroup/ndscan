"""
Tests for ndscan.experiment top-level runners.
"""

import json
from artiq.language import HasEnvironment
from ndscan.experiment import (make_fragment_scan_exp, run_fragment_once,
                               create_and_run_fragment_once)
from fixtures import AddOneFragment, ReboundAddOneFragment, TrivialKernelFragment
from mock_environment import HasEnvironmentCase

ScanAddOneExp = make_fragment_scan_exp(AddOneFragment)
ScanReboundAddOneExp = make_fragment_scan_exp(ReboundAddOneFragment)


class FragmentScanExpCase(HasEnvironmentCase):
    def test_run_trivial_scan(self):
        exp = self.create(ScanAddOneExp)
        exp._params["scan"]["continuous_without_axes"] = False
        exp.prepare()
        exp.run()

        def d(key):
            return self.dataset_db.get("ndscan." + key)

        self.assertEqual(json.loads(d("axes")), [])
        self.assertEqual(d("completed"), True)
        self.assertEqual(d("fragment_fqn"), "fixtures.AddOneFragment")
        self.assertEqual(d("rid"), 0)

    def test_run_1d_scan(self):
        self._test_run_1d(ScanAddOneExp, "fixtures.AddOneFragment")
        self.assertEqual(
            json.loads(self.dataset_db.get("ndscan.auto_fit")), [{
                "data": {
                    "x": "axis_0",
                    "y": "channel_result"
                },
                "fit_type": "lorentzian",
                "pois": [{
                    "x": "x0"
                }]
            }])

    def test_run_rebound_1d_scan(self):
        self._test_run_1d(ScanReboundAddOneExp, "fixtures.ReboundAddOneFragment")

    def _test_run_1d(self, klass, fragment_fqn):
        exp = self.create(klass)
        fqn = fragment_fqn + ".value"
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
        self.assertEqual(d("completed"), True)
        self.assertEqual(d("points.axis_0"), [0, 1, 2])
        self.assertEqual(d("points.channel_result"), [1, 2, 3])
        self.assertEqual(d("fragment_fqn"), fragment_fqn)
        self.assertEqual(d("rid"), 0)


class RunOnceCase(HasEnvironmentCase):
    def test_run_once_host(self):
        fragment = self.create(AddOneFragment, [])
        self.assertEqual(run_fragment_once(fragment), {fragment.result: 1.0})

    def test_run_once_kernel(self):
        fragment = self.create(TrivialKernelFragment, [])
        run_fragment_once(fragment)
        self.assertEqual(self.core.run.call_count, 1)

    def test_create_and_run_once(self):
        self.assertEqual(
            create_and_run_fragment_once(self.create(HasEnvironment), AddOneFragment),
            {"result": 1.0})

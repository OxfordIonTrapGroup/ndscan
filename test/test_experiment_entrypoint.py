"""
Tests for ndscan.experiment top-level runners.
"""

import json
from artiq.language import HasEnvironment
from ndscan.experiment import *
from ndscan.utils import SCHEMA_REVISION, SCHEMA_REVISION_KEY
from fixtures import (AddOneFragment, ReboundAddOneFragment, TrivialKernelFragment,
                      TransitoryErrorFragment)
from mock_environment import HasEnvironmentCase

ScanAddOneExp = make_fragment_scan_exp(AddOneFragment)
ScanReboundAddOneExp = make_fragment_scan_exp(ReboundAddOneFragment)


class FragmentScanExpCase(HasEnvironmentCase):
    def test_run_trivial_scan(self):
        self.dataset_db.data["system_id"] = (True, "system")
        exp = self.create(ScanAddOneExp)
        exp.prepare()
        exp.run()

        self.assertEqual(exp.fragment.num_host_setup_calls, 1)
        self.assertEqual(exp.fragment.num_device_setup_calls, 1)
        self.assertEqual(exp.fragment.num_host_cleanup_calls, 1)
        self.assertEqual(exp.fragment.num_device_cleanup_calls, 1)

        def d(key):
            return self.dataset_db.get("ndscan." + key)

        self.assertEqual(d(SCHEMA_REVISION_KEY), SCHEMA_REVISION)
        self.assertEqual(json.loads(d("axes")), [])
        self.assertEqual(d("completed"), True)
        self.assertEqual(d("fragment_fqn"), "fixtures.AddOneFragment")
        self.assertEqual(d("source_id"), "system_0")

    def test_run_1d_scan(self):
        exp = self._test_run_1d(ScanAddOneExp, "fixtures.AddOneFragment")
        self.assertEqual(exp.fragment.num_host_setup_calls, 1)
        self.assertEqual(exp.fragment.num_device_setup_calls, 3)
        self.assertEqual(exp.fragment.num_host_cleanup_calls, 1)
        self.assertEqual(exp.fragment.num_device_cleanup_calls, 1)

        curve_annotation = {
            "kind": "computed_curve",
            "parameters": {
                "function_name": "lorentzian",
                "associated_channels": ["channel_result"]
            },
            "coordinates": {},
            "data": {
                "a": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "a"
                },
                "fwhm": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "fwhm"
                },
                "x0": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0"
                },
                "y0": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "y0"
                }
            }
        }
        location_annotation = {
            "kind": "location",
            "parameters": {
                "associated_channels": ["channel_result"]
            },
            "coordinates": {
                "axis_0": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0"
                }
            },
            "data": {
                "axis_0_error": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0_error"
                }
            }
        }
        self.assertEqual(json.loads(self.dataset_db.get("ndscan.annotations")),
                         [curve_annotation, location_annotation])

        self.assertEqual(
            json.loads(self.dataset_db.get("ndscan.online_analyses")), {
                "fit_lorentzian_channel_result": {
                    "constants": {
                        "y0": 1.0
                    },
                    "data": {
                        "y": "channel_result",
                        "x": "axis_0"
                    },
                    "fit_type": "lorentzian",
                    "initial_values": {
                        "fwhm": 2.0
                    },
                    "kind": "named_fit"
                }
            })

    def test_run_rebound_1d_scan(self):
        exp = self._test_run_1d(ScanReboundAddOneExp, "fixtures.ReboundAddOneFragment")
        self.assertEqual(exp.fragment.add_one.num_host_setup_calls, 1)
        self.assertEqual(exp.fragment.add_one.num_device_setup_calls, 3)
        self.assertEqual(exp.fragment.add_one.num_host_cleanup_calls, 1)
        self.assertEqual(exp.fragment.add_one.num_device_cleanup_calls, 1)

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

        self.assertEqual(json.loads(d("axes")), [{
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
        self.assertEqual(d(SCHEMA_REVISION_KEY), SCHEMA_REVISION)
        self.assertEqual(d("completed"), True)
        self.assertEqual(d("points.axis_0"), [0, 1, 2])
        self.assertEqual(d("points.channel_result"), [1, 2, 3])
        self.assertEqual(d("fragment_fqn"), fragment_fqn)
        self.assertEqual(d("source_id"), "rid_0")

        return exp


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

    def test_run_once_transitory_errors(self):
        fragment = self.create(TransitoryErrorFragment, [])
        fragment.num_device_setup_to_fail = 3
        fragment.num_device_setup_to_restart_fail = 3
        fragment.num_run_once_to_fail = 3
        fragment.num_run_once_to_restart_fail = 3

        self.assertEqual(run_fragment_once(fragment, max_transitory_error_retries=12),
                         {fragment.result: 42})

        self.assertEqual(fragment.num_device_setup_to_fail, 0)
        self.assertEqual(fragment.num_device_setup_to_restart_fail, 0)
        self.assertEqual(fragment.num_run_once_to_fail, 0)
        self.assertEqual(fragment.num_run_once_to_restart_fail, 0)

    def test_run_once_transitory_error_limit(self):
        fragment = self.create(TransitoryErrorFragment, [])
        fragment.num_run_once_to_fail = 3
        with self.assertRaises(TransitoryError):
            run_fragment_once(fragment, max_transitory_error_retries=2)

    def test_run_once_restart_kernel_transitory_error_limit(self):
        fragment = self.create(TransitoryErrorFragment, [])
        fragment.num_run_once_to_restart_fail = 3
        with self.assertRaises(RestartKernelTransitoryError):
            run_fragment_once(fragment, max_transitory_error_retries=2)

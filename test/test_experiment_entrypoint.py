"""
Tests for ndscan.experiment top-level runners.
"""

import json
from ndscan.experiment import *
from ndscan.experiment.utils import is_kernel
from ndscan.utils import PARAMS_ARG_KEY, SCHEMA_REVISION, SCHEMA_REVISION_KEY
from sipyco import pyon
from fixtures import (AddOneFragment, ReboundAddOneFragment, TrivialKernelFragment,
                      TransitoryErrorFragment, MultiPointTransitoryErrorFragment,
                      RequestTerminationFragment, AddOneAggregate,
                      TrivialKernelAggregate, TwoAnalysisAggregate)
from mock_environment import HasEnvironmentCase

ScanAddOneExp = make_fragment_scan_exp(AddOneFragment)
ScanReboundAddOneExp = make_fragment_scan_exp(ReboundAddOneFragment)
ScanTwoAnalysisAggregateExp = make_fragment_scan_exp(TwoAnalysisAggregate)


class TestAggregateExpFragment(HasEnvironmentCase):
    def test_aggregate(self):
        parent = self.create(AddOneAggregate, [])

        self.assertIn(parent.a.value, parent.get_always_shown_params())
        self.assertIn(parent.b.value, parent.get_always_shown_params())

        self.assertFalse(is_kernel(parent.run_once))
        result = run_fragment_once(parent)
        self.assertEqual(parent.a.num_prepare_calls, 1)
        self.assertEqual(parent.b.num_prepare_calls, 1)
        self.assertEqual(result[parent.a.result], 1.0)
        self.assertEqual(result[parent.b.result], 1.0)

    def test_kernel(self):
        parent = self.create(TrivialKernelAggregate, [])
        self.assertTrue(is_kernel(parent.run_once))


class FragmentScanExpCase(HasEnvironmentCase):
    def test_wrong_fqn_override(self):
        exp = self.create(ScanAddOneExp,
                          env_args={
                              PARAMS_ARG_KEY:
                              pyon.encode({
                                  "overrides": {
                                      "non_existent": [{
                                          "path": "*",
                                          "value": 3
                                      }]
                                  }
                              })
                          })
        with self.assertRaises(KeyError):
            exp.prepare()

    def test_no_path_match_override(self):
        exp = self.create(ScanAddOneExp,
                          env_args={
                              PARAMS_ARG_KEY:
                              pyon.encode({
                                  "overrides": {
                                      "fixtures.AddOneFragment.value": [{
                                          "path": "non_existent",
                                          "value": 3
                                      }]
                                  }
                              })
                          })
        with self.assertRaises(ValueError):
            exp.prepare()

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
            return self.dataset_db.get("ndscan.rid_0." + key)

        self.assertEqual(d(SCHEMA_REVISION_KEY), SCHEMA_REVISION)
        self.assertEqual(json.loads(d("axes")), [])
        self.assertEqual(d("completed"), True)
        self.assertEqual(d("fragment_fqn"), "fixtures.AddOneFragment")
        self.assertEqual(d("source_id"), "system_0")

    def test_run_time_series_scan(self):
        # Make fragment that fails device_setup() as many times as allowed to test
        # whether counters are correctly reset between points in time series scan.
        exp = self.create(
            make_fragment_scan_exp(MultiPointTransitoryErrorFragment,
                                   3,
                                   max_transitory_error_retries=3))
        exp.args._params["scan"]["no_axes_mode"] = "time_series"

        # Terminate eventually.
        self.scheduler.num_check_pause_calls_until_termination = 13

        exp.prepare()
        exp.run()

        def d(key):
            return self.dataset_db.get("ndscan.rid_0." + key)

        self.assertEqual(d("points.channel_result"), [42, 42, 42])
        timestamps = d("points.axis_0")
        self.assertEqual(len(timestamps), 3)
        for i in range(len(timestamps)):
            prev = 0.0 if i == 0 else timestamps[i - 1]
            cur = timestamps[i]
            self.assertGreaterEqual(cur, prev)
            # Timestamps are in seconds, so this is a _very_ conservative bound on
            # running the test loop in-process (which will take much less than a
            # millisecond).
            self.assertLess(cur, 1.0)

    def test_time_series_transitory_limit(self):
        exp = self.create(
            make_fragment_scan_exp(MultiPointTransitoryErrorFragment,
                                   3,
                                   max_transitory_error_retries=2))
        exp.args._params["scan"]["no_axes_mode"] = "time_series"

        # Terminate eventually even in case there are bugs.
        self.scheduler.num_check_pause_calls_until_termination = 100

        exp.prepare()
        with self.assertRaises(TransitoryError):
            exp.run()

    def test_run_1d_scan(self):
        exp = self._test_run_1d(ScanAddOneExp, "fixtures.AddOneFragment")
        self.assertEqual(exp.fragment.num_host_setup_calls, 1)
        self.assertEqual(exp.fragment.num_device_setup_calls, 3)
        self.assertEqual(exp.fragment.num_host_cleanup_calls, 1)
        self.assertEqual(exp.fragment.num_device_cleanup_calls, 1)

        pref = "fit_ndscan.fitting.oitg."
        curve_annotation = {
            "kind": "computed_curve",
            "parameters": {
                "fit_class_name": "lorentzian",
                "fit_module": "ndscan.fitting.oitg",
                "associated_channels": ["channel_result"]
            },
            "coordinates": {},
            "data": {
                "a": {
                    "analysis_name": f"{pref}lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "a"
                },
                "fwhm": {
                    "analysis_name": f"{pref}lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "fwhm"
                },
                "x0": {
                    "analysis_name": f"{pref}lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0"
                },
                "y0": {
                    "analysis_name": f"{pref}lorentzian_channel_result",
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
                    "analysis_name": f"{pref}lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0"
                }
            },
            "data": {
                "axis_0_error": {
                    "analysis_name": f"{pref}lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0_error"
                }
            }
        }
        self.assertEqual(json.loads(self.dataset_db.get("ndscan.rid_0.annotations")),
                         [curve_annotation, location_annotation])

        self.assertEqual(
            json.loads(self.dataset_db.get("ndscan.rid_0.online_analyses")), {
                f"{pref}lorentzian_channel_result": {
                    "fixed_params": {
                        "y0": 1.0
                    },
                    "data": {
                        "y": "channel_result",
                        "x": "axis_0"
                    },
                    "param_bounds": {}
                    "scale_factors": {}
                    "fit_class_name": "lorentzian",
                    "fit_module": "ndscan.fitting.oitg",
                    "initial_values": {
                        "fwhm": 2.0
                    },
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
        exp.args._params["scan"]["axes"].append({
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
            return self.dataset_db.get("ndscan.rid_0." + key)

        self.assertEqual(json.loads(d("axes")), [{
            "increment": 1.0,
            "max": 2,
            "min": 0,
            "param": {
                "default": "0.0",
                "description": "Value to return",
                "fqn": fqn,
                "spec": {
                    "is_scannable": True,
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

    def test_aggregate_scan(self):
        exp = self.create(ScanTwoAnalysisAggregateExp)
        exp.args._params["scan"]["axes"].append({
            "type": "linear",
            "range": {
                "start": 0,
                "stop": 1,
                "num_points": 5,
                "randomise_order": False
            },
            "fqn": "fixtures.TwoAnalysisAggregate.a",
            "path": "*"
        })
        exp.prepare()
        exp.run()
        results = json.loads(self.dataset_db.get("ndscan.rid_0.analysis_results"))
        self.assertTrue("first_result_a" in results)
        self.assertTrue("second_result_a" in results)


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


class TopLevelRunnerCase(HasEnvironmentCase):
    def test_single_run_termination_requested(self):
        """Make sure TerminationRequested is not suppressed for non-scans."""

        fragment = self.create(RequestTerminationFragment, [])
        tlr = self.create(TopLevelRunner, fragment, ScanSpec([], [], ScanOptions()))
        with self.assertRaises(TerminationRequested):
            tlr.run()

"""
Tests for subscan functionality.
"""

import json

from fixtures import (
    AddOneCustomAnalysisFragment,
    AddOneFragment,
    ReboundAddOneFragment,
    TwoAnalysisAggregate,
    TwoAnalysisFragment,
)
from mock_environment import ExpFragmentCase

from ndscan.experiment import *


class Scan1DFragment(ExpFragment):
    def build_fragment(self, klass):
        self.setattr_fragment("child", klass)
        scan = setattr_subscan(self, "scan", self.child, [(self.child, "value")])
        assert self.scan == scan

    def run_once(self):
        return self.scan.run(
            [(self.child.value, LinearGenerator(0, 3, 4, False))],
            ScanOptions(seed=1234),
        )[:2]


class SubscanCase(ExpFragmentCase):
    def test_1d_subscan_return(self):
        parent = self.create(Scan1DFragment, AddOneFragment)
        self._test_1d(parent, parent.child.result)

    def test_1d_rebound_subscan_return(self):
        parent = self.create(Scan1DFragment, ReboundAddOneFragment)
        self._test_1d(parent, parent.child.add_one.result)

    def _test_1d(self, parent, result_channel):
        coords, values = parent.run_once()

        expected_values = [float(n) for n in range(0, 4)]
        expected_results = [v + 1 for v in expected_values]
        self.assertEqual(coords, {parent.child.value: expected_values})
        self.assertEqual(values, {result_channel: expected_results})

    def test_1d_result_channels(self):
        parent = self.create(Scan1DFragment, AddOneFragment)
        results = run_fragment_once(parent)

        expected_values = [float(n) for n in range(0, 4)]
        expected_results = [v + 1 for v in expected_values]
        self.assertEqual(results[parent.scan_axis_0], expected_values)
        self.assertEqual(results[parent.scan_channel_result], expected_results)

        spec = json.loads(results[parent.scan_spec])
        self.assertEqual(spec["fragment_fqn"], "fixtures.AddOneFragment")
        self.assertEqual(spec["seed"], 1234)

        curve_annotation = {
            "kind": "computed_curve",
            "parameters": {
                "function_name": "lorentzian",
                "associated_channels": ["channel_result"],
            },
            "coordinates": {},
            "data": {
                "a": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "a",
                },
                "fwhm": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "fwhm",
                },
                "x0": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0",
                },
                "y0": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "y0",
                },
            },
        }
        location_annotation = {
            "kind": "location",
            "parameters": {"associated_channels": ["channel_result"]},
            "coordinates": {
                "axis_0": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0",
                }
            },
            "data": {
                "axis_0_error": {
                    "analysis_name": "fit_lorentzian_channel_result",
                    "kind": "online_result",
                    "result_key": "x0_error",
                }
            },
        }
        self.assertEqual(spec["annotations"], [curve_annotation, location_annotation])
        self.assertEqual(
            spec["online_analyses"],
            {
                "fit_lorentzian_channel_result": {
                    "constants": {"y0": 1.0},
                    "data": {"y": "channel_result", "x": "axis_0"},
                    "fit_type": "lorentzian",
                    "initial_values": {"fwhm": 2.0},
                    "kind": "named_fit",
                }
            },
        )
        self.assertEqual(
            spec["channels"],
            {
                "result": {
                    "description": "",
                    "scale": 1.0,
                    "path": "child/result",
                    "type": "float",
                    "unit": "",
                }
            },
        )
        self.assertEqual(
            spec["axes"],
            [
                {
                    "min": 0,
                    "max": 3,
                    "path": "child",
                    "param": {
                        "description": "Value to return",
                        "default": "0.0",
                        "fqn": "fixtures.AddOneFragment.value",
                        "spec": {"is_scannable": True, "scale": 1.0, "step": 0.1},
                        "type": "float",
                    },
                    "increment": 1.0,
                }
            ],
        )

    def test_1d_custom_analysis(self):
        parent = self.create(Scan1DFragment, AddOneCustomAnalysisFragment)
        results = run_fragment_once(parent)
        annotations = json.loads(results[parent.scan_spec])["annotations"]
        x_location = {
            "coordinates": {"axis_0": {"kind": "fixed", "value": 1.5}},
            "data": {},
            "kind": "location",
            "parameters": {},
        }
        y_location = {
            "coordinates": {"channel_result": {"kind": "fixed", "value": 2.5}},
            "data": {},
            "kind": "location",
            "parameters": {},
        }
        # FIXME: This should probably use fuzzy comparison for the floating point
        # values.
        self.assertEqual(annotations, [x_location, y_location])

    def test_fragment_detach(self):
        parent = self.create(Scan1DFragment, AddOneFragment)
        run_fragment_once(parent)

        # Make sure the setup and cleanup methods aren't also called during the parent
        # fragment setup/cleanup (in addition to the subscan).
        self.assertEqual(parent.child.num_host_setup_calls, 1)
        self.assertEqual(parent.child.num_device_setup_calls, 4)
        self.assertEqual(parent.child.num_device_cleanup_calls, 1)
        self.assertEqual(parent.child.num_host_cleanup_calls, 1)


class RunSubscanTwiceFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("child", AddOneFragment)
        setattr_subscan(
            self,
            "scan",
            self.child,
            [(self.child, "value")],
            expose_analysis_results=False,
        )

    def run_once(self):
        r0 = self.scan.run([(self.child.value, LinearGenerator(0, 3, 4, False))])
        r1 = self.scan.run([(self.child.value, LinearGenerator(4, 7, 4, False))])
        return r0, r1


class RunSubscanTwiceCase(ExpFragmentCase):
    def test_1d_subscan_twice(self):
        parent = self.create(RunSubscanTwiceFragment)
        results = parent.run_once()

        for base, (coords, values, _) in zip([0, 4], results):
            expected_values = [float(n) for n in range(base, base + 4)]
            expected_results = [v + 1 for v in expected_values]
            self.assertEqual(coords, {parent.child.value: expected_values})
            self.assertEqual(values, {parent.child.result: expected_results})


class SubscanAnalysisFragment(ExpFragment):
    def build_fragment(
        self, declare_both_scannable=False, always_execute_analyses=True
    ):
        self.always_execute_analyses = always_execute_analyses
        self.setattr_fragment("child", TwoAnalysisFragment)
        axes = [(self.child, "a")]
        if declare_both_scannable:
            axes.append((self.child, "b"))
        setattr_subscan(self, "scan", self.child, axes)
        self.had_result = False

    def run_once(self):
        _, _, analysis_results = self.scan.run(
            [(self.child.a, LinearGenerator(0.0, 1.0, 3, True))],
            execute_default_analyses=self.always_execute_analyses,
        )
        self.had_result = "result_a" in analysis_results


class AggregateSubscanAnalysisFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("child", TwoAnalysisAggregate)
        setattr_subscan(self, "scan", self.child, [(self.child, "a")])
        self.had_all_results = False

    def run_once(self):
        _, _, analysis_results = self.scan.run(
            [(self.child.a, LinearGenerator(0.0, 1.0, 3, True))]
        )
        self.had_all_results = all(
            f"{n}_result_a" in analysis_results for n in ("first", "second")
        )


class SubscanAnalysisCase(ExpFragmentCase):
    def test_simple_filtering(self):
        parent = self.create(SubscanAnalysisFragment)
        results = run_fragment_once(parent)
        spec = json.loads(results[parent.scan_spec])
        self.assertEqual(spec["analysis_results"], {"result_a": "scan_result_a"})
        self.assertEqual(results[parent.scan_result_a], 42.0)
        self.assertTrue(parent.had_result)

    def _test_subset_filtering(self, always_execute_analyses):
        parent = self.create(
            SubscanAnalysisFragment,
            declare_both_scannable=True,
            always_execute_analyses=always_execute_analyses,
        )
        results = run_fragment_once(parent)
        spec = json.loads(results[parent.scan_spec])

        # Shouldn't have a result channel, since it wasn't statically known which
        # axes would be scanned.
        self.assertEqual(spec.get("analysis_results", {}), {})

        # If requested, the analysis should have still been executed at run()-time,
        # though.
        self.assertEqual(parent.had_result, always_execute_analyses)

    def test_subset_filtering(self):
        self._test_subset_filtering(False)

    def test_subset_filtering_2(self):
        self._test_subset_filtering(True)

    def test_aggregate(self):
        # For simplicity, test AggregateExpFragment through an actual subscan instead of
        # manually verifying the analysis result handling/…
        parent = self.create(AggregateSubscanAnalysisFragment)
        results = run_fragment_once(parent)
        self.assertTrue(parent.had_all_results)
        self.assertEqual(results[parent.scan_first_result_a], 42.0)
        self.assertEqual(results[parent.scan_second_result_a], 42.0)

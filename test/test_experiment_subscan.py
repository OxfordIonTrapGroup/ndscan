"""
Tests for subscan functionality.
"""

import json

import numpy as np
from fixtures import (
    AddOneCustomAnalysisFragment,
    AddOneFragment,
    MultiPointTransitoryErrorFragment,
    ReboundAddOneFragment,
    TwoAnalysisAggregate,
    TwoAnalysisFragment,
)
from mock_environment import ExpFragmentCase, HasEnvironmentCase

from ndscan.experiment import *
from ndscan.experiment.entry_point import _InProgressDatasetWriter
from ndscan.utils import SCHEMA_REVISION, SCHEMA_REVISION_KEY


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
                        "explanation": "",
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


class TransitoryErrorSubscan(SubscanExpFragment):
    def build_fragment(self, **kwargs):
        self.setattr_fragment("frag", MultiPointTransitoryErrorFragment, **kwargs)
        super().build_fragment(self, self.frag, [(self.frag, "value")])
        self.configure(
            [(self.frag.value, LinearGenerator(0, 10, 11, randomise_order=True))]
        )


class TransitoryErrorSubscanCase(ExpFragmentCase):
    def _test_with_kwargs(self, **kwargs):
        # Fail every third point (as good as any).
        subscan = self.create(
            TransitoryErrorSubscan, fail_at_point=lambda i: i % 3 == 1, **kwargs
        )
        results = run_fragment_once(subscan)
        inputs = results[subscan._axis_0]
        outputs = results[subscan._channel_result]
        np.testing.assert_array_equal(np.sort(inputs), np.arange(11))
        np.testing.assert_array_equal(inputs, outputs)

    def test_nominal(self):
        self._test_with_kwargs()

    def test_transitory_setup(self):
        self._test_with_kwargs(num_device_setup_to_fail=2)

    def test_transitory_run(self):
        self._test_with_kwargs(num_run_once_to_fail=2)

    def test_restart_transitory_setup(self):
        self._test_with_kwargs(num_device_setup_to_restart_fail=2)

    def test_restart_transitory_run(self):
        self._test_with_kwargs(num_run_once_to_restart_fail=2)


class ProbedAddOneFragment(AddOneFragment):
    """AddOneFragment invoking a callback at the beginning of each ``run_once()``
    (i.e. before the results for the respective point have been pushed)."""

    def build_fragment(self):
        super().build_fragment()
        self.run_once_callback = None

    def run_once(self):
        if self.run_once_callback is not None:
            self.run_once_callback()
        super().run_once()


class InProgressSubscanFragment(ExpFragment):
    def build_fragment(self, expose_in_progress=True):
        self.setattr_fragment("child", ProbedAddOneFragment)
        setattr_subscan(
            self,
            "scan",
            self.child,
            [(self.child, "value")],
            expose_in_progress=expose_in_progress,
        )


class TwoParamFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("a", FloatParam, "a", default=0.0)
        self.setattr_param("b", FloatParam, "b", default=0.0)
        self.setattr_result("result", FloatChannel)

    def run_once(self):
        self.result.push(self.a.get())


class TwoParamSubscanFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("child", TwoParamFragment)
        setattr_subscan(
            self, "scan", self.child, [(self.child, "a"), (self.child, "b")]
        )


class InProgressWriterCase(ExpFragmentCase):
    """Tests for the fragment-side in-progress publishing hooks, with a writer
    attached directly (rather than by a TopLevelRunner)."""

    PREFIX = "preview."

    def create_parent(self):
        parent = self.create(InProgressSubscanFragment)
        parent.scan.attach_in_progress_writer(
            _InProgressDatasetWriter(parent, self.PREFIX, "rid_0")
        )
        return parent

    def d(self, key, prefix=None):
        try:
            return self.dataset_db.get((prefix or self.PREFIX) + key)
        except KeyError:
            return None

    def run_scan(self, parent, start=0, num=4):
        return parent.scan.run(
            [
                (
                    parent.child.value,
                    LinearGenerator(start, start + num - 1, num, False),
                )
            ],
            ScanOptions(seed=1234),
        )

    def test_datasets_after_run(self):
        parent = self.create_parent()
        self.run_scan(parent)

        self.assertEqual(self.d(SCHEMA_REVISION_KEY), SCHEMA_REVISION)
        self.assertEqual(self.d("source_id"), "rid_0")
        self.assertEqual(self.d("completed"), True)
        self.assertEqual(
            self.d("fragment_fqn"), "test_experiment_subscan.ProbedAddOneFragment"
        )
        self.assertEqual(self.d("seed"), 1234)

        axes = json.loads(self.d("axes"))
        self.assertEqual(len(axes), 1)
        self.assertEqual(axes[0]["path"], "child")
        self.assertEqual(axes[0]["min"], 0)
        self.assertEqual(axes[0]["max"], 3)

        channels = json.loads(self.d("channels"))
        self.assertEqual(list(channels.keys()), ["result"])
        self.assertEqual(channels["result"]["path"], "child/result")

        self.assertEqual(
            list(json.loads(self.d("online_analyses")).keys()),
            ["fit_lorentzian_channel_result"],
        )
        self.assertEqual(
            [a["kind"] for a in json.loads(self.d("annotations"))],
            ["computed_curve", "location"],
        )

        self.assertEqual(self.d("points.axis_0"), [0.0, 1.0, 2.0, 3.0])
        self.assertEqual(self.d("points.channel_result"), [1.0, 2.0, 3.0, 4.0])

        # "axes" must have been broadcast after all the other metadata, as
        # subscribers use it as the marker for a consistent set of metadata.
        keys = [k for k in self.dataset_db.data.keys() if k.startswith(self.PREFIX)]
        for key in ("channels", "online_analyses", "annotations", "seed"):
            self.assertGreater(
                keys.index(self.PREFIX + "axes"), keys.index(self.PREFIX + key)
            )

    def test_incremental_updates(self):
        parent = self.create_parent()

        observed = []

        def probe():
            # Metadata must be complete before the first point executes.
            self.assertEqual(self.d("completed"), False)
            self.assertEqual(json.loads(self.d("axes"))[0]["path"], "child")
            # Copy the arrays, as the mock dataset db mutates them in place.
            observed.append(
                (
                    list(self.d("points.axis_0") or []),
                    list(self.d("points.channel_result") or []),
                )
            )

        parent.child.run_once_callback = probe
        self.run_scan(parent)

        # At the start of point i, exactly the previous i points are visible.
        self.assertEqual(
            observed,
            [
                ([], []),
                ([0.0], [1.0]),
                ([0.0, 1.0], [1.0, 2.0]),
                ([0.0, 1.0, 2.0], [1.0, 2.0, 3.0]),
            ],
        )

    def test_overwrite_between_iterations(self):
        parent = self.create_parent()
        self.run_scan(parent, start=0, num=4)

        observed = []

        def probe():
            # Copy the array, as the mock dataset db mutates it in place.
            observed.append((list(self.d("points.axis_0")), self.d("completed")))

        parent.child.run_once_callback = probe
        # Different length/values to verify the datasets are overwritten (and end up
        # shorter than before).
        self.run_scan(parent, start=4, num=3)

        # The data from the completed first iteration stays visible until the first
        # point of the next one has been acquired.
        self.assertEqual(observed[0], ([0.0, 1.0, 2.0, 3.0], False))
        self.assertEqual(observed[1], ([4.0], False))

        self.assertEqual(self.d("points.axis_0"), [4.0, 5.0, 6.0])
        self.assertEqual(self.d("points.channel_result"), [5.0, 6.0, 7.0])
        self.assertEqual(self.d("completed"), True)
        axes = json.loads(self.d("axes"))
        self.assertEqual(axes[0]["min"], 4)
        self.assertEqual(axes[0]["max"], 6)

    def test_unchanged_spec_reannounce(self):
        parent = self.create_parent()

        def run(seed):
            # Pass the seed explicitly: ScanOptions() as a default argument is
            # evaluated once at function-definition time, so relying on the default
            # would give both runs the very same (randomised-once) seed and never
            # exercise the seed-only-difference path at all.
            return parent.scan.run(
                [(parent.child.value, LinearGenerator(0, 3, 4, False))],
                ScanOptions(seed=seed),
            )

        run(1234)
        final_annotations = self.d("annotations")

        observed = []

        def probe():
            observed.append(
                (
                    list(self.d("points.axis_0")),
                    self.d("completed"),
                    self.d("annotations"),
                )
            )

        parent.child.run_once_callback = probe
        run(5678)

        # With the spec unchanged (up to the seed), the completed previous run
        # remains visible in its final state – including the completed flag and any
        # final annotations – until the first point of the new one has been acquired.
        self.assertEqual(observed[0], ([0.0, 1.0, 2.0, 3.0], True, final_annotations))
        self.assertEqual(observed[1][:2], ([0.0], False))

        # The seed is still kept up to date for the new run.
        self.assertEqual(self.d("seed"), parent.scan._spec.options.seed)
        self.assertEqual(self.d("completed"), True)

    def test_changed_spec_blanks_stale_datasets(self):
        parent = self.create(TwoParamSubscanFragment)
        parent.scan.attach_in_progress_writer(
            _InProgressDatasetWriter(parent, self.PREFIX, "rid_0")
        )

        parent.scan.run(
            [
                (parent.child.a, LinearGenerator(0, 1, 2, False)),
                (parent.child.b, LinearGenerator(2, 3, 2, False)),
            ]
        )
        self.assertEqual(len(json.loads(self.d("axes"))), 2)
        self.assertEqual(len(self.d("points.axis_1")), 4)

        # Running with only one axis afterwards must blank the now-stale axis_1 data,
        # which would otherwise linger indefinitely (including into the archived HDF5
        # file), as the new pushes would never overwrite it.
        parent.scan.run([(parent.child.a, LinearGenerator(0, 3, 4, False))])
        self.assertEqual(len(json.loads(self.d("axes"))), 1)
        self.assertEqual(self.d("points.axis_1"), [])
        self.assertEqual(self.d("points.axis_0"), [0.0, 1.0, 2.0, 3.0])
        self.assertEqual(self.d("points.channel_result"), [0.0, 1.0, 2.0, 3.0])
        self.assertEqual(self.d("completed"), True)

    def test_attach_requires_expose_in_progress(self):
        parent = self.create(InProgressSubscanFragment, expose_in_progress=False)
        with self.assertRaises(AssertionError):
            parent.scan.attach_in_progress_writer(
                _InProgressDatasetWriter(parent, self.PREFIX, "rid_0")
            )

    def test_transitory_errors(self):
        # Points hit by transitory errors are retried and must not leave stray
        # elements in the in-progress datasets. Since the scan spec is already
        # configured in build_fragment(), this also exercises the immediate metadata
        # broadcast on attaching the writer.
        subscan = self.create(
            TransitoryErrorSubscan,
            fail_at_point=lambda idx: idx % 3 == 1,
            num_run_once_to_restart_fail=2,
        )
        subscans = {}
        subscan._collect_subscans(subscans)
        self.assertEqual(list(subscans.keys()), ["TransitoryErrorSubscan"])
        subscans["TransitoryErrorSubscan"].attach_in_progress_writer(
            _InProgressDatasetWriter(subscan, self.PREFIX, "rid_0")
        )
        self.assertEqual(len(json.loads(self.d("axes"))), 1)

        results = run_fragment_once(subscan)

        inputs = self.d("points.axis_0")
        outputs = self.d("points.channel_result")
        np.testing.assert_array_equal(inputs, results[subscan._axis_0])
        np.testing.assert_array_equal(outputs, results[subscan._channel_result])
        self.assertEqual(self.d("completed"), True)


class PreviewedSubscansFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("child_a", AddOneFragment)
        self.setattr_fragment("child_b", AddOneFragment)
        setattr_subscan(self, "scan_a", self.child_a, [(self.child_a, "value")])
        setattr_subscan(
            self,
            "scan_b",
            self.child_b,
            [(self.child_b, "value")],
            expose_in_progress=False,
        )

    def run_once(self):
        self.scan_a.run([(self.child_a.value, LinearGenerator(0, 1, 2, False))])
        self.scan_b.run([(self.child_b.value, LinearGenerator(2, 3, 2, False))])


class InnerSubscanFragment(ExpFragment):
    """Fragment containing a subscan, itself scanned by NestedSubscan below."""

    def build_fragment(self):
        self.setattr_param(
            "outer_value", FloatParam, "Value scanned by the outer scan", default=0.0
        )
        self.setattr_fragment("leaf", AddOneFragment)
        setattr_subscan(self, "leaf_scan", self.leaf, [(self.leaf, "value")])
        self.setattr_result("total", FloatChannel)

    def run_once(self):
        _, values, _ = self.leaf_scan.run(
            [(self.leaf.value, LinearGenerator(0, 1, 2, False))]
        )
        self.total.push(sum(values[self.leaf.result]) + self.outer_value.get())


class NestedSubscan(SubscanExpFragment):
    def build_fragment(self):
        self.setattr_fragment("inner", InnerSubscanFragment)
        super().build_fragment(self, self.inner, [(self.inner, "outer_value")])
        self.configure([(self.inner.outer_value, LinearGenerator(0, 1, 2, False))])


class SubscanHolderFragment(ExpFragment):
    """Holds a subscan under the given name (to construct discovery name collisions
    below)."""

    def build_fragment(self, scan_name):
        self.setattr_fragment("child", AddOneFragment)
        setattr_subscan(self, scan_name, self.child, [(self.child, "value")])

    def run_once(self):
        pass


class CollidingSubscanNamesFragment(ExpFragment):
    def build_fragment(self):
        # Both subscans map to the underscore-joined discovery name "a_b".
        self.setattr_fragment("a", SubscanHolderFragment, "b")
        self.setattr_fragment("other", AddOneFragment)
        setattr_subscan(self, "a_b", self.other, [(self.other, "value")])

    def run_once(self):
        pass


class SubscanPreviewRunnerCase(HasEnvironmentCase):
    """Tests for TopLevelRunner discovering subscans and publishing in-progress
    previews for them."""

    def make_tlr(self, fragment_class, **kwargs):
        fragment = self.create(fragment_class, [])
        fragment.init_params()
        return self.create(
            TopLevelRunner, fragment, ScanSpec([], [], ScanOptions()), **kwargs
        )

    def d(self, key):
        return self.dataset_db.get("ndscan.rid_0." + key)

    def test_previews_published(self):
        tlr = self.make_tlr(PreviewedSubscansFragment)
        tlr.run()

        self.assertEqual(
            self.d("previews.scan_a." + SCHEMA_REVISION_KEY), SCHEMA_REVISION
        )
        self.assertEqual(self.d("previews.scan_a.source_id"), "rid_0")
        self.assertEqual(self.d("previews.scan_a.completed"), True)
        self.assertEqual(self.d("previews.scan_a.points.axis_0"), [0.0, 1.0])
        self.assertEqual(self.d("previews.scan_a.points.channel_result"), [1.0, 2.0])

        # scan_b was opted out via expose_in_progress=False.
        for key in self.dataset_db.data.keys():
            self.assertNotIn("previews.scan_b", key)

    def test_preview_applets(self):
        tlr = self.make_tlr(PreviewedSubscansFragment)
        tlr.create_applet("Test", group="foo")

        self.assertEqual(self.ccb.issue.call_count, 2)
        (main_args, main_kwargs), (preview_args, preview_kwargs) = (
            self.ccb.issue.call_args_list
        )
        self.assertEqual(main_args[0:2], ("create_applet", "Test"))
        self.assertIn("--prefix=ndscan.rid_0. ", main_args[2] + " ")
        self.assertEqual(main_kwargs.get("group"), "foo")
        self.assertEqual(preview_args[0:2], ("create_applet", "Test: subscan 'scan_a'"))
        self.assertIn("--prefix=ndscan.rid_0.previews.scan_a.", preview_args[2])
        self.assertEqual(preview_kwargs.get("group"), ["foo", "previews"])

    def test_previews_disabled(self):
        tlr = self.make_tlr(PreviewedSubscansFragment, publish_subscan_previews=False)
        tlr.run()
        for key in self.dataset_db.data.keys():
            self.assertNotIn("previews.", key)

        tlr.create_applet("Test")
        self.assertEqual(self.ccb.issue.call_count, 1)

    def test_nested_subscans(self):
        tlr = self.make_tlr(NestedSubscan)
        # Discovery recurses into the fragment scanned by the outer subscan.
        self.assertEqual(
            sorted(tlr._subscan_previews.keys()),
            ["NestedSubscan", "inner_leaf_scan"],
        )

        tlr.run()

        self.assertEqual(self.d("previews.NestedSubscan.points.axis_0"), [0.0, 1.0])
        self.assertEqual(
            self.d("previews.NestedSubscan.points.channel_total"), [3.0, 4.0]
        )
        self.assertEqual(self.d("previews.NestedSubscan.completed"), True)

        # The inner subscan preview shows its last iteration.
        self.assertEqual(self.d("previews.inner_leaf_scan.points.axis_0"), [0.0, 1.0])
        self.assertEqual(
            self.d("previews.inner_leaf_scan.points.channel_result"), [1.0, 2.0]
        )
        self.assertEqual(self.d("previews.inner_leaf_scan.completed"), True)

        # The analysis result channels the inner subscan exposes on its parent are
        # part of the outer preview as well; even if the fit fails for a point (as
        # the nonsensical lorentzian fit does here, resulting in NaN values), the
        # arrays must stay rectangular.
        self.assertEqual(
            len(
                self.d(
                    "previews.NestedSubscan.points.channel_leaf_scan_lorentzian_fit_a"
                )
            ),
            2,
        )

    def test_colliding_preview_names(self):
        fragment = self.create(CollidingSubscanNamesFragment, [])
        subscans = {}
        with self.assertLogs("ndscan.experiment.fragment", level="WARNING"):
            fragment._collect_subscans(subscans)
        # Only the first subscan mapping to the ambiguous name is kept (rather than
        # e.g. failing the whole experiment over a display-only feature).
        self.assertEqual(list(subscans.keys()), ["a_b"])
        self.assertIs(subscans["a_b"], fragment.a_b)

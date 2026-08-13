import json
import unittest

from sipyco.sync_struct import Notifier

from ndscan.plots.model import Context
from ndscan.plots.model.subscriber import (
    SubscriberRoot,
    SubscriberScanModel,
    SubscriberSinglePointModel,
)
from ndscan.utils import SCHEMA_REVISION, SCHEMA_REVISION_KEY


class SinglePointTest(unittest.TestCase):
    def setUp(self):
        self.context = Context()
        self.root = SubscriberRoot("ndscan.", self.context)
        self.datasets = Notifier(
            {
                "ndscan.axes": (False, "[]", {}),
                "ndscan.channels": (
                    False,
                    json.dumps(
                        {
                            "foo": {
                                "description": "Foo",
                                "path": "foo",
                                "type": "int",
                                "unit": "",
                            },
                            "bar": {
                                "description": "Bar",
                                "path": "foo",
                                "type": "int",
                                "unit": "",
                            },
                        }
                    ),
                    {},
                ),
                ("ndscan." + SCHEMA_REVISION_KEY): (False, SCHEMA_REVISION, {}),
            }
        )
        self.pending_mods = []
        self.datasets.publish = lambda a: self.pending_mods.append(a)

    def init(self):
        self.pending_mods = [
            {"action": "init", "struct": self.datasets.raw_view.copy()}
        ]
        self.sync()

    def sync(self):
        values = {k: v[1] for k, v in self.datasets.raw_view.items()}
        self.root.data_changed(values, self.pending_mods)
        self.pending_mods.clear()

    def test_new_point(self):
        self.init()
        self.datasets["ndscan.point.foo"] = (False, 42, {})
        self.datasets["ndscan.point.bar"] = (False, 23, {})
        self.datasets["ndscan.point_phase"] = (False, True, {})
        self.sync()
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})

    def test_halfway(self):
        self.datasets["ndscan.point.foo"] = (False, 42, {})
        self.init()

        # No complete point yet.
        self.assertIsNone(self.root.get_model().get_point())

        self.datasets["ndscan.point.bar"] = (False, 23, {})
        self.datasets["ndscan.point_phase"] = (False, True, {})
        self.sync()
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})

    def test_one_and_a_half(self):
        self.datasets["ndscan.point.foo"] = (False, 42, {})
        self.init()

        # No complete point yet.
        self.assertIsNone(self.root.get_model().get_point())

        self.datasets["ndscan.point.bar"] = (False, 23, {})
        self.datasets["ndscan.point_phase"] = (False, True, {})

        # Already write foo value of next point.
        self.datasets["ndscan.point.foo"] = (False, 0, {})
        self.sync()

        # Foo should still be the old value.
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})

    def test_preexisting(self):
        self.datasets["ndscan.point.foo"] = (False, 42, {})
        self.datasets["ndscan.point.bar"] = (False, 42, {})
        self.datasets["ndscan.point_phase"] = (False, True, {})
        self.datasets["ndscan.point.foo"] = (False, 0, {})
        self.init()

        # Can't know whether point is complete (it indeed isn't).
        self.assertIsNone(self.root.get_model().get_point())

        self.datasets["ndscan.point.bar"] = (False, 1, {})
        self.datasets["ndscan.point_phase"] = (False, False, {})
        self.sync()

        self.assertEqual(self.root.get_model().get_point(), {"foo": 0, "bar": 1})

    def test_already_completed(self):
        self.datasets["ndscan.point.foo"] = (False, 42, {})
        self.datasets["ndscan.point.bar"] = (False, 23, {})
        self.datasets["ndscan.point_phase"] = (False, True, {})
        self.datasets["ndscan.completed"] = (False, True, {})
        self.init()
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})


def _axes_json(num_axes: int) -> str:
    return json.dumps(
        [
            {
                "param": {
                    "description": f"Axis {i}",
                    "fqn": f"foo.axis_{i}",
                    "type": "float",
                    "default": "0.0",
                    "spec": {"scale": 1.0, "step": 0.1},
                },
                "path": "*",
            }
            for i in range(num_axes)
        ]
    )


_CHANNELS_JSON = json.dumps(
    {"result": {"description": "", "path": "result", "type": "float", "unit": ""}}
)


class ScanTest(unittest.TestCase):
    def setUp(self):
        self.context = Context()
        self.root = SubscriberRoot("ndscan.", self.context)
        self.datasets = Notifier(
            {
                "ndscan.axes": (False, _axes_json(1), {}),
                "ndscan.channels": (False, _CHANNELS_JSON, {}),
                "ndscan.online_analyses": (False, "{}", {}),
                "ndscan.source_id": (False, "rid_0", {}),
                ("ndscan." + SCHEMA_REVISION_KEY): (False, SCHEMA_REVISION, {}),
            }
        )
        self.pending_mods = []
        self.datasets.publish = lambda a: self.pending_mods.append(a)

        self.models = []
        self.root.model_changed.connect(self.models.append)

    def init(self):
        self.pending_mods = [
            {"action": "init", "struct": self.datasets.raw_view.copy()}
        ]
        self.sync()

    def sync(self):
        values = {k: v[1] for k, v in self.datasets.raw_view.items()}
        self.root.data_changed(values, self.pending_mods)
        self.pending_mods.clear()

    def set_points(self, axis_0, result):
        self.datasets["ndscan.points.axis_0"] = (False, axis_0, {})
        self.datasets["ndscan.points.channel_result"] = (False, result, {})

    def connect_point_signals(self):
        model = self.root.get_model()
        signals = []
        model.points_appended.connect(lambda _: signals.append("appended"))
        model.points_rewritten.connect(lambda _: signals.append("rewritten"))
        return signals

    def test_scan_model(self):
        self.set_points([0.0, 1.0], [10.0, 11.0])
        self.init()
        model = self.root.get_model()
        self.assertIsInstance(model, SubscriberScanModel)
        self.assertEqual(len(model.axes), 1)
        self.assertEqual(list(model.get_channel_schemata().keys()), ["result"])
        self.assertEqual(model.get_point_data()["axis_0"], [0.0, 1.0])
        self.assertEqual(model.get_point_data()["channel_result"], [10.0, 11.0])
        self.assertEqual(self.models, [model])

    def test_points_appended(self):
        self.set_points([0.0], [10.0])
        self.init()
        signals = self.connect_point_signals()
        self.set_points([0.0, 1.0], [10.0, 11.0])
        self.sync()
        self.assertEqual(signals, ["appended"])

    def test_points_appended_in_place(self):
        # sipyco applies append mods in place (ModAction.append), so the list objects
        # the model sees in `values` are the very same ones it cached on the previous
        # update – which is how a live scan actually appends points, as opposed to the
        # wholesale replacement the other tests here simulate. The model must not
        # compare the cached list against itself and conclude nothing changed.
        axis_0 = [0.0]
        result = [10.0]
        self.set_points(axis_0, result)
        self.init()
        signals = self.connect_point_signals()

        axis_0.append(1.0)
        result.append(11.0)
        self.sync()
        self.assertEqual(signals, ["appended"])
        self.assertEqual(self.root.get_model().get_point_data()["axis_0"], [0.0, 1.0])

    def test_points_shrunk_is_rewrite(self):
        # An array getting shorter is what a subscriber sees when e.g. an in-progress
        # subscan publisher starts a new iteration, even if all the values that remain
        # are unchanged; this must invalidate the previously displayed data.
        self.set_points([0.0, 1.0, 2.0], [10.0, 11.0, 12.0])
        self.init()
        signals = self.connect_point_signals()
        self.set_points([0.0], [10.0])
        self.sync()
        self.assertEqual(signals, ["rewritten"])
        self.assertEqual(self.root.get_model().get_point_data()["axis_0"], [0.0])

    def test_points_changed_is_rewrite(self):
        self.set_points([0.0, 1.0], [10.0, 11.0])
        self.init()
        signals = self.connect_point_signals()
        self.set_points([0.0, 1.0], [20.0, 11.0])
        self.sync()
        self.assertEqual(signals, ["rewritten"])

    def test_no_op_update_does_not_emit(self):
        self.set_points([0.0], [10.0])
        self.init()
        signals = self.connect_point_signals()
        # An unrelated dataset change (e.g. another dataset below the subscribed
        # prefix being written) should not cause any point signals to be emitted.
        self.datasets["ndscan.completed"] = (False, True, {})
        self.sync()
        self.assertEqual(signals, [])

    def test_axes_change_rebuilds_model(self):
        self.set_points([0.0], [10.0])
        self.init()
        old_model = self.root.get_model()

        quit_calls = []
        orig_quit = old_model.quit
        old_model.quit = lambda: (quit_calls.append(True), orig_quit())

        self.datasets["ndscan.axes"] = (False, _axes_json(2), {})
        self.sync()

        new_model = self.root.get_model()
        self.assertIsNot(new_model, old_model)
        self.assertEqual(len(new_model.axes), 2)
        self.assertEqual(self.models, [old_model, new_model])
        self.assertEqual(quit_calls, [True])

    def test_channels_change_rebuilds_model(self):
        self.set_points([0.0], [10.0])
        self.init()
        old_model = self.root.get_model()

        new_channels = json.dumps(
            {
                "other": {
                    "description": "",
                    "path": "other",
                    "type": "float",
                    "unit": "",
                }
            }
        )
        self.datasets["ndscan.channels"] = (False, new_channels, {})
        self.sync()

        new_model = self.root.get_model()
        self.assertIsNot(new_model, old_model)
        self.assertEqual(list(new_model.get_channel_schemata().keys()), ["other"])

    def test_unchanged_metadata_rewrite_keeps_model(self):
        # Re-broadcasting identical metadata (e.g. for every iteration of an
        # in-progress subscan with an unchanged spec) must not rebuild the model.
        self.set_points([0.0], [10.0])
        self.init()
        model = self.root.get_model()
        self.datasets["ndscan.axes"] = (False, _axes_json(1), {})
        self.datasets["ndscan.channels"] = (False, _CHANNELS_JSON, {})
        self.sync()
        self.assertIs(self.root.get_model(), model)
        self.assertEqual(self.models, [model])

    def test_axes_change_to_single_point(self):
        # When the scan under the prefix is replaced by a zero-dimensional one, the
        # rebuilt SinglePointModel must pick up already-published point data even
        # though it never observes an init mod.
        self.set_points([0.0], [10.0])
        self.init()

        self.datasets["ndscan.axes"] = (False, "[]", {})
        self.datasets["ndscan.point.result"] = (False, 42.0, {})
        self.datasets["ndscan.completed"] = (False, True, {})
        self.sync()

        model = self.root.get_model()
        self.assertIsInstance(model, SubscriberSinglePointModel)
        self.assertEqual(model.get_point(), {"result": 42.0})

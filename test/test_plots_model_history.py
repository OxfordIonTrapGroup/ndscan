"""Tests for the HistoryFromScanModel proxy the plot widgets consume their source
models through."""

import unittest

from ndscan.plots.model import Context, ScanModel
from ndscan.plots.model.history import HistoryFromScanModel
from ndscan.utils import SCHEMA_REVISION

_AXES = [
    {
        "param": {
            "description": "Axis 0",
            "fqn": "foo.axis_0",
            "type": "float",
            "default": "0.0",
            "spec": {"scale": 1.0, "step": 0.1},
        },
        "path": "*",
    }
]


class FakeScanModel(ScanModel):
    """Minimal stand-in for a source model (e.g. SubscriberScanModel)."""

    def __init__(self):
        super().__init__(_AXES, SCHEMA_REVISION, Context())
        self.data = {"axis_0": [], "channel_result": []}

    def get_point_data(self):
        return self.data

    def get_channel_schemata(self):
        return {
            "result": {"description": "", "path": "result", "type": "float", "unit": ""}
        }

    def append(self, axis_values, result_values):
        self.data = {
            "axis_0": self.data["axis_0"] + axis_values,
            "channel_result": self.data["channel_result"] + result_values,
        }
        self.points_appended.emit(self.data)

    def rewrite(self, axis_values, result_values):
        self.data = {"axis_0": axis_values, "channel_result": result_values}
        self.points_rewritten.emit(self.data)


class HistoryTest(unittest.TestCase):
    def setUp(self):
        self.parent = FakeScanModel()
        self.model = HistoryFromScanModel(self.parent)
        self.signals = []
        self.model.points_appended.connect(
            lambda d: self.signals.append(("appended", dict(d)))
        )
        self.model.points_rewritten.connect(
            lambda d: self.signals.append(("rewritten", dict(d)))
        )

    def test_appended_forwarded(self):
        self.parent.append([0.0], [10.0])
        self.assertEqual(
            self.signals, [("appended", {"axis_0": [0.0], "channel_result": [10.0]})]
        )

    def test_rewrite_forwarded_even_if_prefix_unchanged(self):
        # Values can repeat between reruns of a scan (e.g. in-progress subscan
        # previews restarting for every point of the enclosing scan), so the parent's
        # classification must be forwarded as-is even if the new data happens to be a
        # prefix of the old (consumers reset accumulated state, e.g. for averaging,
        # only on points_rewritten).
        self.parent.append([0.0, 1.0], [10.0, 11.0])
        self.signals.clear()
        self.parent.rewrite([0.0], [10.0])
        self.assertEqual(
            self.signals, [("rewritten", {"axis_0": [0.0], "channel_result": [10.0]})]
        )
        self.assertEqual(self.model.get_point_data()["axis_0"], [0.0])

    def test_cutoff_slicing(self):
        self.parent.append([0.0, 1.0, 2.0], [10.0, 11.0, 12.0])
        self.signals.clear()

        # Rolling back emits the sliced data as an append (consumers handle shrinking
        # data on that path).
        self.model.update_cutoff(0)
        self.assertEqual(
            self.signals, [("appended", {"axis_0": [0.0], "channel_result": [10.0]})]
        )

        # While rolled back, parent appends are not shown (and nothing is emitted).
        self.signals.clear()
        self.parent.append([3.0], [13.0])
        self.assertEqual(self.signals, [])
        self.assertEqual(self.model.get_point_data()["axis_0"], [0.0])

        # Scrubbing back to the latest data resumes following it.
        self.model.update_cutoff(-1)
        self.assertEqual(self.model.get_point_data()["axis_0"], [0.0, 1.0, 2.0, 3.0])

    def test_rewrite_resets_out_of_range_cutoff(self):
        self.parent.append([0.0, 1.0, 2.0], [10.0, 11.0, 12.0])
        self.model.update_cutoff(1)
        self.signals.clear()

        self.parent.rewrite([5.0], [15.0])
        self.assertEqual(
            self.signals, [("rewritten", {"axis_0": [5.0], "channel_result": [15.0]})]
        )

        # Follows the latest data again after the reset.
        self.parent.append([6.0], [16.0])
        self.assertEqual(self.model.get_point_data()["axis_0"], [5.0, 6.0])

    def test_quit_disconnects(self):
        self.model.quit()
        self.parent.append([0.0], [10.0])
        self.parent.rewrite([1.0], [11.0])
        self.assertEqual(self.signals, [])

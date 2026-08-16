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

    def test_quit_disconnects(self):
        self.model.quit()
        self.parent.append([0.0], [10.0])
        self.parent.rewrite([1.0], [11.0])
        self.assertEqual(self.signals, [])

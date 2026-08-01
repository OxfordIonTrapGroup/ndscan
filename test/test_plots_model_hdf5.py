import json
import unittest

import h5py

from ndscan.plots.model import Context
from ndscan.plots.model.hdf5 import HDF5Root
from ndscan.utils import SCHEMA_REVISION, SCHEMA_REVISION_KEY

ENUM_AXIS = {
    "param": {
        "fqn": "fqn.enum_param",
        "description": "Enum param",
        "type": "enum",
        "default": "'foo'",
        "spec": {
            "members": {"foo": "foo", "bar": "bar"},
            "is_scannable": True,
        },
    },
    "path": "*",
}

FLOAT_CHANNEL = {
    "path": "value",
    "description": "",
    "type": "float",
    "scale": 1.0,
    "unit": "",
}

SUBSCAN_CHANNEL = {"path": "child/_spec", "description": "", "type": "subscan"}


def make_in_memory_file():
    # Can be replaced by h5py.File.in_memory() once we require h5py >= 3.13.
    return h5py.File("test.h5", "w", driver="core", backing_store=False)


class HDF5ScanModelTest(unittest.TestCase):
    def test_string_data_loaded_as_str(self):
        """String datasets (e.g. enum param axes) should be decoded to str, matching
        the types seen in live (subscriber) plots (h5py returns bytes by default)."""
        file = make_in_memory_file()
        file["ndscan." + SCHEMA_REVISION_KEY] = SCHEMA_REVISION
        file["ndscan.axes"] = json.dumps([ENUM_AXIS])
        file["ndscan.channels"] = json.dumps(
            {"value": FLOAT_CHANNEL, "spec": SUBSCAN_CHANNEL}
        )
        file["ndscan.online_analyses"] = json.dumps({})
        file["ndscan.annotations"] = json.dumps([])
        file["ndscan.analysis_results"] = json.dumps({"message": None})
        file["ndscan.analysis_result.message"] = "all fine"
        file["ndscan.points.axis_0"] = ["foo", "bar"]
        file["ndscan.points.channel_value"] = [0.1, 0.2]
        file["ndscan.points.channel_spec"] = ["{}", "{}"]

        model = HDF5Root(file, "ndscan.", Context(), "test").get_model()

        data = model.get_point_data()
        self.assertEqual(list(data["axis_0"]), ["foo", "bar"])
        self.assertEqual(list(data["channel_value"]), [0.1, 0.2])
        self.assertEqual(list(data["channel_spec"]), ["{}", "{}"])
        self.assertEqual(model.get_analysis_result_source("message").get(), "all fine")


class HDF5SingleShotModelTest(unittest.TestCase):
    def test_string_data_loaded_as_str(self):
        file = make_in_memory_file()
        file["ndscan." + SCHEMA_REVISION_KEY] = SCHEMA_REVISION
        file["ndscan.axes"] = json.dumps([])
        file["ndscan.channels"] = json.dumps(
            {"value": FLOAT_CHANNEL, "spec": SUBSCAN_CHANNEL}
        )
        file["ndscan.point.value"] = 0.1
        file["ndscan.point.spec"] = "{}"

        model = HDF5Root(file, "ndscan.", Context(), "test").get_model()

        point = model.get_point()
        self.assertEqual(point["value"], 0.1)
        self.assertEqual(point["spec"], "{}")

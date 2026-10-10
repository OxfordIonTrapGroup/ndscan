import json
import unittest

from fixtures import AddOneTextAnnotationsFragment
from mock_environment import HasEnvironmentCase

from ndscan.experiment import *
from ndscan.experiment.annotations import AnnotationContext

ScanTextAnnotationsExp = make_fragment_scan_exp(AddOneTextAnnotationsFragment)


def describe(annotation):
    context = AnnotationContext(
        get_axis_index=lambda handle: 0,
        name_channel=lambda channel: channel.path,
        analysis_result_is_exported=lambda channel: True,
    )
    return annotation.describe(context)


def fixed(value):
    return {"kind": "fixed", "value": value}


class AssociatedChannelsCase(unittest.TestCase):
    def test_resolve_channels(self):
        channel = FloatChannel("foo")
        spec = describe(
            annotations.axis_location(
                "axis_0", 1.0, associated_channels=[channel, "channel_bar"]
            )
        )
        self.assertEqual(
            spec["parameters"], {"associated_channels": ["channel_foo", "channel_bar"]}
        )


class NoteCase(unittest.TestCase):
    def test_fixed(self):
        self.assertEqual(
            describe(annotations.note("foo")),
            {
                "kind": "note",
                "coordinates": {},
                "parameters": {},
                "data": {"text": fixed("foo")},
            },
        )

    def test_associated_channels(self):
        spec = describe(
            annotations.note("foo", associated_channels=[FloatChannel("bar")])
        )
        self.assertEqual(spec["parameters"], {"associated_channels": ["channel_bar"]})


class NamedValueCase(unittest.TestCase):
    def test_fixed_without_error(self):
        self.assertEqual(
            describe(annotations.named_value("foo", 1.5)),
            {
                "kind": "named_value",
                "coordinates": {},
                "parameters": {"label": "foo", "unit": "", "scale": 1},
                "data": {"value": fixed(1.5)},
            },
        )

    def test_explicit_unit(self):
        spec = describe(annotations.named_value("foo", 2e-3, 1e-4, unit="ms"))
        self.assertEqual(
            spec["parameters"], {"label": "foo", "unit": "ms", "scale": 1e-3}
        )
        self.assertEqual(
            spec["data"], {"value": fixed(2e-3), "value_error": fixed(1e-4)}
        )

        spec = describe(annotations.named_value("foo", 2.0, unit="ms", scale=2.0))
        self.assertEqual(spec["parameters"]["scale"], 2.0)

        with self.assertRaises(KeyError):
            annotations.named_value("foo", 1.0, unit="not_a_unit")

    def test_unit_from_channel(self):
        value = FloatChannel("t", unit="us")
        error = FloatChannel("t_err", unit="us")
        spec = describe(annotations.named_value("foo", value, error))
        self.assertEqual(
            spec["parameters"], {"label": "foo", "unit": "us", "scale": 1e-6}
        )
        self.assertEqual(
            spec["data"],
            {
                "value": {"kind": "analysis_result", "name": "t"},
                "value_error": {"kind": "analysis_result", "name": "t_err"},
            },
        )

        # Explicitly specified unit takes precedence.
        spec = describe(annotations.named_value("foo", value, unit="ns"))
        self.assertEqual(
            spec["parameters"], {"label": "foo", "unit": "ns", "scale": 1e-9}
        )


class TextAnnotationsScanCase(HasEnvironmentCase):
    def test_custom_analysis(self):
        exp = self.create(ScanTextAnnotationsExp)
        exp.args._params["scan"]["axes"].append(
            {
                "type": "linear",
                "range": {
                    "start": 0,
                    "stop": 2,
                    "num_points": 3,
                    "randomise_order": False,
                },
                "fqn": "fixtures.AddOneTextAnnotationsFragment.value",
                "path": "*",
            }
        )
        exp.prepare()
        exp.run()
        exp.analyze()

        self.assertEqual(
            json.loads(self.dataset_db.get("ndscan.rid_0.annotations")),
            [
                {
                    "kind": "note",
                    "coordinates": {},
                    "parameters": {"associated_channels": ["channel_result"]},
                    "data": {"text": fixed("Hello")},
                },
                {
                    "kind": "named_value",
                    "coordinates": {},
                    "parameters": {
                        "label": "Centre",
                        "unit": "ms",
                        "scale": 1e-3,
                        "associated_channels": ["channel_result"],
                    },
                    "data": {
                        "value": {"kind": "analysis_result", "name": "centre"},
                        "value_error": {
                            "kind": "analysis_result",
                            "name": "centre_err",
                        },
                    },
                },
            ],
        )

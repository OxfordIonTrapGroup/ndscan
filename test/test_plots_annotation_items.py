import unittest

from ndscan.plots.annotation_items import format_named_value


class FormatNamedValueTest(unittest.TestCase):
    def test_without_error(self):
        self.assertEqual(format_named_value("x", 1.5, None, "", 1), "x: 1.5")
        self.assertEqual(format_named_value("n", 3, None, "", 1), "n: 3")
        self.assertEqual(format_named_value("x", 1.5, float("nan"), "", 1), "x: 1.5")
        self.assertEqual(format_named_value("x", 1.5, 0.0, "", 1), "x: 1.5")

    def test_with_error(self):
        self.assertEqual(format_named_value("x", 1.2345, 0.012, "", 1), "x: 1.23(1)")

    def test_unit_scaling(self):
        self.assertEqual(
            format_named_value("f", 12.5e3, None, "kHz", 1e3), "f: 12.5 kHz"
        )
        self.assertEqual(
            format_named_value("f", 12.5e3, 0.3e3, "kHz", 1e3), "f: 12.5(3) kHz"
        )

    def test_non_numeric(self):
        self.assertEqual(format_named_value("s", "foo", None, "us", 1e-6), "s: foo")
        self.assertEqual(format_named_value("b", True, None, "", 1), "b: True")

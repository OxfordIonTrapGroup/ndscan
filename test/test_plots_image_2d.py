import unittest

from ndscan.plots.image_2d import _calc_range_spec


class CalcRangeSpecTest(unittest.TestCase):
    def test_preset_limits(self):
        self.assertEqual(_calc_range_spec(1.0, 3.0, 0.5, [2.0]), (1.0, 3.0, 0.5))

    def test_zero_preset_limits(self):
        # Zero limits must not be mistaken for missing ones.
        self.assertEqual(_calc_range_spec(0.0, 2.0, 0.5, [1.0]), (0.0, 2.0, 0.5))
        self.assertEqual(_calc_range_spec(-2.0, 0.0, 0.5, [-1.0]), (-2.0, 0.0, 0.5))

    def test_infer_from_data(self):
        self.assertEqual(
            _calc_range_spec(None, None, None, [3.0, 1.0, 2.0, 1.0]), (1.0, 3.0, 1.0)
        )

import numpy as np
import unittest
from ndscan.experiment.scan_generator import CentreSpanGenerator, ExpandingGenerator


class ScanGeneratorCase(unittest.TestCase):
    def test_expanding_trivial(self):
        gen = ExpandingGenerator(centre=0.0,
                                 spacing=10.0,
                                 randomise_order=False,
                                 limit_lower=-1.0,
                                 limit_upper=1.0)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        for i in range(1, 3):
            self.assertFalse(gen.has_level(i))

    def test_expanding_one(self):
        gen = ExpandingGenerator(centre=0.0,
                                 spacing=1.0,
                                 randomise_order=False,
                                 limit_lower=-1.0,
                                 limit_upper=1.0)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [-1.0, 1.0])
        self.assertFalse(gen.has_level(2))

    def test_expanding_lower_lim(self):
        gen = ExpandingGenerator(centre=0.0,
                                 spacing=1.0,
                                 randomise_order=False,
                                 limit_lower=-1.0)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [-1.0, 1.0])
        self.assertTrue(gen.has_level(2))
        self.assertEqual(gen.points_for_level(2), [2.0])
        self.assertTrue(gen.has_level(10))
        self.assertEqual(gen.points_for_level(10), [10.0])

    def test_expanding_upper_lim(self):
        gen = ExpandingGenerator(centre=0.0,
                                 spacing=1.0,
                                 randomise_order=False,
                                 limit_upper=1.0)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [-1.0, 1.0])
        self.assertTrue(gen.has_level(2))
        self.assertEqual(gen.points_for_level(2), [-2.0])
        self.assertTrue(gen.has_level(10))
        self.assertEqual(gen.points_for_level(10), [-10.0])

    def test_expanding_no_lim(self):
        gen = ExpandingGenerator(centre=0.0, spacing=1.0, randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [-1.0, 1.0])
        self.assertTrue(gen.has_level(10))
        self.assertEqual(gen.points_for_level(10), [-10.0, 10.0])

    def test_centre_empty(self):
        with self.assertRaises(ValueError):
            CentreSpanGenerator(centre=0.0,
                                half_span=1.0,
                                num_points=0,
                                randomise_order=False)

    def test_centre_one(self):
        gen = CentreSpanGenerator(centre=0.0,
                                  half_span=1.0,
                                  num_points=1,
                                  randomise_order=True)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0, np.random), [0.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_two(self):
        gen = CentreSpanGenerator(centre=0.0,
                                  half_span=1.0,
                                  num_points=2,
                                  randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [-1.0, 1.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_three(self):
        gen = CentreSpanGenerator(centre=0.0,
                                  half_span=1.0,
                                  num_points=3,
                                  randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [-1.0, 0.0, 1.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_lower_lim(self):
        gen = CentreSpanGenerator(centre=0.0,
                                  half_span=1.0,
                                  num_points=2,
                                  randomise_order=False,
                                  limit_lower=0.0)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0, 1.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_upper_lim(self):
        gen = CentreSpanGenerator(centre=0.0,
                                  half_span=1.0,
                                  num_points=2,
                                  randomise_order=False,
                                  limit_upper=0.0)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [-1.0, 0.0])
        self.assertFalse(gen.has_level(1))

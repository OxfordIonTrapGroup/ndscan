import unittest
from ndscan.experiment.scan_generator import ExpandingGenerator


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

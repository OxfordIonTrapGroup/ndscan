import unittest

from ndscan.plots.utils import *


class FragmentScanExpCase(unittest.TestCase):
    def test_find_neighbour_index(self):
        # This could be a good fit for property-based testing… For now, just check a
        # a few edge cases with a single input order.
        vals = [5, 3, 2, 4, 1]
        self.assertEqual(find_neighbour_index(vals, vals.index(1), -2), vals.index(1))
        self.assertEqual(find_neighbour_index(vals, vals.index(1), -1), vals.index(1))
        self.assertEqual(find_neighbour_index(vals, vals.index(1), 0), vals.index(1))
        self.assertEqual(find_neighbour_index(vals, vals.index(1), 1), vals.index(2))
        self.assertEqual(find_neighbour_index(vals, vals.index(1), 2), vals.index(3))

        self.assertEqual(find_neighbour_index(vals, vals.index(3), -2), vals.index(1))
        self.assertEqual(find_neighbour_index(vals, vals.index(3), -1), vals.index(2))
        self.assertEqual(find_neighbour_index(vals, vals.index(3), 0), vals.index(3))
        self.assertEqual(find_neighbour_index(vals, vals.index(3), 1), vals.index(4))
        self.assertEqual(find_neighbour_index(vals, vals.index(3), 2), vals.index(5))

        self.assertEqual(find_neighbour_index(vals, vals.index(5), -2), vals.index(3))
        self.assertEqual(find_neighbour_index(vals, vals.index(5), -1), vals.index(4))
        self.assertEqual(find_neighbour_index(vals, vals.index(5), 0), vals.index(5))
        self.assertEqual(find_neighbour_index(vals, vals.index(5), 1), vals.index(5))
        self.assertEqual(find_neighbour_index(vals, vals.index(5), 2), vals.index(5))

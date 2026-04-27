import unittest

import numpy as np

from ndscan.experiment.scan_generator import (
    CentreSpanGenerator,
    CentreSpanRefiningGenerator,
    ExpandingGenerator,
    IntCentreSpanGenerator,
    IntCentreSpanRefiningGenerator,
    IntLinearGenerator,
    IntRefiningGenerator,
    LinearGenerator,
    ListGenerator,
    RefiningGenerator,
    ScanOptions,
    generate_points,
)


class ScanGeneratorCase(unittest.TestCase):
    def test_expanding_trivial(self):
        gen = ExpandingGenerator(
            centre=0.0,
            spacing=10.0,
            randomise_order=False,
            limit_lower=-1.0,
            limit_upper=1.0,
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        for i in range(1, 3):
            self.assertFalse(gen.has_level(i))

    def test_expanding_one(self):
        gen = ExpandingGenerator(
            centre=0.0,
            spacing=1.0,
            randomise_order=False,
            limit_lower=-1.0,
            limit_upper=1.0,
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [-1.0, 1.0])
        self.assertFalse(gen.has_level(2))

    def test_expanding_lower_lim(self):
        gen = ExpandingGenerator(
            centre=0.0, spacing=1.0, randomise_order=False, limit_lower=-1.0
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [-1.0, 1.0])
        self.assertTrue(gen.has_level(2))
        self.assertEqual(gen.points_for_level(2), [2.0])
        self.assertTrue(gen.has_level(10))
        self.assertEqual(gen.points_for_level(10), [10.0])

    def test_expanding_upper_lim(self):
        gen = ExpandingGenerator(
            centre=0.0, spacing=1.0, randomise_order=False, limit_upper=1.0
        )
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
            CentreSpanGenerator(
                centre=0.0, half_span=1.0, num_points=0, randomise_order=False
            )

    def test_centre_one(self):
        gen = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=1, randomise_order=True
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0, np.random), [0.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_two(self):
        gen = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=2, randomise_order=False
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [-1.0, 1.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_three(self):
        gen = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=3, randomise_order=False
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [-1.0, 0.0, 1.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_lower_lim(self):
        gen = CentreSpanGenerator(
            centre=0.0,
            half_span=1.0,
            num_points=2,
            randomise_order=False,
            limit_lower=0.0,
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0, 1.0])
        self.assertFalse(gen.has_level(1))

    def test_centre_upper_lim(self):
        gen = CentreSpanGenerator(
            centre=0.0,
            half_span=1.0,
            num_points=2,
            randomise_order=False,
            limit_upper=0.0,
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [-1.0, 0.0])
        self.assertFalse(gen.has_level(1))

    def test_linear_basic(self):
        gen = LinearGenerator(start=0.0, stop=2.0, num_points=3, randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0, 1.0, 2.0])
        self.assertFalse(gen.has_level(1))

    def test_linear_one_point(self):
        with self.assertRaises(ValueError):
            LinearGenerator(start=5.0, stop=5.0, num_points=1, randomise_order=False)

    def test_listgenerator(self):
        gen = ListGenerator(values=[1.0, 2.0, 3.0], randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [1.0, 2.0, 3.0])
        self.assertFalse(gen.has_level(1))

    def test_refininggenerator(self):
        gen = RefiningGenerator(lower=0.0, upper=2.0, randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0.0, 2.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [1.0])
        self.assertTrue(gen.has_level(2))
        self.assertEqual(gen.points_for_level(2), [0.5, 1.5])
        self.assertTrue(gen.has_level(3))
        self.assertEqual(gen.points_for_level(3), [0.25, 0.75, 1.25, 1.75])

    def test_centrespanrefininggenerator(self):
        gen = CentreSpanRefiningGenerator(
            centre=200.0, half_span=100.0, randomise_order=False
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [100.0, 300.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [200.0])
        self.assertTrue(gen.has_level(2))
        self.assertEqual(gen.points_for_level(2), [150.0, 250.0])
        self.assertTrue(gen.has_level(3))
        self.assertEqual(gen.points_for_level(3), [125.0, 175.0, 225.0, 275.0])

    def test_centrespanrefininggenerator_lower_lim(self):
        gen = CentreSpanRefiningGenerator(
            centre=200.0, half_span=100.0, randomise_order=False, limit_lower=120.0
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [300.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [200.0])
        self.assertTrue(gen.has_level(2))
        self.assertEqual(gen.points_for_level(2), [150.0, 250.0])
        self.assertTrue(gen.has_level(3))
        self.assertEqual(gen.points_for_level(3), [125.0, 175.0, 225.0, 275.0])

    def test_centrespanrefininggenerator_span_outside_limit(self):
        gen = CentreSpanRefiningGenerator(
            centre=200.0,
            half_span=100.0,
            randomise_order=False,
            limit_lower=120.0,
            limit_upper=250.0,
        )
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [120.0])
        self.assertTrue(gen.has_level(1))
        self.assertEqual(gen.points_for_level(1), [200.0])
        self.assertTrue(gen.has_level(2))
        self.assertEqual(gen.points_for_level(2), [160.0, 240.0])
        self.assertTrue(gen.has_level(3))
        self.assertEqual(gen.points_for_level(3), [140.0, 180.0, 220.0])


def _exhaust_levels(gen, rng=None):
    """Return ``(levels, all_points)`` for a generator, calling ``points_for_level``
    until ``has_level`` reports the scan is exhausted."""
    levels = []
    all_points = []
    level = 0
    while gen.has_level(level):
        pts = gen.points_for_level(level, rng)
        levels.append(pts)
        all_points.extend(pts)
        level += 1
    return levels, all_points


class IntRefiningCase(unittest.TestCase):
    def test_power_of_two_width(self):
        # d=8: clean halving, exhausts in (8-1).bit_length() = 3 levels after L0.
        gen = IntRefiningGenerator(lower=0, upper=8, randomise_order=False)
        levels, all_points = _exhaust_levels(gen)
        self.assertEqual(levels[0], [0, 8])
        self.assertEqual(levels[1], [4])
        self.assertEqual(levels[2], [2, 6])
        self.assertEqual(levels[3], [1, 3, 5, 7])
        self.assertEqual(len(levels), 4)
        self.assertFalse(gen.has_level(4))
        # Each integer visited exactly once, and all of [0, 8] covered.
        self.assertEqual(sorted(all_points), list(range(9)))
        self.assertEqual(len(all_points), len(set(all_points)))

    def test_odd_width(self):
        # d=7: floor-biased midpoints; still exhausts in 3 + 1 = 4 levels.
        gen = IntRefiningGenerator(lower=0, upper=7, randomise_order=False)
        levels, all_points = _exhaust_levels(gen)
        self.assertEqual(levels[0], [0, 7])
        self.assertEqual(levels[1], [3])
        self.assertEqual(levels[2], [1, 5])
        self.assertEqual(levels[3], [2, 4, 6])
        self.assertFalse(gen.has_level(4))
        self.assertEqual(sorted(all_points), list(range(8)))

    def test_range_not_a_power_of_two(self):
        # d=10: max_level = 9.bit_length() = 4; level 4 fills in stragglers.
        gen = IntRefiningGenerator(lower=0, upper=10, randomise_order=False)
        levels, all_points = _exhaust_levels(gen)
        self.assertEqual(levels[0], [0, 10])
        self.assertEqual(levels[1], [5])
        self.assertEqual(levels[2], [2, 7])
        self.assertEqual(levels[3], [1, 3, 6, 8])
        self.assertEqual(levels[4], [4, 9])
        self.assertFalse(gen.has_level(5))
        self.assertEqual(sorted(all_points), list(range(11)))

    def test_negative_range(self):
        # Floor div on negative odd sums biases the midpoint downward; verify the
        # full range is still covered exactly once.
        gen = IntRefiningGenerator(lower=-5, upper=5, randomise_order=False)
        levels, all_points = _exhaust_levels(gen)
        self.assertEqual(levels[0], [-5, 5])
        self.assertEqual(levels[1], [0])
        self.assertEqual(levels[2], [-3, 2])
        self.assertEqual(levels[3], [-4, -2, 1, 3])
        self.assertEqual(levels[4], [-1, 4])
        self.assertFalse(gen.has_level(5))
        self.assertEqual(sorted(all_points), list(range(-5, 6)))

    def test_single_point(self):
        gen = IntRefiningGenerator(lower=5, upper=5, randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [5])
        self.assertFalse(gen.has_level(1))

    def test_adjacent_endpoints(self):
        # d=1: only the two endpoints, no interior to refine.
        gen = IntRefiningGenerator(lower=3, upper=4, randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [3, 4])
        self.assertFalse(gen.has_level(1))

    def test_swapped_bounds(self):
        # Swapping bounds is normalised on the way in.
        gen = IntRefiningGenerator(lower=8, upper=0, randomise_order=False)
        self.assertEqual(gen.points_for_level(0), [0, 8])
        self.assertEqual(gen.points_for_level(1), [4])

    def test_with_limits(self):
        # Limits filter the emitted points but do not change the bisection
        # tree, so termination still respects the underlying range.
        gen = IntRefiningGenerator(lower=0, upper=10, randomise_order=False)
        gen.limit_lower = 2
        gen.limit_upper = 8
        levels, all_points = _exhaust_levels(gen)
        self.assertEqual(levels[0], [])
        self.assertEqual(levels[1], [5])
        self.assertEqual(levels[2], [2, 7])
        self.assertEqual(levels[3], [3, 6, 8])
        self.assertEqual(levels[4], [4])
        self.assertFalse(gen.has_level(5))
        self.assertEqual(sorted(all_points), list(range(2, 9)))

    def test_coverage_is_unique_for_many_widths(self):
        # Stress check: every integer in [lower, upper] is visited exactly once,
        # for a variety of widths (covering both powers of two and arbitrary).
        for d in [0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 17, 32, 33, 100]:
            gen = IntRefiningGenerator(lower=0, upper=d, randomise_order=False)
            _, all_points = _exhaust_levels(gen)
            self.assertEqual(
                sorted(all_points),
                list(range(d + 1)),
                f"coverage failure for d={d}",
            )
            self.assertEqual(
                len(all_points), len(set(all_points)), f"duplicates for d={d}"
            )


class IntCentreSpanRefiningCase(unittest.TestCase):
    def test_basic(self):
        gen = IntCentreSpanRefiningGenerator(
            centre=200, half_span=8, randomise_order=False
        )
        levels, all_points = _exhaust_levels(gen)
        self.assertEqual(levels[0], [192, 208])
        self.assertEqual(levels[1], [200])
        self.assertEqual(levels[2], [196, 204])
        self.assertEqual(levels[3], [194, 198, 202, 206])
        self.assertEqual(levels[4], [193, 195, 197, 199, 201, 203, 205, 207])
        self.assertFalse(gen.has_level(5))
        self.assertEqual(sorted(all_points), list(range(192, 209)))

    def test_lower_limit(self):
        gen = IntCentreSpanRefiningGenerator(
            centre=200, half_span=8, randomise_order=False, limit_lower=196
        )
        # Endpoint 192 is below the limit; 208 is unrestricted.
        self.assertEqual(gen.points_for_level(0), [208])
        self.assertEqual(gen.points_for_level(1), [200])
        self.assertEqual(gen.points_for_level(2), [196, 204])

    def test_span_exceeds_both_limits(self):
        # Both extents fall outside the limits → half_span re-centred to the
        # larger of the two distances, but limits still clip the emitted points.
        gen = IntCentreSpanRefiningGenerator(
            centre=200,
            half_span=100,
            randomise_order=False,
            limit_lower=120,
            limit_upper=250,
        )
        # New half_span = max(|200-120|, |250-200|) = 80, so [120, 280] internally.
        self.assertEqual(gen.lower, 120)
        self.assertEqual(gen.upper, 280)
        # Level 0: 120 is in-range, 280 is filtered (> 250).
        self.assertEqual(gen.points_for_level(0), [120])
        self.assertEqual(gen.points_for_level(1), [200])
        self.assertEqual(gen.points_for_level(2), [160, 240])


class IntLinearCase(unittest.TestCase):
    def test_basic(self):
        gen = IntLinearGenerator(start=0, stop=4, num_points=5, randomise_order=False)
        self.assertTrue(gen.has_level(0))
        self.assertEqual(gen.points_for_level(0), [0, 1, 2, 3, 4])
        self.assertFalse(gen.has_level(1))

    def test_coarse(self):
        gen = IntLinearGenerator(start=0, stop=10, num_points=3, randomise_order=False)
        self.assertEqual(gen.points_for_level(0), [0, 5, 10])

    def test_reverse(self):
        gen = IntLinearGenerator(start=4, stop=0, num_points=5, randomise_order=False)
        self.assertEqual(gen.points_for_level(0), [4, 3, 2, 1, 0])

    def test_equal_endpoints(self):
        # All linspace points collapse to the single integer.
        gen = IntLinearGenerator(start=5, stop=5, num_points=3, randomise_order=False)
        self.assertEqual(gen.points_for_level(0), [5])

    def test_more_points_than_integers(self):
        # 10 points across a width of 2 → must dedup to {0, 1, 2}.
        with self.assertLogs(level="WARNING"):
            gen = IntLinearGenerator(
                start=0, stop=2, num_points=10, randomise_order=False
            )
        self.assertEqual(gen.points_for_level(0), [0, 1, 2])
        self.assertEqual(gen.num_points, 3)

    def test_banker_rounding_at_half(self):
        # linspace(0, 2, 5) = [0, 0.5, 1.0, 1.5, 2.0]. Python's banker's rounding
        # sends 0.5 → 0 and 1.5 → 2; adjacent dedup leaves [0, 1, 2].
        gen = IntLinearGenerator(start=0, stop=2, num_points=5, randomise_order=False)
        self.assertEqual(gen.points_for_level(0), [0, 1, 2])

    def test_negative_range(self):
        gen = IntLinearGenerator(start=-2, stop=2, num_points=5, randomise_order=False)
        self.assertEqual(gen.points_for_level(0), [-2, -1, 0, 1, 2])

    def test_too_few_points(self):
        with self.assertRaises(ValueError):
            IntLinearGenerator(start=0, stop=4, num_points=1, randomise_order=False)


class IntCentreSpanCase(unittest.TestCase):
    def test_basic(self):
        gen = IntCentreSpanGenerator(
            centre=5, half_span=2, num_points=5, randomise_order=False
        )
        self.assertEqual(gen.points_for_level(0), [3, 4, 5, 6, 7])

    def test_one_point(self):
        gen = IntCentreSpanGenerator(
            centre=5, half_span=2, num_points=1, randomise_order=False
        )
        self.assertEqual(gen.points_for_level(0), [5])
        self.assertFalse(gen.has_level(1))

    def test_lower_limit(self):
        gen = IntCentreSpanGenerator(
            centre=5,
            half_span=4,
            num_points=5,
            randomise_order=False,
            limit_lower=3,
        )
        # start clamped from 1 up to 3; stop unchanged at 9.
        self.assertEqual(gen.points_for_level(0), [3, 4, 6, 7, 9])

    def test_more_points_than_integers(self):
        with self.assertLogs(level="WARNING"):
            gen = IntCentreSpanGenerator(
                centre=1, half_span=1, num_points=10, randomise_order=False
            )
        self.assertEqual(gen.points_for_level(0), [0, 1, 2])
        self.assertEqual(gen.num_points, 3)

    def test_empty_after_limits(self):
        with self.assertRaises(ValueError):
            IntCentreSpanGenerator(
                centre=0,
                half_span=1,
                num_points=3,
                randomise_order=False,
                limit_lower=5,
            )


class GeneratePointsCase(unittest.TestCase):
    def test_no_repeats(self):
        opt = ScanOptions()
        gen = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=2, randomise_order=False
        )
        points = list(generate_points([gen], opt))
        self.assertEqual(points, [(-1.0,), (1.0,)])

    def test_repeat_scan(self):
        opt = ScanOptions(num_repeats=2)
        gen = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=2, randomise_order=False
        )
        points = list(generate_points([gen], opt))
        self.assertEqual(points, [(-1.0,), (1.0,), (-1.0,), (1.0,)])

    def test_repeat_each_point(self):
        opt = ScanOptions(num_repeats=1, num_repeats_per_point=2)
        gen = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=2, randomise_order=False
        )
        points = list(generate_points([gen], opt))
        self.assertEqual(points, [(-1.0,), (-1.0,), (1.0,), (1.0,)])

    def test_repeat_scan_and_each_point(self):
        opt = ScanOptions(num_repeats=2, num_repeats_per_point=2)
        gen = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=2, randomise_order=False
        )
        points = list(generate_points([gen], opt))
        self.assertEqual(
            points, [(-1.0,), (-1.0,), (1.0,), (1.0,), (-1.0,), (-1.0,), (1.0,), (1.0,)]
        )

    def test_2d_scan(self):
        opt = ScanOptions(num_repeats=1, num_repeats_per_point=1)
        gen1 = CentreSpanGenerator(
            centre=0.0, half_span=1.0, num_points=2, randomise_order=False
        )
        gen2 = CentreSpanGenerator(
            centre=0.0, half_span=20.0, num_points=2, randomise_order=False
        )
        points = list(generate_points([gen1, gen2], opt))
        self.assertEqual(
            points, [(-1.0, -20.0), (1.0, -20.0), (-1.0, 20.0), (1.0, 20.0)]
        )

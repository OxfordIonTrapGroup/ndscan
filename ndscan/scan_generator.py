import numpy as np
import random

from itertools import product
from typing import Dict, List


class RefiningGenerator:
    def __init__(self, lower, upper, randomise_order):
        self.lower = float(min(lower, upper))
        self.upper = float(max(lower, upper))
        self.randomise_order = randomise_order

    def has_level(self, level: int):
        return True

    def points_for_level(self, level: int, rng=None):
        if level == 0:
            return [self.lower, self.upper]

        d = self.upper - self.lower
        num = 2**(level - 1)
        points = np.arange(num) * d / num + d / (2 * num) + self.lower

        if self.randomise_order:
            rng.shuffle(points)

        return points

    def describe_limits(self, target: dict):
        target["min"] = self.lower
        target["max"] = self.upper


class LinearGenerator:
    def __init__(self, start, stop, num_points, randomise_order):
        self.start = start
        self.stop = stop
        self.num_points = num_points
        self.randomise_order = randomise_order

    def has_level(self, level: int):
        return level == 0

    def points_for_level(self, level: int, rng=None):
        assert level == 0
        points = np.linspace(start=self.start, stop=self.stop, num=self.num_points, endpoint=True)
        if self.randomise_order:
            rng.shuffle(points)
        return points

    def describe_limits(self, target: dict):
        target["min"] = min(self.start, self.stop)
        target["max"] = max(self.start, self.stop)


GENERATORS = {
    "refining": RefiningGenerator,
    "linear": LinearGenerator
}


class ScanAxis:
    def __init__(self, param_schema: str, path: str, param_store, generator):
        self.param_schema = param_schema
        self.path = path
        self.generator = generator
        self.param_store = param_store

    def describe(self) -> Dict[str, any]:
        result = {
            "param": self.param_schema,
            "path": self.path,
        }
        self.generator.describe_limits(result)
        return result


class ScanSpec:
    def __init__(self, axes: List[ScanAxis], num_repeats: int,
        continuous_without_axes: bool, randomise_order_globally: bool, seed=None):
        self.axes = axes
        self.num_repeats = num_repeats
        self.continuous_without_axes = continuous_without_axes
        self.randomise_order_globally = randomise_order_globally

        if seed is None:
            seed = random.getrandbits(32)
        self.seed = seed


def generate_points(scan: ScanSpec):
    rng = np.random.RandomState(scan.seed)

    # Stores computed coordinates for each axis, indexed first by
    # axis order, then by level.
    axis_level_points = [[]] * len(scan.axes)

    max_level = 0
    while True:
        found_new_levels = False
        for i, a in enumerate(scan.axes):
            if a.generator.has_level(max_level):
                axis_level_points[i].append(a.generator.points_for_level(max_level, rng))
                found_new_levels = True

        if not found_new_levels:
            # No levels left to exhaust, done.
            return

        points = []

        for axis_levels in product(*(range(0, len(p)) for p in axis_level_points)):
            if all(l < max_level for l in axis_levels):
                # Previously visited this combination already.
                continue
            tp = product(*(p[l] for (l, p) in zip(axis_levels, axis_level_points)))
            points.extend(tp)

        for _ in range(scan.num_repeats):
            if scan.randomise_order_globally:
                rng.shuffle(points)

            for p in points:
                yield p

        max_level += 1

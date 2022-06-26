from itertools import product
import numpy as np
import random
from typing import Any, Dict, List, Iterator

__all__ = [
    "ScanGenerator", "RefiningGenerator", "LinearGenerator", "ListGenerator",
    "ScanOptions"
]


class ScanGenerator:
    """Generates points along a single scan axis to be visited.
    """
    def has_level(self, level: int) -> bool:
        """
        """
        raise NotImplementedError

    def points_for_level(self, level: int, rng=None) -> List[Any]:
        """
        """
        raise NotImplementedError

    def describe_limits(self, target: Dict[str, Any]) -> None:
        """
        """
        raise NotImplementedError


class RefiningGenerator(ScanGenerator):
    """Generates progressively finer grid by halving distance between points each level.
    """
    def __init__(self, lower, upper, randomise_order):
        self.lower = float(min(lower, upper))
        self.upper = float(max(lower, upper))
        self.randomise_order = randomise_order

    def has_level(self, level: int) -> bool:
        ""
        # For floating-point parameters, a refining scan, in practical terms, never runs
        # out of points. Will need to be amended for integer parameters.
        return True

    def points_for_level(self, level: int, rng=None) -> List[Any]:
        ""
        if level == 0:
            return [self.lower, self.upper]

        d = self.upper - self.lower
        num = 2**(level - 1)
        points = np.arange(num) * d / num + d / (2 * num) + self.lower

        if self.randomise_order:
            rng.shuffle(points)

        return points

    def describe_limits(self, target: Dict[str, Any]) -> None:
        ""
        target["min"] = self.lower
        target["max"] = self.upper


class ExpandingGenerator(ScanGenerator):
    """Generates points with given, constant spacing in progressively growing range
    around a given centre.
    """
    def __init__(self,
                 centre,
                 spacing,
                 randomise_order: bool,
                 limit_lower=None,
                 limit_upper=None):
        """
        :param limit_lower: Optional lower limit (inclusive) to the range of generated
            points. Useful for representing scans on parameters the range of which is
            limited (e.g. to be non-negative).
        :param limit_upper: See `limit_lower`.
        """
        self.centre = centre
        self.spacing = abs(spacing)
        self.randomise_order = randomise_order

        self.limit_lower = limit_lower if limit_lower is not None else float("-inf")
        if centre < self.limit_lower:
            raise ValueError("Given scan centre exceeds lower limit")

        self.limit_upper = limit_upper if limit_upper is not None else float("inf")
        if centre > self.limit_upper:
            raise ValueError("Given scan centre exceeds upper limit")

    def has_level(self, level: int) -> bool:
        ""

        def num_points(limit):
            return np.floor(abs(self.centre - limit) / self.spacing)

        return level <= max(num_points(self.limit_lower), num_points(self.limit_upper))

    def points_for_level(self, level: int, rng=None) -> List[Any]:
        ""
        if level == 0:
            return [self.centre]

        points = []
        lower = self.centre - level * self.spacing
        if lower >= self.limit_lower:
            points.append(lower)

        upper = self.centre + level * self.spacing
        if upper <= self.limit_upper:
            points.append(upper)

        if self.randomise_order:
            rng.shuffle(points)
        return points

    def describe_limits(self, target: Dict[str, Any]) -> None:
        ""
        if self.limit_lower > float("-inf"):
            target["min"] = self.limit_lower
        if self.limit_upper < float("inf"):
            target["max"] = self.limit_upper
        target["increment"] = self.spacing


class LinearGenerator(ScanGenerator):
    """Generates equally spaced points between two endpoints."""
    def __init__(self, start, stop, num_points, randomise_order):
        if num_points < 2:
            raise ValueError("Need at least 2 points in linear scan")
        self.start = start
        self.stop = stop
        self.num_points = num_points
        self.randomise_order = randomise_order

    def has_level(self, level: int) -> bool:
        ""
        return level == 0

    def points_for_level(self, level: int, rng=None) -> List[Any]:
        ""
        assert level == 0
        points = np.linspace(start=self.start,
                             stop=self.stop,
                             num=self.num_points,
                             endpoint=True)
        if self.randomise_order:
            rng.shuffle(points)
        return points

    def describe_limits(self, target: Dict[str, Any]) -> None:
        ""
        target["min"] = min(self.start, self.stop)
        target["max"] = max(self.start, self.stop)
        target["increment"] = abs(self.stop - self.start) / (self.num_points - 1)


class ListGenerator(ScanGenerator):
    """Generates points by reading from an explicitly specified list."""
    def __init__(self, values, randomise_order):
        self.values = values
        self.randomise_order = randomise_order

    def has_level(self, level: int) -> bool:
        ""
        return level == 0

    def points_for_level(self, level: int, rng=None) -> List[Any]:
        ""
        assert level == 0
        values = self.values
        if self.randomise_order:
            values = np.array(self.values)
            rng.shuffle(values)
        return values

    def describe_limits(self, target: Dict[str, Any]) -> None:
        ""
        values = np.array(self.values)
        if np.issubdtype(values.dtype, np.number):
            target["min"] = np.min(values)
            target["max"] = np.max(values)


GENERATORS = {
    "refining": RefiningGenerator,
    "expanding": ExpandingGenerator,
    "linear": LinearGenerator,
    "list": ListGenerator
}


class ScanOptions:
    """
    """
    def __init__(self,
                 num_repeats: int = 1,
                 randomise_order_globally: bool = False,
                 seed=None):
        self.num_repeats = num_repeats
        self.randomise_order_globally = randomise_order_globally

        if seed is None:
            seed = random.getrandbits(32)
        self.seed = seed


def generate_points(axis_generators: List[ScanGenerator],
                    options: ScanOptions) -> Iterator[Any]:
    rng = np.random.RandomState(options.seed)

    # Stores computed coordinates for each axis, indexed first by
    # axis order, then by level.
    axis_level_points = [[] for _ in axis_generators]

    max_level = 0
    while True:
        found_new_levels = False
        for i, a in enumerate(axis_generators[::-1]):
            if a.has_level(max_level):
                axis_level_points[i].append(a.points_for_level(max_level, rng))
                found_new_levels = True

        if not found_new_levels:
            # No levels left to exhaust, done.
            return

        points = []

        for axis_levels in product(*(range(len(p)) for p in axis_level_points)):
            if all(lvl < max_level for lvl in axis_levels):
                # Previously visited this combination already.
                continue
            tp = product(*(p[lvl] for (lvl, p) in zip(axis_levels, axis_level_points)))
            points.extend(tp)

        for _ in range(options.num_repeats):
            if options.randomise_order_globally:
                rng.shuffle(points)

            for p in points:
                yield p[::-1]

        max_level += 1

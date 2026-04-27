import random
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import product
from typing import Any
from venv import logger

import numpy as np

__all__ = [
    "ScanGenerator",
    "RefiningGenerator",
    "LinearGenerator",
    "ListGenerator",
    "CentreSpanRefiningGenerator",
    "IntRefiningGenerator",
    "IntLinearGenerator",
    "IntCentreSpanGenerator",
    "IntCentreSpanRefiningGenerator",
    "ScanOptions",
    "GENERATORS",
    "INT_GENERATORS",
]


class ScanGenerator:
    """Generates points along a single scan axis to be visited."""

    def has_level(self, level: int) -> bool:
        """ """
        raise NotImplementedError

    def points_for_level(self, level: int, rng=None) -> list[Any]:
        """ """
        raise NotImplementedError

    def describe_limits(self, target: dict[str, Any]) -> None:
        """ """
        raise NotImplementedError


class RefiningGenerator(ScanGenerator):
    """Generates progressively finer grid by halving distance between points each level."""

    def __init__(self, lower, upper, randomise_order):
        self.lower = float(min(lower, upper))
        self.upper = float(max(lower, upper))
        self.randomise_order = randomise_order
        self.limit_lower = float("-inf")
        self.limit_upper = float("inf")

    def has_level(self, level: int) -> bool:
        ""
        # For floating-point parameters, a refining scan, in practical terms, never runs
        # out of points. Will need to be amended for integer parameters.
        return True

    def points_for_level(self, level: int, rng=None) -> list[Any]:
        ""
        if level == 0:
            points = np.array([self.lower, self.upper])
        else:
            d = self.upper - self.lower
            num = 2 ** (level - 1)
            points = np.arange(num) * d / num + d / (2 * num) + self.lower

        # Silently drop points outside of the limits
        points = points[(points >= self.limit_lower) & (points <= self.limit_upper)]
        if self.randomise_order:
            rng.shuffle(points)

        return points.tolist()

    def describe_limits(self, target: dict[str, Any]) -> None:
        ""
        target["min"] = self.lower
        target["max"] = self.upper

    @staticmethod
    def args_from_centre_span(
        centre, half_span, randomise_order, limit_lower, limit_upper
    ):
        lower = centre - half_span
        upper = centre + half_span
        if lower < limit_lower and upper > limit_upper:
            # If both extents of the span exceed the limits, change the span to fit
            #   the larger of the two. This way we still don't move the scan centre.
            half_span = max(abs(centre - limit_lower), abs(limit_upper - centre))
            lower = centre - half_span
            upper = centre + half_span

        return lower, upper, randomise_order


class CentreSpanRefiningGenerator(RefiningGenerator):
    """Generates progressively finer grid in span around centre by halving distance
    between points each level. Scan is always centred on the given centre, even if the
    span exceeds the limits.
    :param limit_lower: Optional lower limit (inclusive) to the range of generated
            points. Useful for representing scans on parameters the range of which is
            limited (e.g. to be non-negative).
    :param limit_upper: See `limit_lower`.
    """

    def __init__(
        self,
        centre,
        half_span,
        randomise_order,
        limit_lower=-np.inf,
        limit_upper=np.inf,
    ):
        super().__init__(
            *super().args_from_centre_span(
                centre, half_span, randomise_order, limit_lower, limit_upper
            )
        )

        self.centre, self.half_span = centre, half_span
        self.limit_lower, self.limit_upper = limit_lower, limit_upper


class ExpandingGenerator(ScanGenerator):
    """Generates points with given, constant spacing in progressively growing range
    around a given centre.
    """

    def __init__(
        self, centre, spacing, randomise_order: bool, limit_lower=None, limit_upper=None
    ):
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

    def points_for_level(self, level: int, rng=None) -> list[Any]:
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

    def describe_limits(self, target: dict[str, Any]) -> None:
        ""
        if self.limit_lower > float("-inf"):
            target["min"] = self.limit_lower
        if self.limit_upper < float("inf"):
            target["max"] = self.limit_upper
        target["increment"] = self.spacing


class LinearGenerator(ScanGenerator):
    """Generates equally spaced points between two endpoints."""

    min_num_points = 2

    def __init__(self, start, stop, num_points, randomise_order):
        if num_points < self.min_num_points:
            raise ValueError(
                f"Need at least {self.min_num_points} point(s) in linear scan"
            )
        self.start = start
        self.stop = stop
        self.num_points = num_points
        self.randomise_order = randomise_order

    def has_level(self, level: int) -> bool:
        ""
        return level == 0

    def points_for_level(self, level: int, rng=None) -> list[Any]:
        ""
        assert level == 0
        points = np.linspace(
            start=self.start, stop=self.stop, num=self.num_points, endpoint=True
        )
        if self.randomise_order:
            rng.shuffle(points)
        return points.tolist()

    def describe_limits(self, target: dict[str, Any]) -> None:
        ""
        target["min"] = min(self.start, self.stop)
        target["max"] = max(self.start, self.stop)
        target["increment"] = abs(self.stop - self.start) / (self.num_points - 1)

    @staticmethod
    def args_from_centre_span(
        centre, half_span, num_points, randomise_order, limit_lower, limit_upper
    ):
        num_points = num_points
        randomise_order = randomise_order

        start = centre - half_span
        if limit_lower is not None:
            start = max(start, limit_lower)
        stop = centre + half_span
        if limit_upper is not None:
            stop = min(stop, limit_upper)
        if start > stop:
            raise ValueError("Empty centre/span scan (lower limit larger than upper)")

        if num_points == 1:
            start = stop = centre

        return start, stop, num_points, randomise_order


class CentreSpanGenerator(LinearGenerator):
    """Generates equally spaced points in ``centre``±``half_span``."""

    min_num_points = 1

    def __init__(
        self,
        centre,
        half_span,
        num_points: int,
        randomise_order: bool,
        limit_lower=None,
        limit_upper=None,
    ):
        """
        :param limit_lower: Optional lower limit (inclusive) to the range of generated
            points. Useful for representing scans on parameters the range of which is
            limited (e.g. to be non-negative).
        :param limit_upper: See `limit_lower`.
        """
        super().__init__(
            *super().args_from_centre_span(
                centre, half_span, num_points, randomise_order, limit_lower, limit_upper
            )
        )
        self.limit_lower, self.limit_upper = limit_lower, limit_upper


def _refining_max_level(d: int) -> int:
    """Largest refinement level at which :class:`IntRefiningGenerator`'s bisection
    scheme can contribute further integers, given range size ``d = upper - lower``.

    The bisection forms an implicit binary search tree of depth at most
    ``ceil(log2(d))`` over the integers in ``[lower, upper]`` — each level halves
    the largest open interval, until no interval has interior integers left.
    """
    if d <= 0:
        return 0
    return (d - 1).bit_length()


def _bisection_points(lower: int, upper: int, depth: int):
    """Yield, in left-to-right order, the integer midpoints at the given depth of
    the bisection tree of the open interval ``(lower, upper)``.

    Depth 0 is the root midpoint ``(lower + upper) // 2``; depth ``k`` recurses
    into the left and right halves at depth ``k - 1``. Once an interval has no
    interior integers (``upper - lower < 2``), recursion stops — each integer in
    ``[lower, upper]`` is therefore visited at most once across all depths.
    """
    if upper - lower < 2:
        return
    mid = (lower + upper) // 2
    if depth == 0:
        yield mid
        return
    yield from _bisection_points(lower, mid, depth - 1)
    yield from _bisection_points(mid, upper, depth - 1)


class IntRefiningGenerator(RefiningGenerator):
    """Integer-valued analogue of :class:`RefiningGenerator`.

    Level 0 emits ``lower`` and ``upper``; subsequent levels recursively bisect
    the resulting open intervals, emitting the integer midpoint of each. By
    construction every integer in ``[lower, upper]`` is visited at most once,
    and the scan terminates exactly once all of them have been visited.
    """

    def __init__(self, lower, upper, randomise_order):
        super().__init__(lower, upper, randomise_order)
        self.lower, self.upper = round(self.lower), round(self.upper)

    def has_level(self, level: int) -> bool:
        ""
        return level <= _refining_max_level(self.upper - self.lower)

    def points_for_level(self, level: int, rng=None) -> list[Any]:
        ""
        if level == 0:
            if self.lower == self.upper:
                points = [self.lower]
            else:
                points = [self.lower, self.upper]
        else:
            points = list(_bisection_points(self.lower, self.upper, level - 1))

        # Silently drop points outside of the limits.
        points = [p for p in points if self.limit_lower <= p <= self.limit_upper]
        if self.randomise_order:
            rng.shuffle(points)

        return points


class IntCentreSpanRefiningGenerator(IntRefiningGenerator):
    """Integer-valued analogue of :class:`CentreSpanRefiningGenerator`.

    :param limit_lower: Optional lower limit (inclusive) to the range of generated
            points.
    :param limit_upper: See ``limit_lower``.
    """

    def __init__(
        self,
        centre,
        half_span,
        randomise_order,
        limit_lower=-np.inf,
        limit_upper=np.inf,
    ):
        super().__init__(
            *super().args_from_centre_span(
                centre, half_span, randomise_order, limit_lower, limit_upper
            )
        )

        self.centre, self.half_span = centre, half_span
        self.limit_lower, self.limit_upper = limit_lower, limit_upper


class IntLinearGenerator(LinearGenerator):
    """Integer-valued analogue of :class:`LinearGenerator`.

    Computes ``num_points`` equally spaced positions between ``start`` and
    ``stop`` and rounds each to the nearest integer. Because the underlying
    floating-point sequence is monotonic, integer duplicates that appear when
    the requested density exceeds one point per integer are always adjacent —
    a single pass collapses them, so every integer is visited at most once.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start, self.stop, self.num_points = (
            round(self.start),
            round(self.stop),
            round(self.num_points),
        )

        max_num_points = abs(self.stop - self.start) + 1
        if self.num_points > max_num_points:
            logger.warning(
                "Cannot generate %d unique points in range [%d, %d]. "
                "Reducing number of points to %d.",
                self.num_points,
                self.start,
                self.stop,
                max_num_points,
            )
            self.num_points = max_num_points

    def points_for_level(self, level: int, rng=None) -> list[Any]:
        ""
        assert level == 0
        points = np.linspace(
            start=self.start,
            stop=self.stop,
            num=self.num_points,
            endpoint=True,
            dtype=int,
        )
        if self.randomise_order:
            rng.shuffle(points)
        return points.tolist()


class IntCentreSpanGenerator(IntLinearGenerator):
    """Integer-valued analogue of :class:`CentreSpanGenerator`.."""

    min_num_points = 1

    def __init__(
        self,
        centre,
        half_span,
        num_points: int,
        randomise_order: bool,
        limit_lower=None,
        limit_upper=None,
    ):
        super().__init__(
            *super().args_from_centre_span(
                centre, half_span, num_points, randomise_order, limit_lower, limit_upper
            )
        )

        self.start, self.stop = round(self.start), round(self.stop)
        self.limit_lower, self.limit_upper = limit_lower, limit_upper


class ListGenerator(ScanGenerator):
    """Generates points by reading from an explicitly specified list."""

    def __init__(self, values, randomise_order):
        self.values = values
        self.randomise_order = randomise_order

    def has_level(self, level: int) -> bool:
        ""
        return level == 0

    def points_for_level(self, level: int, rng=None) -> list[Any]:
        ""
        assert level == 0
        values = self.values
        if self.randomise_order:
            rng.shuffle(values)
        return values

    def describe_limits(self, target: dict[str, Any]) -> None:
        ""
        values = np.array(self.values)
        if np.issubdtype(values.dtype, np.number):
            target["min"] = np.min(values)
            target["max"] = np.max(values)


GENERATORS = {
    "refining": RefiningGenerator,
    "expanding": ExpandingGenerator,
    "linear": LinearGenerator,
    "centre_span": CentreSpanGenerator,
    "centre_span_refining": CentreSpanRefiningGenerator,
    "list": ListGenerator,
}

#: Generators to prefer for integer parameters; fall back to :data:`GENERATORS` for
#: scan types not listed here (``expanding`` already produces integer-spaced points,
#: ``list`` simply enumerates user-supplied values).
INT_GENERATORS = {
    "refining": IntRefiningGenerator,
    "linear": IntLinearGenerator,
    "centre_span": IntCentreSpanGenerator,
    "centre_span_refining": IntCentreSpanRefiningGenerator,
}


@dataclass
class ScanOptions:
    """ """

    #: How many times to iterate through the specified scan points.
    #:
    #: For scans with more than one level, the repetitions are executed for each level
    #: before proceeding to the next one. This way, e.g. :class:`.RefiningGenerator`,
    #: which produces infinitely many points, can still be employed in interactive
    #: work when wanting to use repeats to gather statistical information.
    num_repeats: int = 1

    #: How many times to repeat each point consecutively in a scan (i.e. without
    #: changing parameters). This is useful for scans where there is some settling time
    #: after moving to a new point.
    num_repeats_per_point: int = 1

    #: Whether to randomise the acquisition order of data points across all axes
    #: (within a refinement level).
    #:
    #: This is complementary to the randomisation options a :class:`.ScanGenerator` will
    #: typically have, as for a multi-dimensional scan, that alone would still lead to
    #: data being acquired "stripe by stripe" (hyperplane by hyperplane).
    randomise_order_globally: bool = False

    #: Global seed to use for randomising the point acquisition order (if requested).
    seed: int = None

    def __post_init__(self):
        if self.seed is None:
            self.seed = random.getrandbits(32)


def generate_points(
    axis_generators: list[ScanGenerator], options: ScanOptions
) -> Iterator[Any]:
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
                for _ in range(options.num_repeats_per_point):
                    yield p[::-1]

        max_level += 1

"""Odds and ends common to all of ndscan."""

from collections.abc import Callable, Iterable
from enum import Enum, unique
from itertools import pairwise
from typing import Any, Protocol, TypeVar

import oitg.fitting

#: Registry of well-known fit procedure names.
FIT_OBJECTS: dict[str, oitg.fitting.FitBase.FitBase] = {
    n: getattr(oitg.fitting, n)
    for n in [
        "cos",
        "decaying_sinusoid",
        "detuned_square_pulse",
        "exponential_decay",
        "gaussian",
        "line",
        "lorentzian",
        "rabi_flop",
        "sinusoid",
        "v_function",
    ]
}
FIT_OBJECTS["parabola"] = oitg.fitting.shifted_parabola

#: Name of the ``artiq.language.HasEnvironment`` argument that is used to confer the
#: list of available parameters to the dashboard plugin, and to pass the information
#: about scanned and overridden parameters to the :class:`FragmentScanExperiment`
#: when it is launched.
#:
#: Users should not need to directly interface with this.
PARAMS_ARG_KEY = "ndscan_params"

#: Revision indicator for the schema of the data produced by experiments (e.g. name of
#: datasets, semantics of parameters, etc.). Should be incremented on
#: backwards-incompatible changes, so that clients can issue warnings on unsupported new
#: versions, and, where support for older results files is desired, appropriate parsing
#: code for previous revisions can be used.
SCHEMA_REVISION = 2

#: The current :data:`.SCHEMA_REVISION` is always saved directly under the root of the
#: respective ndscan tree as `ndscan_schema_revision`, and hence can be used by
#: analysis tools (e.g. ndscan_show) to locate all ndscan roots in a results file.
SCHEMA_REVISION_KEY = "ndscan_schema_revision"


@unique
class NoAxesMode(Enum):
    """Behaviours when launching an experiment with no parameter to be scanned."""

    single = "Single (run once)"
    repeat = "Repeat (save only last)"
    time_series = "Time series (save all, with timestamps)"


def strip_prefix(string: str, prefix: str) -> str:
    if string.startswith(prefix):
        return string[len(prefix) :]
    return string


def strip_suffix(string: str, suffix: str) -> str:
    if string.endswith(suffix):
        return string[: -len(suffix)]
    return string


T = TypeVar("T")


# As of Python 3.12, there does not seem to be a straightforward way to specify T as
# being ordered; as this is going to be ``str`` anyway in virtually all cases, just
# live with the inaccuracy until we actually integrate a type checker.
def shorten_to_unambiguous_suffixes(
    fqns: Iterable[T], get_last_n_parts: Callable[[T, int], T]
) -> dict[T, T]:
    """Shorten a list of names (typically fully qualified names or paths, "FQNs") by
    removing common prefixes where possible to leave only the suffixes necessary to
    keep the names unambiguous.

    The strings are not truncated at arbitrary locations, but only at separators
    dividing the full names into parts (e.g. ``/``, ``.``). In this implementation, this
    is achieved via the ``get_last_n_parts()`` parameter, which is flexible but a bit
    inefficient; if the strings are long, the implementation should be specialised for
    e.g. the common case of parts being delimited by a single separator character.

    :param fqns: The collection of names to shorten (typically ``str``s, but could be
        an arbitrary type that is ordered and supports a notion of reversal via
        ``[::-1]``). The elements must be unique, but are not assumed to be sorted in
        any particular order.
    :param get_last_n_parts: A callable (function/lambda/…) such that
        ``get_last_n_parts(fqn, n)`` returns the ``n`` last parts of FQN. What
        constitutes a part can be adapted to the application, but it must be consistent
        with the obvious notion of "last" such that strings with the same
        ``get_last_n_parts`` result actually share a common string suffix (of
        whatever arbitrary length). The case where ``n`` exceeds the number of available
        parts should be handled by (simply returning the full
        element in that case). Example: ``lambda fqn, n: "/".join(fqn.split("/")[-n:])``
        is a simple implementation to split strings at forward slashes.
    :return: A dictionary mapping each long FQN to its shortened equivalent.
    :raises: :class:`ValueError` on duplicate fqns.
    """

    # First, sort the given names by looking at each string in reverse order. This way,
    # potentially colliding suffixes will always be adjacent to each other.
    sorted_fqns = sorted(fqns, key=lambda fqn: fqn[::-1])
    if not sorted_fqns:
        return {}

    # For every pair of names, obtain the minimum number of parts by just increasing it
    # until the names differ. This simple implementation is slightly painful to write to
    # the performance-minded programmer (needlessly quadratic time complexity), but wins
    # out on simplicity while keeping the generic get_last_n_parts() interface.
    def min_n_for_pair(fqn_a, fqn_b):
        if fqn_a == fqn_b:
            raise ValueError(f"Duplicate fqn '{fqn_a}'")
        n = 1
        while get_last_n_parts(fqn_a, n) == get_last_n_parts(fqn_b, n):
            n += 1
        return n

    # Now, build the result map by iterating through sorted_fqns and taking enough parts
    # to disambiguate each entry from its two neighbours. The first and last only have
    # one neighbour, being reflected in the initial value for min_n_with_prev and the
    # final store() call for the last element.
    result = dict[T, T]()

    def store(fqn, n):
        result[fqn] = get_last_n_parts(fqn, n)

    min_n_with_prev = 1
    for current_fqn, next_fqn in pairwise(sorted_fqns):
        min_n_with_next = min_n_for_pair(current_fqn, next_fqn)
        store(current_fqn, max(min_n_with_prev, min_n_with_next))
        min_n_with_prev = min_n_with_next
    store(sorted_fqns[-1], min_n_with_prev)

    return result


class GetDataset(Protocol):
    """Callback which is used to implement the user-facing ``dataset(…)`` default value
    syntax.

    If the ``key`` dataset does not exist, the callback should return the value given in
    the second parameter, ``default``, or if that is not specified, raise an exception.
    """

    def __call__(self, key: str, default: T | None = None) -> T: ...


def eval_param_default(value: str, get_dataset: GetDataset) -> Any:
    from artiq.language import units

    env = {name: getattr(units, name) for name in units.__all__}
    env.update({"dataset": get_dataset})
    return eval(value, env)


def merge_no_duplicates(target: dict, source: dict, kind: str = "entries") -> None:
    """Merges ``source`` into ``target``, raising a ``ValueError`` on duplicate keys."""
    for k, v in source.items():
        if k in target:
            raise ValueError(f"Duplicate {kind} of key '{k}'")
        target[k] = v
    return target

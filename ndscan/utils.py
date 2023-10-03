"""Odds and ends common to all of ndscan."""

from collections.abc import Callable, Iterable
from enum import Enum, unique
import oitg.fitting
from typing import Any, Protocol, TypeVar

#: Registry of well-known fit procedure names.
FIT_OBJECTS = {
    n: getattr(oitg.fitting, n)
    for n in [
        "cos", "decaying_sinusoid", "detuned_square_pulse", "exponential_decay",
        "gaussian", "line", "lorentzian", "rabi_flop", "sinusoid", "v_function"
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
        return string[len(prefix):]
    return string


def strip_suffix(string: str, suffix: str) -> str:
    if string.endswith(suffix):
        return string[:-len(suffix)]
    return string


def shorten_to_unambiguous_suffixes(
        fqns: Iterable[str], get_last_n_parts: Callable[[str, int],
                                                        str]) -> dict[str, str]:
    short_to_fqns = dict()
    shortened_fqns = dict()

    for current in fqns:
        if current in shortened_fqns:
            continue

        n = 1
        while True:
            candidate = get_last_n_parts(current, n)
            if candidate not in short_to_fqns:
                short_to_fqns[candidate] = {current}
                shortened_fqns[current] = candidate
                break

            # We have a conflict, disambiguate.
            existing_fqns = short_to_fqns[candidate]
            for old in existing_fqns:
                if shortened_fqns[old] == candidate:
                    # This hasn't previously been moved to a higher n, so
                    # do it now.
                    new = get_last_n_parts(old, n + 1)
                    shortened_fqns[old] = new
                    short_to_fqns[new] = {old}
                    break  # Exits inner for loop.
            existing_fqns.add(current)
            n += 1

    return shortened_fqns


T = TypeVar("T")


class GetDataset(Protocol):
    """Callback which is used to implement the user-facing ``dataset(…)`` default value
    syntax.

    If the ``key`` dataset does not exist, the callback should return the value given in
    the second parameter, ``default``, or if that is not specified, raise an exception.
    """
    def __call__(self, key: str, default: T | None = None) -> T:
        ...


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

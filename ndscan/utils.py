from artiq.language import units
from typing import Any, Callable, Dict, List


def path_matches_spec(path: List[str], spec: str) -> bool:
    # TODO: Think about how we want to match.
    if spec == "*":
        return True
    if "*" in spec:
        raise NotImplementedError(
            "Non-trivial wildcard path specifications not implemented yet")
    return "/".join(path) == spec


def strip_prefix(string: str, prefix: str) -> str:
    if string.startswith(prefix):
        return string[len(prefix):]
    return string


def will_spawn_kernel(func) -> bool:
    if not hasattr(func, "artiq_embedded"):
        return False
    meta = func.artiq_embedded
    return meta.core_name is not None and not meta.portable


def shorten_to_unambiguous_suffixes(
        fqns: List[str], get_last_n_parts: Callable[[str, int], str]) -> Dict[str, str]:
    short_to_fqns = dict()
    shortened_fqns = dict()

    for current in fqns:
        if current in shortened_fqns:
            continue

        n = 1
        while True:
            candidate = get_last_n_parts(current, n)
            if candidate not in short_to_fqns:
                short_to_fqns[candidate] = set([current])
                shortened_fqns[current] = candidate
                break

            # We have a conflict, disambiguate.
            existing_fqns = short_to_fqns[candidate]
            for old in existing_fqns:
                if shortened_fqns[old] == candidate:
                    # This hasn't previously been moved to a higher n, so
                    # do it now.
                    shortened_fqns[old] = get_last_n_parts(old, n + 1)
                    break  # Exits inner for loop.
            existing_fqns.add(current)
            n += 1

    return shortened_fqns


def eval_param_default(value: str, get_dataset: Callable) -> Any:
    env = {name: getattr(units, name) for name in units.__all__}
    env.update({"dataset": get_dataset})
    return eval(value, env)

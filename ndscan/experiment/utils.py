from typing import Iterable


def path_matches_spec(path: Iterable[str], spec: str) -> bool:
    # TODO: Think about how we want to match.
    if spec == "*":
        return True
    if "*" in spec:
        raise NotImplementedError(
            "Non-trivial wildcard path specifications not implemented yet")
    return "/".join(path) == spec


def is_kernel(func) -> bool:
    if not hasattr(func, "artiq_embedded"):
        return False
    meta = func.artiq_embedded
    return meta.core_name is not None and not meta.portable

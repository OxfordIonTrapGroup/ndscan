import json
import numpy
from typing import Any, Iterable, Optional
import dataclasses
import enum


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


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        if isinstance(obj, numpy.floating):
            return float(obj)
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        if isinstance(obj, enum.Enum):
            return obj.value
        return json.JSONEncoder.default(self, obj)


def dump_json(obj: Any) -> str:
    """Serialise ``obj`` as a JSON string, with NumPy numerical/array types encoded as
    their vanilla Python counterparts.
    """
    return json.dumps(obj, cls=Encoder)


def to_metadata_broadcast_type(obj: Any) -> Optional[Any]:
    """Return ``obj`` in a form that can be directly broadcast/saved as a dataset, or
    (conservatively) return ``None`` if this is not possible.

    Since dataset values need to be exportable to HDF5 using h5py without any further
    configuration, and at the same time publishable via sipyco (i.e. PYON), the set of
    allowable types is quite restricted. (Notably, maps andnon-rectangular arrays are
    not supported). If compatibility is not assured, this function conservatively
    returns ``None``, so the value
    """
    if isinstance(obj, numpy.integer):
        return int(obj)
    if isinstance(obj, numpy.floating):
        return float(obj)
    if isinstance(obj, int) or isinstance(obj, float) or isinstance(obj, str):
        return obj
    return None

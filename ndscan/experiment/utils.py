import json
from collections.abc import Callable, Iterable
from typing import Any

import numpy

from ..utils import SCHEMA_REVISION, SCHEMA_REVISION_KEY


def path_matches_spec(path: Iterable[str], spec: str) -> bool:
    # TODO: Think about how we want to match.
    if spec == "*":
        return True
    if "*" in spec:
        raise NotImplementedError(
            "Non-trivial wildcard path specifications not implemented yet"
        )
    return "/".join(path) == spec


def is_kernel(func) -> bool:
    if not hasattr(func, "artiq_embedded"):
        return False
    meta = func.artiq_embedded
    return meta.core_name is not None and not meta.portable


class NumpyToVanillaEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        if isinstance(obj, numpy.floating):
            return float(obj)
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def dump_json(obj: Any) -> str:
    """Serialise ``obj`` as a JSON string, with NumPy numerical/array types encoded as
    their vanilla Python counterparts.
    """
    return json.dumps(obj, cls=NumpyToVanillaEncoder)


def to_metadata_broadcast_type(obj: Any) -> Any | None:
    """Return ``obj`` in a form that can be directly broadcast/saved as a dataset, or
    (conservatively) return ``None`` if this is not possible.

    Since dataset values need to be exportable to HDF5 using h5py without any further
    configuration, and at the same time publishable via sipyco (i.e. PYON), the set of
    allowable types is quite restricted. (Notably, maps and non-rectangular arrays are
    not supported.) If compatibility is not assured, this function conservatively
    returns ``None``, such that callers can choose a safe encoding.
    """
    if isinstance(obj, numpy.integer):
        return int(obj)
    if isinstance(obj, numpy.floating):
        return float(obj)
    if isinstance(obj, int) or isinstance(obj, float) or isinstance(obj, str):
        return obj
    return None


def to_metadata_broadcast_value(obj: Any) -> Any:
    """Return the given metadata value in a form directly representable as a dataset,
    flattening anything else (arrays, dictionaries, …) to a JSON string.

    Use this for every metadata value written to a scan dataset root, such that the
    same key always holds the same representation (see
    :func:`to_metadata_broadcast_type`).
    """
    ds_value = to_metadata_broadcast_type(obj)
    return dump_json(obj) if ds_value is None else ds_value


def broadcast_scan_metadata(
    push: Callable[[str, Any], None], source_id: str, scan_desc: dict[str, Any]
) -> None:
    """Push scan metadata via ``push(name, value)`` in the standard layout for a
    top-level scan (schema revision, source id, ``completed = False``, plus the given
    scan description, with values not directly representable as datasets flattened to
    JSON strings).

    ``axes`` is always pushed last, such that subscribers observing the (ordered)
    stream of dataset modifications can use a change to it as the marker for having
    seen a consistent new set of metadata.
    """
    push(SCHEMA_REVISION_KEY, SCHEMA_REVISION)
    push("source_id", source_id)
    push("completed", False)

    def push_flattened(name, value):
        # Flatten arrays/dictionaries to JSON strings for HDF5 compatibility.
        push(name, to_metadata_broadcast_value(value))

    for name, value in scan_desc.items():
        if name != "axes":
            push_flattened(name, value)
    if "axes" in scan_desc:
        push_flattened("axes", scan_desc["axes"])


def issue_create_applet_ccb(
    ccb, title: str, dataset_prefix: str, group: str | list[str] = "ndscan"
) -> None:
    """Issue a client control broadcast to create an ndscan applet displaying the scan
    published under the given dataset prefix.
    """
    cmd = [
        "${python}",
        "-m ndscan.applet",
        "--server=${server}",
        "--port-notify=${port_notify}",
        "--port-control=${port_control}",
        f"--prefix={dataset_prefix}",
    ]
    ccb.issue("create_applet", title, " ".join(cmd), group=group)

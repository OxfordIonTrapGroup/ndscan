from typing import Any
from ..utils import SCHEMA_REVISION_KEY, strip_suffix


def find_ndscan_roots(datasets: dict[str, Any]) -> list[str]:
    """Detect ndscan roots among the passed datasets, and returns a list of the name
    prefixes (e.g. ``ndscan.``).
    """
    results = []
    for name in datasets.keys():
        if name == SCHEMA_REVISION_KEY or name.endswith("." + SCHEMA_REVISION_KEY):
            results.append(strip_suffix(name, SCHEMA_REVISION_KEY))
    if not results:
        # This might be an old file before the schema revision dataset and multiple
        # roots were added.
        if "ndscan.axes" in datasets.keys():
            results.append("ndscan.")
    return results


def get_source_id(datasets: dict[str, Any], prefixes: list[str]):
    # Take source_id from first prefix. This is pretty arbitrary, but for
    # experiment-generated files, they will all be the same anyway.
    if (prefixes[0] + "source_id") in datasets:
        source = datasets[prefixes[0] + "source_id"][()]
        if isinstance(source, bytes):
            # h5py 3+ – can use datasets[…].asstr() as soon as we don't support
            # version 2 any longer.
            source = source.decode("utf-8")
    else:
        # Old ndscan versions had a rid dataset instead of source_id.
        source = "rid_{}".format(datasets[prefixes[0] + "rid"][()])

    return source

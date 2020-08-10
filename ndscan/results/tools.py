from typing import Any, Dict, List
from ..utils import SCHEMA_REVISION_KEY, strip_suffix


def find_ndscan_roots(datasets: Dict[str, Any]) -> List[str]:
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

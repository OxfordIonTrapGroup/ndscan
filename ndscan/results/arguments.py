"""
Functions for pretty-printing user argument data (scan parameters, overrides, …) for
FragmentScanExperiments from ARTIQ results.
"""
from typing import Any, Dict, Iterable
from sipyco import pyon
from ..utils import PARAMS_ARG_KEY


def extract_param_schema(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Extract ndscan parameter data from the given ARTIQ arguments directory.

    :param arguments: The arguments for an ARTIQ experiment, as e.g. obtained using
        ``oitg.results.load_hdf5_file(…)["expid"]["arguments"]``.
    """
    try:
        string = arguments[PARAMS_ARG_KEY]
    except KeyError:
        raise KeyError(f"ndscan argument ({PARAMS_ARG_KEY}) not found")

    return pyon.decode(string)


def format_numeric(value, spec: Dict[str, Any]) -> str:
    unit = spec.get("unit", "")
    if not unit:
        return str(value)
    return f"{value / spec['scale']} {unit}"


def dump_overrides(schema: Dict[str, Any]) -> Iterable[str]:
    """Format information about overrides as a human-readable string.

    :return: Generator yielding the output line-by-line.
    """
    for fqn, overrides in schema["overrides"].items():
        for override in overrides:
            ps = schema["schemata"][fqn]
            value = format_numeric(override["value"], ps["spec"])
            yield f" - {ps['description']}: {value}"
            path = override["path"] or "*"
            yield f"   ({fqn}@{path})"


def format_scan_range(typ: str, rang: Dict[str, Any], param_spec: Dict[str,
                                                                       Any]) -> str:
    if typ == "linear":
        start = format_numeric(rang["start"], param_spec["spec"])
        stop = format_numeric(rang["stop"], param_spec["spec"])
        return f"{start} to {stop}, {rang['num_points']} points"
    if typ == "refining":
        lower = format_numeric(rang["lower"], param_spec["spec"])
        upper = format_numeric(rang["upper"], param_spec["spec"])
        return f"{lower} to {upper}, refining"
    if typ == "list":
        return f"list: [{rang['values']}]"

    return f"<Unknown scan type '{typ}'.>"


def dump_scan(schema: Dict[str, Any]) -> Iterable[str]:
    """Format information about the configured scan (if any) as a human-readable string.

    :return: Generator yielding the output line-by-line.
    """

    scan = schema["scan"]

    axes = scan["axes"]
    if not axes:
        yield f"No scan (mode: {scan['no_axis_mode']})"
        return

    yield " - Axes:"
    for ax in axes:
        fqn = ax["fqn"]
        ps = schema["schemata"][fqn]
        path = ax["path"] or "*"
        yield f"   - {ps['description']} ({fqn}@{path}):"
        yield f"     {format_scan_range(ax['type'], ax['range'], ps)}"
    yield f" - Number of repeats: {scan['num_repeats']}"
    yield f" - Randomise order globally: {scan['randomise_order_globally']}"

from typing import Any, Dict, List, Tuple, Union, Optional
from ..utils import SCHEMA_REVISION_KEY, strip_suffix
import json
from sipyco import pyon
from oitg.results import load_result
from dataclasses import dataclass
import numpy as np


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


@dataclass
class ResultData:
    data: np.ndarray
    data_raw: np.ndarray
    spec: dict


@dataclass
class ResultAxis:
    data: np.ndarray
    data_raw: np.ndarray
    description: str
    path: str
    step: float
    scale: float
    unit: str
    ax_idx: int


@dataclass
class ResultArgs:
    value: Any
    fqn: str
    path: str
    unit: str
    scale: float
    is_ndscan: bool


def load_ndscan(
    day: Union[None, str, List[str]] = None,
    hour: Union[None, int, List[int]] = None,
    rid: Union[None, int, List[int]] = None,
    class_name: Union[None, str, List[str]] = None,
    experiment: Optional[str] = None,
    root_path: Optional[str] = None,
) -> Tuple[Dict[str, ResultData], List[ResultAxis], Dict[str, ResultArgs], Dict[str,
                                                                                Any]]:
    """
    Unpacks the results from an N-dimensional ndscan experiment to make scan data
    and axes more accessible. Returns sorted results and axes.

    :return: A tuple containing the following:
        - scan_results: a dictionary containing ResultData instances for each
            results channel, mapped to by the name of the results channel. Each
            ResultData instance contains attributes:

                - data: numpy N-dimensional array (or N+M dimensional for results
                    channels with M-dimensional lists) containing data sorted according
                    to the sorted scan axes. If the data cannot be sorted, array is
                    filled with nan.
                - data_raw: numpy array containing the raw scan results.
                - spec: results spec.

        - scan_axes: a list of ResultAxis instances that each contain the scan axis data
            in each scanned parameter. The axes are ordered with the innermost axis
            first. Each ResultAxis contains attributes:

                - data: numpy array containing the sorted axis data. If data cannot
                    be sorted, array is filled with nan.
                - data_raw: numpy array containing the raw scanned axis data.
                - description: The param description provided in the experiment
                    (if any).
                - path: Path to the scanned param.
                - spec: Param spec dictionary.
                - ax_idx: The index of the axis in the N-dimensional scan, with 0 being
                    the innermost axis being scanned.

        - args: A dictionary containing the arguments submitted to the experiment.

        - raw_results: the raw output of load_result().
    """
    # TODO: add analyses and annotations.
    raw_results = load_result(
        day=day,
        hour=hour,
        rid=rid,
        class_name=class_name,
        experiment=experiment,
        root_path=root_path,
    )
    d = raw_results["datasets"]
    a = raw_results["expid"]["arguments"]
    base_key = f"ndscan.rid_{rid}."

    axs = json.loads(d[base_key + "axes"])
    if axs == []:
        scan_axes = []
        points_key = "point."
    else:
        scan_axes = [
            ResultAxis(
                data=np.full(np.shape(d[base_key + f"points.axis_{i}"]), np.nan),
                data_raw=d[base_key + f"points.axis_{i}"],
                description=ax["param"].get("description", ""),
                path=ax["path"],
                scale=ax["param"]["spec"]["scale"],
                step=ax["param"]["spec"]["step"],
                unit=ax["param"]["spec"]["unit"],
                ax_idx=i,
            ) for i, ax in enumerate(axs)
        ]
        points_key = "points.channel_"

    ndscan_results_channel_spec = json.loads(d[base_key + "channels"])
    scan_results = {}
    for chan, spec in ndscan_results_channel_spec.items():
        try:
            dat = d[base_key + points_key + chan]
            scan_results[chan] = ResultData(
                data=np.full(np.shape(dat), np.nan),
                data_raw=dat,
                spec=spec,
            )
        except KeyError:
            print(f"Results channel {chan} not found.")

    scan_results, scan_axes = sort_data(scan_results, scan_axes)

    args = {}
    for key, arg in a.items():
        if key == "ndscan_params":
            ndscan_params = pyon.decode(arg)
            for fqn, overrides in ndscan_params["overrides"].items():
                for override in overrides:
                    schem = ndscan_params["schemata"][fqn]
                    value = override["value"]
                    description = schem["description"]
                    path = override["path"]
                    try:
                        args[description] = ResultArgs(
                            value=value,
                            fqn=fqn,
                            path=path,
                            unit=schem.get("unit", ""),
                            scale=schem["spec"]["scale"],
                            is_ndscan=True,
                        )
                    except KeyError:
                        print(f"Could not get args for {fqn}.")

            args["scan"] = ndscan_params["scan"]

        else:
            # TODO: find the arg values for non-ndscan arguments too.
            args[key] = {"value": arg, "ndscan": False}
            args[key] = ResultArgs(
                value=arg,
                fqn="",
                path="",
                unit="",
                scale=1,
                is_ndscan=False,
            )
    args["completed"] = d[base_key + "completed"]

    return scan_results, scan_axes, args, raw_results


def sort_data(
    scan_results: Dict[str, ResultData], scan_axes: List[ResultAxis]
) -> Tuple[Dict[str, ResultData], List[Dict[str, ResultData]]]:
    """
    Sort the results of an N-dimensional scan. Takes in dictionaries with
    entries 'data_raw' and adds an entry 'data' with a sorted scan axis, or
    a sorted N-dimensional array of results values that match the axes. If a
    result value is missing (due to eg an unfinished refined scan), entries
    are left as np.nan.

    Returns the (mutated) input scan_results and scan_axes dictionaries. If
    the scan data can't be sorted, sets 'data' entry to None.
    """
    # If the experiment is not a scan, nothing to sort.
    if len(scan_axes) == 0:
        for result in scan_results.items():
            result.data = result.data_raw
        return scan_results, scan_axes

    # Sort the axis data into 1-D arrays.
    for axis in scan_axes:
        axis.data = np.unique(axis.data_raw)
    axes_lengths = [np.size(ax.data) for ax in scan_axes]
    num_points = len(scan_axes[0].data_raw)

    # Find the coordinates of each point in the raw result data according to the
    # sorted axes.
    coords = []
    for point_num in range(num_points):
        _coords = []
        for ax in scan_axes:
            idcs = np.nonzero(ax.data == ax.data_raw[point_num])
            _coords.append(idcs[0][0])
        coords.append(tuple(np.flip(_coords)))

    # Create N-dimensional arrays that store the result data, according to
    # the obtained coordinates. If a coordinate is missing (due to eg an
    # unfinished refined scan) leaves entry as nan.
    for key, dat_dict in scan_results.items():
        dat_raw = dat_dict.data_raw
        # Take into account results channels that are arrays.
        data_shape = np.shape(dat_raw)
        _axes = tuple(
            np.concatenate((np.flip(axes_lengths), data_shape[1:])).astype(int))
        _dat_sorted = np.full(_axes, np.nan)
        try:
            for point_number, d in enumerate(dat_raw):
                _dat_sorted[coords[point_number]] = d
            scan_results[key].data = _dat_sorted
        except Exception:
            print(f"Couldn't sort results channel {key}. Filling 'data' entry with nan")
        scan_results[key].data = _dat_sorted

    return scan_results, scan_axes

"""Standalone tool to write scan data to text files"""

import argparse
import json
import os

import numpy as np

from .results.tools import get_source_id
from .show import load_h5
from .utils import shorten_to_unambiguous_suffixes, strip_suffix


def get_argparser():
    parser = argparse.ArgumentParser(
        description="Extracts ndscan results from ARTIQ HDF5 file to text file",
        epilog=(
            "Instead of a file name, just a run id or 'magic' source string can be "
            "supplied, which is then resolved using oitg.results (e.g. 'alice_12345', "
            "or just '12345' to infer the experiment name from the environment)."
        ),
    )
    parser.add_argument(
        "--prefix",
        default=None,
        type=str,
        help="Prefix of root in dataset tree (default: auto-detect)",
    )
    parser.add_argument(
        "path", metavar="FILE", type=str, help="Path to HDF5 results file"
    )
    return parser


def main():
    args = get_argparser().parse_args()

    path, datasets, prefixes, schema = load_h5(args)
    if len(prefixes) > 1:
        raise Exception(
            "More than one ndscan prefix found. Please specify one. "
            + f"Prefixes: {prefixes}"
        )
    prefix = prefixes[0]

    # Use parameter names from last part of FQNs (maybe more if ambiguous) as column
    # labels in the header line.
    # FIXME: This breaks if there are two axes with the same FQN (but different paths).
    fqns = [ax["param"]["fqn"] for ax in json.loads(datasets[prefix + "axes"][()])]
    axes_names = list(
        shorten_to_unambiguous_suffixes(
            fqns, lambda fqn, n: ".".join(fqn.split(".")[-n:])
        ).values()
    )

    channel_names = []
    point_data = {}

    channel_schemata = json.loads(datasets[prefix + "channels"][()])

    for i, name in enumerate(axes_names):
        dat = datasets[prefix + f"points.axis_{i}"][:]
        point_data[name] = dat

    for c, cval in channel_schemata.items():
        if cval["type"] == "subscan":
            continue
        dat = datasets[prefix + "points.channel_" + c][:]
        # Ignore multi-dimensional point data.
        if len(dat.shape) == 1:
            point_data[c] = dat
            channel_names.append(c)

    num_points = [a.size for a in point_data.values()]
    if len(np.unique(num_points)) != 1:
        print("Point data is not rectangular. Deleting incomplete last point(s).")
        min_len = np.min(num_points)
        for key in point_data.keys():
            point_data[key] = point_data[key][:min_len]

    # Order by axis values, with x axis varying fastest
    if len(axes_names) > 1:
        order = np.lexsort(tuple(point_data[name] for name in axes_names))
    else:
        order = np.argsort(point_data[axes_names[0]])

    column_names = axes_names + channel_names
    ordered_data = [point_data[name][order] for name in column_names]

    # Save into current working directory.
    target_path = strip_suffix(os.path.basename(path), ".h5") + ".txt"
    print(f"Saving axes and point data to '{target_path}'")
    with open(target_path, "wb") as f:
        f.write(
            bytes(
                f"# source id: {get_source_id(datasets, prefixes)}, "
                + f"original file: {path}, dataset prefix: {prefix[:-1]}\n",
                "UTF-8",
            )
        )
        np.savetxt(
            f,
            np.transpose(ordered_data + [order]),
            comments="",
            header=" ".join(column_names + ["acquisition_order"]),
            fmt="%1.6e " * len(column_names) + "%1.0f",
        )


if __name__ == "__main__":
    main()

"""Standalone tool to show ndscan plots from ARTIQ HDF5 results files."""

import asyncio
import argparse
import h5py
import os
import sys
from quamash import QEventLoop, QtWidgets

from .plots.container_widgets import PlotContainerWidget
from .plots.model import Context
from .plots.model.hdf5 import HDF5Root


def get_argparser():
    parser = argparse.ArgumentParser(
        description="Displays ndscan plot from ARTIQ HDF5 results file")
    parser.add_argument("path", metavar="FILE", help="Path to HDF5 results file")
    return parser


def main():
    args = get_argparser().parse_args()

    app = QtWidgets.QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    file = h5py.File(args.path, "r")
    try:
        file["datasets"]["ndscan.axes"][()]
    except KeyError:
        QtWidgets.QMessageBox.critical(
            None, "Not an ndscan file",
            "No ndscan result datasets found in file: '{}'".format(args.path))
        sys.exit(1)

    try:
        context = Context()
        context.set_title(os.path.basename(args.path))
        # Old ndscan versions had a rid dataset instead of source_id.
        datasets = file["datasets"]
        if "ndscan.source_id" in datasets:
            context.set_source_id(datasets["ndscan.source_id"][()])
        else:
            context.set_source_id("rid_{}".format(datasets["ndscan.rid"][()]))
        root = HDF5Root(datasets, context)
    except Exception as e:
        QtWidgets.QMessageBox.critical(
            None, "Error parsing ndscan file",
            "Error parsing datasets in '{}': {}".format(args.path, e))
        sys.exit(2)

    widget = PlotContainerWidget(root.get_model())
    widget.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()

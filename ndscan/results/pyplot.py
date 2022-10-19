"""
Functions to automatically display plots for ndscan data using Matplotlib.

These are not intended to be a fully-featured replacement for ``ndscan.plots``, just a
quick way to peek at data in a Matplotlib-centric workflow where pyqtgraph might not be
available.
"""
import json
import matplotlib.pyplot as plt
import numpy as np
from typing import Any, Dict
from ..plots.utils import extract_scalar_channels
from .tools import find_ndscan_roots


def make_default_1d_plot(datasets: Dict[str, Any],
                         root: str,
                         figure,
                         *,
                         channel_filter=lambda name: True):
    def ds(key, is_json=False):
        val = datasets[root + key]
        if is_json:
            return json.loads(val)
        return val

    axis_schema, = ds("axes", is_json=True)
    x_schema = axis_schema["param"]
    x_label = x_schema["description"]
    x_unit = x_schema["spec"].get("unit", "")
    x_scale = x_schema["spec"]["scale"]
    if x_unit:
        x_label += " / " + x_unit

    channel_schemata = ds("channels", is_json=True)
    data_names, _ = extract_scalar_channels(channel_schemata)
    y_axis_names = list(filter(channel_filter, data_names))
    plt_axes = figure.subplots(nrows=len(y_axis_names), sharex=True)
    plt_axes = np.atleast_1d(plt_axes)
    plt_axes[-1].set_xlabel(x_label)

    x_vals = np.array(ds("points.axis_0")) / x_scale
    ascending = np.argsort(x_vals)

    for name, plt_axis in zip(y_axis_names, plt_axes):
        y_schema = channel_schemata[name]
        y_unit = y_schema["unit"]
        y_label = y_schema["description"]
        if not y_label:
            y_label = name
        if y_unit:
            y_label += " / " + y_unit
        y_vals = np.array(ds("points.channel_" + name)) / y_schema["scale"]
        plt_axis.plot(x_vals[ascending], y_vals[ascending])
        plt_axis.set_ylabel(y_label)

    title = ds("source_id")
    if root != "ndscan.":
        title += ": " + root[:-1]
    plt_axes[0].set_title(title)

    figure.tight_layout()


def make_default_plot(datasets: Dict[str, Any],
                      root: str,
                      figure,
                      *,
                      channel_filter=lambda name: True):
    """Render a plot for the specified ndscan root to the given PyPlot figure.

    :param channel_filter: Called with the name for each result channel; if False, the
        channel is not displayed.
    """
    num_axes = len(json.loads(datasets[root + "axes"]))
    if num_axes == 1:
        make_default_1d_plot(datasets, root, figure, channel_filter=channel_filter)
    else:
        raise NotImplementedError(
            "Default plots for {}-dimensional scans not yet implemented".format(
                num_axes))


def auto_plot(datasets: Dict[str, Any], *, channel_filter=lambda name: True):
    """Display PyPlot figures for all the ndscan roots found among the passed datasets.

    :param channel_filter: Called with the name for each result channel; if False, the
        channel is not displayed.
    """
    roots = find_ndscan_roots(datasets)
    for root in roots:
        fig = plt.figure(figsize=(10, 8))
        try:
            make_default_plot(datasets, root, fig, channel_filter=channel_filter)
        except NotImplementedError as e:
            print("Skipping root '{}': {}".format(root, e))

"""Pseudocolor 2D plot for equidistant data."""

from concurrent.futures import ProcessPoolExecutor
from itertools import chain, repeat
import json
import logging
import numpy as np
from oitg import uncertainty_to_string
import pyqtgraph
from quamash import QtWidgets, QtCore
from typing import Union

from . import colormaps
from .cursor import LabeledCrosshairCursor
from .utils import (extract_linked_datasets, extract_scalar_channels, setup_axis_item)

logger = logging.getLogger(__name__)


class NotEnoughPoints(ValueError):
    pass


def _calc_range_spec(preset_min, preset_max, preset_increment, data):
    udata = np.unique(data)
    if len(udata) < 2:
        raise NotEnoughPoints

    lower = preset_min if preset_min else udata[0]
    upper = preset_max if preset_max else udata[-1]
    increment = preset_increment if preset_increment else np.min(udata[1:] - udata[:-1])

    return lower, upper, increment


def _num_points_in_range(range_spec):
    min, max, increment = range_spec
    return int((max - min) / increment + 1)


def _coords_to_indices(coords, range_spec):
    min, max, increment = range_spec
    return np.rint((np.array(coords) - min) / increment).astype(int)


class _ImagePlot:
    def __init__(self, image_item: pyqtgraph.ImageItem, active_channel_name: str,
                 x_min: Union[float, None], x_max: Union[float, None],
                 x_increment: Union[float, None], y_min: Union[float, None],
                 y_max: Union[float, None], y_increment: Union[float, None]):
        self.image_item = image_item
        self.active_channel_name = active_channel_name

        self.x_min = x_min
        self.x_max = x_max
        self.x_increment = x_increment

        self.y_min = y_min
        self.y_max = y_max
        self.y_increment = y_increment

        self.num_shown = 0
        self.x_range = None
        self.y_range = None
        self.image_data = None

    def activate_channel(self, channel_name: str):
        self.active_channel_name = channel_name
        self.num_shown = 0
        self._update()

    def data_changed(self, datasets):
        self.datasets = datasets
        self._update()

    def _update(self):
        def d(name):
            return self.datasets.get("ndscan.points." + name, (False, None))[1]

        x_data, y_data = d("axis_0"), d("axis_1")
        z_data = d("channel_" + self.active_channel_name)
        num_to_show = min(len(x_data), len(y_data), len(z_data))

        if num_to_show == self.num_shown:
            return
        num_skip = self.num_shown
        self.num_shown = num_to_show

        try:
            x_range = _calc_range_spec(self.x_min, self.x_max, self.x_increment, x_data)
            y_range = _calc_range_spec(self.y_min, self.y_max, self.y_increment, y_data)
        except NotEnoughPoints:
            # Not enough points yet, will retry next time around.
            return

        if x_range != self.x_range or y_range != self.y_range:
            self.x_range = x_range
            self.y_range = y_range

            # TODO: Splat old data for progressively less blurry look on refining scans?
            self.image_data = np.full(
                (_num_points_in_range(x_range), _num_points_in_range(y_range), 4),
                0,
                dtype="ubyte")
            self.image_rect = QtCore.QRectF(
                QtCore.QPointF(x_range[0] - x_range[2] / 2,
                               y_range[0] - y_range[2] / 2),
                QtCore.QPointF(x_range[1] + x_range[2] / 2,
                               y_range[1] + y_range[2] / 2))

            num_skip = 0

        x_inds = _coords_to_indices(x_data[num_skip:num_to_show], self.x_range)
        y_inds = _coords_to_indices(y_data[num_skip:num_to_show], self.y_range)

        # TODO: Range!
        self.image_data[x_inds, y_inds, :] = colormaps.plasma.map(
            z_data[num_skip:num_to_show])

        self.image_item.setImage(self.image_data, autoLevels=False)
        if num_skip == 0:
            self.image_item.setRect(self.image_rect)


class Image2DPlotWidget(pyqtgraph.PlotWidget):
    error = QtCore.pyqtSignal(str)

    def __init__(self, x_schema, y_schema, set_dataset):
        super().__init__()
        self.x_schema = x_schema
        self.y_schema = y_schema

        self.set_dataset = set_dataset
        self.plot = None

        def setup_axis(schema, location):
            path = schema["path"]
            if not path:
                path = "/"
            identity_string = schema["param"]["fqn"] + "@" + path
            return setup_axis_item(
                self.getAxis(location), schema["param"]["description"], identity_string,
                schema["param"]["spec"])

        self.x_unit_suffix, self.x_data_to_display_scale = \
            setup_axis(x_schema, "bottom")
        self.y_unit_suffix, self.y_data_to_display_scale = \
            setup_axis(y_schema, "left")

        self.crosshair = LabeledCrosshairCursor(
            self, self, self.x_unit_suffix, self.x_data_to_display_scale,
            self.y_unit_suffix, self.y_data_to_display_scale)
        self.showGrid(x=True, y=True)

    def data_changed(self, datasets, mods):
        def d(name):
            return datasets.get("ndscan." + name, (False, None))[1]

        if not self.plot:
            channels_json = d("channels")
            if not channels_json:
                return

            channels = json.loads(channels_json)

            try:
                data_names, _ = extract_scalar_channels(channels)
            except ValueError as e:
                self.error.emit(str(e))

            if not data_names:
                self.error.emit("No scalar result channels to display")

            self._install_context_menu(data_names)

            bounds = lambda s: (s.get("min", None), s.get("max", None), s.get("increment", None))
            image_item = pyqtgraph.ImageItem()
            self.addItem(image_item)
            self.plot = _ImagePlot(image_item, data_names[0], *bounds(self.x_schema),
                                   *bounds(self.y_schema))

        self.plot.data_changed(datasets)

    def _install_context_menu(self, data_names):
        entries = []

        x_datasets = extract_linked_datasets(self.x_schema["param"])
        y_datasets = extract_linked_datasets(self.y_schema["param"])
        for d, axis in chain(
                zip(x_datasets, repeat("x")), zip(y_datasets, repeat("y"))):
            action = QtWidgets.QAction("Set '{}' from crosshair".format(d), self)
            action.triggered.connect(
                lambda *a, d=d: self._set_dataset_from_crosshair(d, axis))
            entries.append(action)
        if len(x_datasets) == 1 and len(y_datasets) == 1:
            action = QtWidgets.QAction("Set both from crosshair".format(d), self)

            def set_both():
                self._set_dataset_from_crosshair(x_datasets[0], "x")
                self._set_dataset_from_crosshair(y_datasets[0], "y")

            action.triggered.connect(set_both)
            entries.append(action)

        def append_separator():
            separator = QtWidgets.QAction("", self)
            separator.setSeparator(True)
            entries.append(separator)

        if entries:
            append_separator()

        self.channel_menu_group = QtWidgets.QActionGroup(self)
        first_action = None
        for name in data_names:
            action = QtWidgets.QAction(name, self)
            if not first_action:
                first_action = action
            action.setCheckable(True)
            action.setActionGroup(self.channel_menu_group)
            action.triggered.connect(
                lambda *a, name=name: self.plot.activate_channel(name))
            entries.append(action)
        if first_action:
            first_action.setChecked(True)
        append_separator()

        self.plotItem.getContextMenus = lambda ev: entries

    def _set_dataset_from_crosshair(self, dataset, axis):
        if not self.crosshair:
            logger.warning("Plot not initialised yet, ignoring set dataset request")
            return
        self.set_dataset(
            dataset, self.crosshair.last_x if axis == "x" else self.crosshair.last_y)

"""Pseudocolor 2D plot for equidistant data."""

from itertools import chain, repeat
import logging
import numpy as np
import pyqtgraph
from quamash import QtWidgets, QtCore
from typing import Dict, Union

from . import colormaps
from .cursor import LabeledCrosshairCursor
from .model import DimensionalScanModel
from .utils import (extract_linked_datasets, extract_scalar_channels, setup_axis_item)

logger = logging.getLogger(__name__)


def _calc_range_spec(preset_min, preset_max, preset_increment, data):
    sorted_data = np.unique(data)

    lower = preset_min if preset_min else sorted_data[0]
    upper = preset_max if preset_max else sorted_data[-1]

    if preset_increment:
        increment = preset_increment
    elif len(sorted_data) > 1:
        increment = np.min(sorted_data[1:] - sorted_data[:-1])
    else:
        # Only one point on this (i.e. all data so far is from one row/column), and no
        # way to infer what the increment is going to be. To be able to still display
        # the data as it comes in, fall back on an arbitrary increment so far.
        #
        # If we have lower/upper limits, we can at least try to guess a reasonable order
        # of magnitude.
        if lower != upper:
            increment = (upper - lower) / 32
        else:
            increment = 1.0

    return lower, upper, increment


def _num_points_in_range(range_spec):
    min, max, increment = range_spec
    return int(np.rint((max - min) / increment + 1))


def _coords_to_indices(coords, range_spec):
    min, max, increment = range_spec
    return np.rint((np.array(coords) - min) / increment).astype(int)


class _ImagePlot:
    def __init__(self, image_item: pyqtgraph.ImageItem, active_channel_name: str,
                 x_min: Union[float, None], x_max: Union[float, None],
                 x_increment: Union[float, None], y_min: Union[float, None],
                 y_max: Union[float, None], y_increment: Union[float, None],
                 hints_for_channels: Dict[str, dict]):
        self.image_item = image_item
        self.active_channel_name = active_channel_name
        self.hints_for_channels = hints_for_channels

        self.x_min = x_min
        self.x_max = x_max
        self.x_increment = x_increment

        self.y_min = y_min
        self.y_max = y_max
        self.y_increment = y_increment

        self.num_shown = 0
        self.current_z_limits = None
        self.x_range = None
        self.y_range = None
        self.image_data = None

    def activate_channel(self, channel_name: str):
        self.active_channel_name = channel_name
        self._invalidate_current()
        self._update()

    def data_changed(self, points, invalidate_previous: bool = False):
        self.points = points
        if invalidate_previous:
            self._invalidate_current()
        self._update()

    def _invalidate_current(self):
        self.num_shown = 0
        self.current_z_limits = None

    def _get_display_hints(self):
        return self.hints_for_channels[self.active_channel_name]

    def _update(self):
        x_data = self.points["axis_0"]
        y_data = self.points["axis_1"]
        z_data = self.points["channel_" + self.active_channel_name]

        # Figure out how many complete data points we have, and whether there are any
        # not already shown.

        num_to_show = min(len(x_data), len(y_data), len(z_data))

        if num_to_show == self.num_shown:
            return
        num_skip = self.num_shown
        self.num_shown = num_to_show

        # Update z autorange if active.
        if True:  # TODO: Provide manual override.
            data_min = np.min(z_data[num_skip:num_to_show])
            data_max = np.max(z_data[num_skip:num_to_show])
            if self.current_z_limits is None:
                self.current_z_limits = (data_min, data_max)
                num_skip = 0
            else:
                z_limits = (min(self.current_z_limits[0], data_min),
                            max(self.current_z_limits[1], data_max))
                if z_limits != self.current_z_limits:
                    self.current_z_limits = z_limits
                    num_skip = 0

        # Determine range of x/y values to show and prepare image buffer accordingly if
        # it changed.
        x_range = _calc_range_spec(self.x_min, self.x_max, self.x_increment, x_data)
        y_range = _calc_range_spec(self.y_min, self.y_max, self.y_increment, y_data)

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

        z_min, z_max = self.current_z_limits
        z_scaled = (z_data[num_skip:num_to_show] - z_min) / (z_max - z_min)

        cmap = colormaps.plasma
        if self._get_display_hints().get("coordinate_type", "") == "cyclic":
            cmap = colormaps.kovesi_c8
        self.image_data[x_inds, y_inds, :] = cmap.map(z_scaled)

        self.image_item.setImage(self.image_data, autoLevels=False)
        if num_skip == 0:
            # Image size has changed, set plot item size accordingly.
            self.image_item.setRect(self.image_rect)


class Image2DPlotWidget(pyqtgraph.PlotWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: DimensionalScanModel):
        super().__init__()

        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.points_appended.connect(lambda p: self._update_points(p, False))
        self.model.points_rewritten.connect(lambda p: self._update_points(p, True))

        self.x_schema, self.y_schema = self.model.axes
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
            setup_axis(self.x_schema, "bottom")
        self.y_unit_suffix, self.y_data_to_display_scale = \
            setup_axis(self.y_schema, "left")

        self.crosshair = LabeledCrosshairCursor(
            self, self, self.x_unit_suffix, self.x_data_to_display_scale,
            self.y_unit_suffix, self.y_data_to_display_scale)
        self.showGrid(x=True, y=True)

    def _initialise_series(self, channels):
        if self.plot is not None:
            self.removeItem(self.plot.image_item)
            self.plot = None

        try:
            data_names, _ = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))

        if not data_names:
            self.error.emit("No scalar result channels to display")

        hints_for_channels = {
            name: channels[name].get("display_hints", {})
            for name in data_names
        }
        self._install_context_menu(data_names)

        def bounds(schema):
            return (schema.get(n, None) for n in ("min", "max", "increment"))

        image_item = pyqtgraph.ImageItem()
        self.addItem(image_item)
        self.plot = _ImagePlot(image_item, data_names[0], *bounds(self.x_schema),
                               *bounds(self.y_schema), hints_for_channels)
        self.ready.emit()

    def _update_points(self, points, invalidate):
        if self.plot:
            self.plot.data_changed(points, invalidate_previous=invalidate)

    def _install_context_menu(self, data_names):
        entries = []

        if self.model.context.is_online_master():
            x_datasets = extract_linked_datasets(self.x_schema["param"])
            y_datasets = extract_linked_datasets(self.y_schema["param"])
            for d, axis in chain(
                    zip(x_datasets, repeat("x")), zip(y_datasets, repeat("y"))):
                action = QtWidgets.QAction("Set '{}' from crosshair".format(d), self)
                action.triggered.connect(lambda *a, d=d: self.
                                         _set_dataset_from_crosshair(d, axis))
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
            action.triggered.connect(lambda *a, name=name: self.plot.activate_channel(
                name))
            entries.append(action)
        if first_action:
            first_action.setChecked(True)
        append_separator()

        self.plotItem.getContextMenus = lambda ev: entries

    def _set_dataset_from_crosshair(self, dataset, axis):
        if not self.crosshair:
            logger.warning("Plot not initialised yet, ignoring set dataset request")
            return
        self.model.context.set_dataset(
            dataset, self.crosshair.last_x if axis == "x" else self.crosshair.last_y)

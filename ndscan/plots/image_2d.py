"""Pseudocolor 2D plot for equidistant data."""

from itertools import chain, repeat
import logging
import numpy as np
import pyqtgraph

from .._qt import QtCore, QtGui
from . import colormaps
from .cursor import CrosshairAxisLabel, CrosshairLabel, LabeledCrosshairCursor
from .model import ScanModel
from .plot_widgets import AlternateMenuPanesWidget, add_source_id_label
from .utils import (extract_linked_datasets, extract_scalar_channels,
                    format_param_identity, get_axis_scaling_info, setup_axis_item)

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
        # the data as it comes in, fall back on an arbitrary increment for now.
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


class CrosshairZDataLabel(CrosshairLabel):
    """Crosshair label for the z value of a 2D image

    :param x_range: A tuple of `(min, max, increment)` for the x axis of the image.
    :param y_range: A tuple of `(min, max, increment)` for the y axis of the image.

    All other arguments are forwarded to ``CrosshairLabel.__init__()``.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.x_range = None
        self.y_range = None
        self.image_data = None
        self.z_limits = None

    def set_crosshair_info(self, unit_suffix: str, data_to_display_scale: float,
                           _color):
        """Update the unit/scale information of the underlying data.

        :param unit_suffix: The unit (including a leading space).
        :param data_to_display_scale: The scaling factor corresponding to the unit.
        :param _color: This parameter has no effect.
        """
        self.unit_suffix = unit_suffix
        self.data_to_display_scale = data_to_display_scale

    def set_image_data(
        self,
        image_data: np.ndarray,
        x_range: tuple[float, float, float],
        y_range: tuple[float, float, float],
        z_limits: tuple[float, float],
    ):
        """Update the underlying image data object and the data limits.

        :param image_data: 2D numpy array containing the data that is displayed.
        :param z_limits: The current colormap limits.
        """
        self.image_data = image_data
        self.x_range = x_range
        self.y_range = y_range
        self.z_limits = z_limits

    def update_coords(self, data_coords):
        if self.image_data is None:
            return
        z = np.nan

        x_idx = _coords_to_indices([data_coords.x()], self.x_range)[0]
        y_idx = _coords_to_indices([data_coords.y()], self.y_range)[0]
        shape = self.image_data.shape
        if (0 <= x_idx < shape[0]) and (0 <= y_idx < shape[1]):
            z = self.image_data[x_idx, y_idx]
        if np.isnan(z):
            self.setText("NaN")
        else:
            self.set_value(z, self.z_limits)


class _ImagePlot:
    def __init__(self, image_item: pyqtgraph.ImageItem,
                 colorbar: pyqtgraph.ColorBarItem, active_channel_name: str,
                 x_min: float | None, x_max: float | None, x_increment: float | None,
                 y_min: float | None, y_max: float | None, y_increment: float | None,
                 channels: dict[str, dict]):
        self.image_item = image_item
        self.colorbar = colorbar
        self.channels = channels

        self.x_min = x_min
        self.x_max = x_max
        self.x_increment = x_increment

        self.y_min = y_min
        self.y_max = y_max
        self.y_increment = y_increment

        self.points = None
        self.num_shown = 0
        self.current_z_limits = None
        self.x_range = None
        self.y_range = None
        self.image_data = None

        self.z_crosshair_item = CrosshairZDataLabel(self.image_item.getViewBox())

        self.activate_channel(active_channel_name)

    def activate_channel(self, channel_name: str):
        self.active_channel_name = channel_name

        channel = self.channels[channel_name]
        label = channel["description"]
        if not label:
            label = channel["path"].split("/")[-1]
        crosshair_info = setup_axis_item(self.colorbar.getAxis("right"),
                                         [(label, channel["path"], None, channel)])
        # Update crosshair label.
        self.z_crosshair_item.set_crosshair_info(*crosshair_info[0])

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

    def _active_fixed_z_limits(self) -> tuple[float, float] | None:
        channel = self.channels[self.active_channel_name]
        if channel.get("min") is None:
            return None
        if channel.get("max") is None:
            return None
        return channel["min"], channel["max"]

    def _update(self):
        if not self.points:
            return

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
        z_limits = self._active_fixed_z_limits()
        if z_limits is None:  # TODO: Provide manual override.
            data_min = np.min(z_data[num_skip:num_to_show])
            data_max = np.max(z_data[num_skip:num_to_show])
            if self.current_z_limits is None:
                z_limits = (data_min, data_max)
            else:
                z_limits = (min(self.current_z_limits[0],
                                data_min), max(self.current_z_limits[1], data_max))
        self.current_z_limits = z_limits
        self.colorbar.setLevels(z_limits)

        # Determine range of x/y values to show and prepare image buffer accordingly if
        # it changed.
        x_range = _calc_range_spec(self.x_min, self.x_max, self.x_increment, x_data)
        y_range = _calc_range_spec(self.y_min, self.y_max, self.y_increment, y_data)

        if x_range != self.x_range or y_range != self.y_range:
            self.x_range = x_range
            self.y_range = y_range

            # TODO: Splat old data for progressively less blurry look on refining scans?
            self.image_data = np.full(
                (_num_points_in_range(x_range), _num_points_in_range(y_range)), np.nan)

            self.image_rect = QtCore.QRectF(
                QtCore.QPointF(x_range[0] - x_range[2] / 2,
                               y_range[0] - y_range[2] / 2),
                QtCore.QPointF(x_range[1] + x_range[2] / 2,
                               y_range[1] + y_range[2] / 2))

            num_skip = 0

        x_inds = _coords_to_indices(x_data[num_skip:num_to_show], self.x_range)
        y_inds = _coords_to_indices(y_data[num_skip:num_to_show], self.y_range)
        for x, y, z in zip(x_inds, y_inds, z_data[num_skip:num_to_show]):
            self.image_data[x, y] = z

        cmap = colormaps.plasma
        channel = self.channels[self.active_channel_name]
        display_hints = channel.get("display_hints", {})
        if display_hints.get("coordinate_type", "") == "cyclic":
            cmap = colormaps.kovesi_c8
        self.colorbar.setColorMap(cmap)

        self.image_item.setImage(self.image_data, autoLevels=False)
        self.z_crosshair_item.set_image_data(self.image_data, self.x_range,
                                             self.y_range, self.current_z_limits)
        if num_skip == 0:
            # Image size has changed, set plot item size accordingly.
            self.image_item.setRect(self.image_rect)


class Image2DPlotWidget(AlternateMenuPanesWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: ScanModel, get_alternate_plot_names):
        super().__init__(get_alternate_plot_names)

        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.points_appended.connect(lambda p: self._update_points(p, False))
        self.model.points_rewritten.connect(lambda p: self._update_points(p, True))

        self.data_names = []

        self.x_schema, self.y_schema = self.model.axes

        self.plot_item = self.add_pane()
        self.plot_item.showGrid(x=True, y=True)
        self.plot = None
        self.crosshair = None

    def _initialise_series(self, channels):
        if self.plot is not None:
            self.plot_item.removeItem(self.plot.image_item)
            self.plot = None

        try:
            self.data_names, _ = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))

        if not self.data_names:
            self.error.emit("No scalar result channels to display")

        def setup_axis(schema, location):
            param = schema["param"]
            setup_axis_item(self.plot_item.getAxis(location),
                            [(param["description"], format_param_identity(schema), None,
                              param["spec"])])

        setup_axis(self.x_schema, "bottom")
        setup_axis(self.y_schema, "left")

        def bounds(schema):
            return (schema.get(n, None) for n in ("min", "max", "increment"))

        image_item = pyqtgraph.ImageItem()
        self.plot_item.addItem(image_item)
        colorbar = self.plot_item.addColorBar(image_item, width=15.0, interactive=False)
        self.plot = _ImagePlot(image_item, colorbar, self.data_names[0],
                               *bounds(self.x_schema), *bounds(self.y_schema), channels)

        x_scaling_info = get_axis_scaling_info(self.x_schema["param"]["spec"])
        y_scaling_info = get_axis_scaling_info(self.y_schema["param"]["spec"])

        x_crosshair_item = CrosshairAxisLabel(self.plot_item.getViewBox(),
                                              *x_scaling_info,
                                              is_x=True)
        y_crosshair_item = CrosshairAxisLabel(self.plot_item.getViewBox(),
                                              *y_scaling_info,
                                              is_x=False)

        self.crosshair = LabeledCrosshairCursor(
            self, self.plot_item,
            [x_crosshair_item, y_crosshair_item, self.plot.z_crosshair_item])

        add_source_id_label(self.plot_item.getViewBox(), self.model.context)

        self.ready.emit()

    def _update_points(self, points, invalidate):
        if self.plot:
            self.plot.data_changed(points, invalidate_previous=invalidate)

    def build_context_menu(self, pane_idx: int, builder):
        if self.model.context.is_online_master():
            x_datasets = extract_linked_datasets(self.x_schema["param"])
            y_datasets = extract_linked_datasets(self.y_schema["param"])
            for d, axis_idx in chain(zip(x_datasets, repeat(0)),
                                     zip(y_datasets, repeat(1))):
                action = builder.append_action(f"Set '{d}' from crosshair")
                action.triggered.connect(lambda *a, axis_idx=axis_idx, d=d: (
                    self._set_dataset_from_crosshair(d, axis_idx)))
            if len(x_datasets) == 1 and len(y_datasets) == 1:
                action = builder.append_action("Set both from crosshair")

                def set_both():
                    self._set_dataset_from_crosshair(x_datasets[0], 0)
                    self._set_dataset_from_crosshair(y_datasets[0], 1)

                action.triggered.connect(set_both)
        builder.ensure_separator()

        self.channel_menu_group = QtGui.QActionGroup(self)
        for name in self.data_names:
            action = builder.append_action(name)
            action.setCheckable(True)
            action.setActionGroup(self.channel_menu_group)
            action.setChecked(name == self.plot.active_channel_name)
            action.triggered.connect(
                lambda *a, name=name: self.plot.activate_channel(name))
        builder.ensure_separator()

        super().build_context_menu(pane_idx, builder)

    def _set_dataset_from_crosshair(self, dataset, axis_idx):
        if not self.plot:
            logger.warning("Plot not initialised yet, ignoring set dataset request")
            return
        self.model.context.set_dataset(
            dataset, self.crosshair.crosshair_items[axis_idx].last_value)

import logging
import numpy as np
import pyqtgraph
from qasync import QtCore

from .annotation_items import ComputedCurveItem, CurveItem, VLineItem
from .cursor import LabeledCrosshairCursor
from .model import ScanModel
from .model.select_point import SelectPointFromScanModel
from .model.subscan import create_subscan_roots
from .plot_widgets import VerticalPlotStackWidget
from .utils import (extract_linked_datasets, extract_scalar_channels,
                    format_param_identity, group_channels_into_axes, setup_axis_item,
                    FIT_COLORS, SERIES_COLORS)

logger = logging.getLogger(__name__)


class _XYSeries(QtCore.QObject):
    def __init__(self, view_box, data_name, data_item, error_bar_name, error_bar_item,
                 plot_left_to_right):
        super().__init__(view_box)

        self.view_box = view_box
        self.data_item = data_item
        self.data_name = data_name
        self.error_bar_item = error_bar_item
        self.error_bar_name = error_bar_name
        self.plot_left_to_right = plot_left_to_right
        self.num_current_points = 0

    def update(self, x_data, data):
        def channel(name):
            return np.array(data.get("channel_" + name, []))

        x_data = np.array(x_data)
        y_data = channel(self.data_name)
        num_to_show = min(len(x_data), len(y_data))

        if self.error_bar_item:
            y_err = channel(self.error_bar_name)
            num_to_show = min(num_to_show, len(y_err))

        if num_to_show == self.num_current_points:
            return

        if self.plot_left_to_right:
            order = np.argsort(x_data[:num_to_show])

            self.data_item.setData(x_data[order], y_data[order])
            if self.num_current_points == 0:
                self.view_box.addItem(self.data_item)

            if self.error_bar_item:
                self.error_bar_item.setData(x=x_data[order],
                                            y=y_data[order],
                                            height=y_err[order])
                if self.num_current_points == 0:
                    self.view_box.addItem(self.error_bar_item)
        else:
            self.data_item.setData(x_data[:num_to_show], y_data[:num_to_show])
            if self.num_current_points == 0:
                self.view_box.addItem(self.data_item)

            if self.error_bar_item:
                self.error_bar_item.setData(x=x_data[:num_to_show],
                                            y=y_data[:num_to_show],
                                            height=(2 * y_err[:num_to_show]))
                if self.num_current_points == 0:
                    self.view_box.addItem(self.error_bar_item)

        self.num_current_points = num_to_show

    def remove_items(self):
        if self.num_current_points == 0:
            return
        self.view_box.removeItem(self.data_item)
        if self.error_bar_item:
            self.view_box.removeItem(self.error_bar_item)
        self.num_current_points = 0


class XY1DPlotWidget(VerticalPlotStackWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: ScanModel, get_alternate_plot_names):
        super().__init__()
        
        self.get_alternate_plot_names = get_alternate_plot_names
        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.points_appended.connect(self._update_points)
        self.model.annotations_changed.connect(self._update_annotations)

        # FIXME: Just re-set values instead of throwing away everything.
        def rewritten(points):
            self._initialise_series(self.model.get_channel_schemata())
            self._update_points(points)

        self.model.points_rewritten.connect(rewritten)

        self.selected_point_model = SelectPointFromScanModel(self.model)

        self.annotation_items = []
        self.series = []

        self.x_schema = self.model.axes[0]
        self.x_param_spec = self.x_schema["param"]["spec"]

        self.y_unit_suffix = None
        self.y_data_to_display_scale = None
        self._highlighted_spot = None

    def _initialise_series(self, channels):
        # Remove all currently shown items and any extra axes added.
        for s in self.series:
            s.remove_items()
        self.series.clear()
        self._clear_annotations()
        self.clear()

        try:
            data_names, error_bar_names = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))
            return

        series_idx = 0
        axes = group_channels_into_axes(channels, data_names)
        for names in axes:
            axis, view_box = self.new_subplot()

            info = []
            for name in names:
                color = SERIES_COLORS[series_idx % len(SERIES_COLORS)]
                data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color, size=6)
                data_item.sigClicked.connect(self._point_clicked)

                error_bar_item = None
                error_bar_name = error_bar_names.get(name, None)
                if error_bar_name:
                    error_bar_item = pyqtgraph.ErrorBarItem(pen=color)

                self.series.append(
                    _XYSeries(view_box, name, data_item, error_bar_name, error_bar_item,
                              False))

                channel = channels[name]
                label = channel["description"]
                if not label:
                    label = channel["path"].split("/")[-1]
                info.append((label, channel["path"], color, channel))

                series_idx += 1

            suffix, scale = setup_axis_item(axis, info)
            if self.y_unit_suffix is None:
                # FIXME: Add multiple lines to the crosshair.
                self.y_unit_suffix = suffix
                self.y_data_to_display_scale = scale

        self.link_x_axes()
        self.x_unit_suffix, self.x_data_to_display_scale = setup_axis_item(
            self.subplots[-1].getAxis("bottom"),
            [(self.x_schema["param"]["description"], format_param_identity(
                self.x_schema), None, self.x_param_spec)])

        self.add_crosshair(self.x_unit_suffix, self.x_data_to_display_scale,
                           self.y_unit_suffix, self.y_data_to_display_scale)
        self.subscan_roots = create_subscan_roots(self.selected_point_model)

        # Make sure we put back annotations (if they haven't changed but the points
        # have been rewritten, there might not be an annotations_changed event).
        self._update_annotations()

        self.ready.emit()

    def _update_points(self, points):
        x_data = points["axis_0"]
        # Compare length to zero instead of using `not x_data` for NumPy array
        # compatibility.
        if len(x_data) == 0:
            return

        for s in self.series:
            s.update(x_data, points)

    def _clear_annotations(self):
        for item in self.annotation_items:
            item.remove()
        self.annotation_items.clear()

    def _update_annotations(self):
        self._clear_annotations()

        def channel_ref_to_series_idx(ref):
            for i, s in enumerate(self.series):
                if "channel_" + s.data_name == ref:
                    return i
            return 0

        def make_curve_item(series_idx):
            color = FIT_COLORS[series_idx % len(FIT_COLORS)]
            pen = pyqtgraph.mkPen(color, width=3)
            return pyqtgraph.PlotCurveItem(pen=pen)

        annotations = self.model.get_annotations()
        for a in annotations:
            if a.kind == "location":
                if set(a.coordinates.keys()) == set(["axis_0"]):
                    associated_series_idx = max(
                        channel_ref_to_series_idx(chan)
                        for chan in a.parameters.get("associated_channels", [None]))

                    color = FIT_COLORS[associated_series_idx % len(FIT_COLORS)]
                    vb = self.series[associated_series_idx].view_box
                    line = VLineItem(a.coordinates["axis_0"],
                                     a.data.get("axis_0_error", None), vb, color,
                                     self.x_data_to_display_scale, self.x_unit_suffix)
                    self.annotation_items.append(line)
                    continue

            if a.kind == "curve":
                associated_series_idx = None
                for series_idx, series in enumerate(self.series):
                    match_coords = set(["axis_0", "channel_" + series.data_name])
                    if set(a.coordinates.keys()) == match_coords:
                        associated_series_idx = series_idx
                        break
                if associated_series_idx is not None:
                    curve = make_curve_item(associated_series_idx)
                    series = self.series[associated_series_idx]
                    vb = series.view_box
                    item = CurveItem(a.coordinates["axis_0"],
                                     a.coordinates["channel_" + series.data_name], vb,
                                     curve)
                    self.annotation_items.append(item)
                    continue

            if a.kind == "computed_curve":
                function_name = a.parameters.get("function_name", None)
                if ComputedCurveItem.is_function_supported(function_name):
                    associated_series_idx = max(
                        channel_ref_to_series_idx(chan)
                        for chan in a.parameters.get("associated_channels", [None]))

                    x_limits = [self.x_param_spec.get(n, None) for n in ("min", "max")]
                    curve = make_curve_item(associated_series_idx)
                    vb = self.series[associated_series_idx].view_box
                    item = ComputedCurveItem(function_name, a.data, vb, curve, x_limits)
                    self.annotation_items.append(item)
                    continue

            logger.info("Ignoring annotation of kind '%s' with coordinates %s", a.kind,
                        list(a.coordinates.keys()))

    def build_context_menu(self, builder):
        x_schema = self.model.axes[0]

        if self.model.context.is_online_master():
            for d in extract_linked_datasets(x_schema["param"]):
                action = builder.append_action("Set '{}' from crosshair".format(d))
                action.triggered.connect(lambda: self._set_dataset_from_crosshair_x(d))

        builder.ensure_separator()
        super().build_context_menu(builder)

    def _set_dataset_from_crosshair_x(self, dataset_key):
        if not self.crosshair:
            logger.warning("Plot not initialised yet, ignoring set dataset request")
            return
        self.model.context.set_dataset(dataset_key, self.crosshair.last_x)

    def _highlight_spot(self, spot):
        if self._highlighted_spot is not None:
            self._highlighted_spot.resetPen()
            self._highlighted_spot = None
        if spot is not None:
            spot.setPen("y", width=2)
            self._highlighted_spot = spot

    def _point_clicked(self, scatter_plot_item, spot_items):
        if not spot_items:
            # No points clicked – events don't seem to emitted in this case anyway.
            self._background_clicked()
            return

        # Arbitrarily choose the first element in the list if multiple spots
        # overlap; the user can always zoom in if that is undesired.
        spot = spot_items[0]
        self._highlight_spot(spot)
        self.selected_point_model.set_source_index(spot.index())

    def _background_clicked(self):
        self._highlight_spot(None)
        self.selected_point_model.set_source_index(None)

    def _handle_scene_click(self, event):
        if not event.isAccepted():
            # Event not handled yet, so background/… was clicked instead of a point.
            self._background_clicked()
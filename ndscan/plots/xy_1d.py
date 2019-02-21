import logging
import numpy as np
import pyqtgraph
from quamash import QtCore

from .annotation_items import ComputedCurveItem, CurveItem, VLineItem
from .cursor import LabeledCrosshairCursor
from .model import ScanModel
from .utils import (extract_linked_datasets, extract_scalar_channels, setup_axis_item,
                    AlternateMenuPlotWidget, FIT_COLORS, SERIES_COLORS)

logger = logging.getLogger(__name__)


class _XYSeries(QtCore.QObject):
    def __init__(self, plot, data_name, data_item, error_bar_name, error_bar_item,
                 plot_left_to_right):
        super().__init__(plot)

        self.plot = plot
        self.data_item = data_item
        self.data_name = data_name
        self.error_bar_item = error_bar_item
        self.error_bar_name = error_bar_name
        self.plot_left_to_right = plot_left_to_right
        self.num_current_points = 0

    def update(self, x_data, data):
        def channel(name):
            return data.get("channel_" + name, [])

        y_data = channel(self.data_name)
        num_to_show = min(len(x_data), len(y_data))

        if self.error_bar_item:
            y_err = channel(self.error_bar_name)
            num_to_show = min(num_to_show, len(y_err))

        if num_to_show == self.num_current_points:
            return

        if self.plot_left_to_right:
            x_data = np.array(x_data)
            order = np.argsort(x_data[:num_to_show])

            y_data = np.array(y_data)
            self.data_item.setData(x_data[order], y_data[order])
            if self.num_current_points == 0:
                self.plot.addItem(self.data_item)

            if self.error_bar_item:
                y_err = np.array(y_err)
                self.error_bar_item.setData(
                    x=x_data[order], y=y_data[order], height=y_err[order])
                if self.num_current_points == 0:
                    self.plot.addItem(self.error_bar_item)
        else:
            self.data_item.setData(x_data[:num_to_show], y_data[:num_to_show])
            if self.num_current_points == 0:
                self.plot.addItem(self.data_item)

            if self.error_bar_item:
                self.error_bar_item.setData(
                    x=x_data[:num_to_show],
                    y=y_data[:num_to_show],
                    height=(2 * np.array(y_err[:num_to_show])))
                if self.num_current_points == 0:
                    self.plot.addItem(self.error_bar_item)

        self.num_current_points = num_to_show

    def remove_items(self):
        if self.num_current_points == 0:
            return
        self.plot.removeItem(self.data_item)
        if self.error_bar_item:
            self.plot.removeItem(self.error_bar_item)
        self.num_current_points = 0


class XY1DPlotWidget(AlternateMenuPlotWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: ScanModel, get_alternate_plot_names):
        super().__init__(get_alternate_plot_names)

        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.points_appended.connect(self._update_points)
        self.model.annotations_changed.connect(self._update_annotations)

        self.annotation_items = []

        # FIXME: Just re-set values instead of throwing away everything.
        def rewritten(points):
            self._initialise_series(self.model.get_channel_schemata())
            self._update_points(points)

        self.model.points_rewritten.connect(rewritten)

        self.series = []

        x_schema = self.model.axes[0]
        path = x_schema["path"]
        if not path:
            path = "/"
        identity_string = x_schema["param"]["fqn"] + "@" + path
        self.x_unit_suffix, self.x_data_to_display_scale = setup_axis_item(
            self.getAxis("bottom"), [(x_schema["param"]["description"], identity_string,
                                      None, x_schema["param"]["spec"])])
        self.crosshair = None
        self.showGrid(x=True, y=True)

    def _initialise_series(self, channels):
        for s in self.series:
            s.remove_items()
        self.series.clear()

        try:
            data_names, error_bar_names = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))
            return

        colors = [SERIES_COLORS[i % len(SERIES_COLORS)] for i in range(len(data_names))]
        for i, (name, color) in enumerate(zip(data_names, colors)):
            data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color, size=5)

            error_bar_name = error_bar_names.get(name, None)
            error_bar_item = pyqtgraph.ErrorBarItem(
                pen=color) if error_bar_name else None

            self.series.append(
                _XYSeries(self, name, data_item, error_bar_name, error_bar_item, False))

        # If there is only one series, set unit/scale accordingly.
        # TODO: Add multiple y axes for additional channels.
        def axis_info(i):
            c = channels[data_names[i]]
            label = c["description"]
            if not label:
                label = c["path"].split("/")[-1]
            return label, c["path"], colors[i], c

        self.y_unit_suffix, self.y_data_to_display_scale = setup_axis_item(
            self.getAxis("left"), [axis_info(i) for i in range(len(data_names))])

        if self.crosshair is None:
            # FIXME: Reinitialise crosshair as necessary on schema changes.
            self.crosshair = LabeledCrosshairCursor(
                self, self.getPlotItem(), self.x_unit_suffix,
                self.x_data_to_display_scale, self.y_unit_suffix,
                self.y_data_to_display_scale)
        self.ready.emit()

    def _update_points(self, points):
        x_data = points["axis_0"]
        # Compare length to zero instead of using `not x_data` for NumPy array
        # compatibility.
        if len(x_data) == 0:
            return

        for s in self.series:
            s.update(x_data, points)

    def _update_annotations(self):
        for item in self.annotation_items:
            item.remove()
        self.annotation_items.clear()

        def series_idx(data_name):
            for i, s in enumerate(self.series):
                if s.data_name == data_name:
                    return i
            return 0

        annotations = self.model.get_annotations()
        for a in annotations:
            if a.kind == "location":
                if set(a.coordinates.keys()) == set(["axis_0"]):
                    color = FIT_COLORS[series_idx(
                        a.parameters.get("associated_channel")) % len(FIT_COLORS)]
                    line = VLineItem(a.coordinates["axis_0"],
                                     a.data.get("axis_0_error",
                                                None), self.getPlotItem(), color,
                                     self.x_data_to_display_scale, self.x_unit_suffix)
                    self.annotation_items.append(line)
                    continue

            if a.kind == "curve":
                series = None
                for series_idx, s in enumerate(self.series):
                    match_coords = set(["axis_0", "channel_" + s.data_name])
                    if set(a.coordinates.keys()) == match_coords:
                        series = s
                        break
                if series is not None:
                    color = FIT_COLORS[series_idx % len(FIT_COLORS)]
                    pen = pyqtgraph.mkPen(color, width=3)
                    curve = pyqtgraph.PlotCurveItem(pen=pen)

                    item = CurveItem(a.coordinates["axis_0"],
                                     a.coordinates["channel_" + s.data_name],
                                     self.getPlotItem(), curve)
                    self.annotation_items.append(item)
                    continue

            if a.kind == "computed_curve":
                function_name = a.parameters.get("function_name", None)
                if ComputedCurveItem.is_function_supported(function_name):
                    idx = series_idx(a.parameters.get("associated_channel"))
                    color = FIT_COLORS[idx % len(FIT_COLORS)]
                    pen = pyqtgraph.mkPen(color, width=3)
                    curve = pyqtgraph.PlotCurveItem(pen=pen)
                    item = ComputedCurveItem(function_name, a.data, self.getPlotItem(),
                                             curve)
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

import logging
import numpy as np
from oitg import uncertainty_to_string
import pyqtgraph
from quamash import QtCore

from .cursor import LabeledCrosshairCursor
from .model import ScanModel
from .utils import (extract_linked_datasets, extract_scalar_channels, setup_axis_item,
                    AlternateMenuPlotWidget, SERIES_COLORS)

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


class _VLineFitPOI:
    def __init__(self, fit_param_name, base_color, x_data_to_display_scale,
                 x_unit_suffix):
        self.fit_param_name = fit_param_name
        self.x_data_to_display_scale = x_data_to_display_scale
        self.x_unit_suffix = x_unit_suffix

        self.left_line = pyqtgraph.InfiniteLine(
            movable=False,
            angle=90,
            pen={
                "color": base_color,
                "style": QtCore.Qt.DotLine
            })
        self.center_line = pyqtgraph.InfiniteLine(
            movable=False,
            angle=90,
            label="",
            labelOpts={
                "position": 0.97,
                "color": base_color,
                "movable": True
            },
            pen={
                "color": base_color,
                "style": QtCore.Qt.SolidLine
            })
        self.right_line = pyqtgraph.InfiniteLine(
            movable=False,
            angle=90,
            pen={
                "color": base_color,
                "style": QtCore.Qt.DotLine
            })

        self.has_warned = False

    def add_to_plot(self, plot):
        plot.addItem(self.left_line, ignoreBounds=True)
        plot.addItem(self.center_line, ignoreBounds=True)
        plot.addItem(self.right_line, ignoreBounds=True)

    def update(self, fit_minimizers, fit_minimizer_errors):
        try:
            x = fit_minimizers[self.fit_param_name]
            delta_x = fit_minimizer_errors[self.fit_param_name]
        except KeyError as e:
            if not self.has_warned:
                logger.warn(
                    "Unknown reference to fit parameter '%s' in point of interest",
                    str(e))
                self.has_warned = True
            # TODO: Remove POI.
            return

        if np.isnan(delta_x) or delta_x == 0.0:
            # If the covariance extraction failed, just don't display the
            # confidence interval at all.
            delta_x = 0.0
            label = str(x)
        else:
            label = uncertainty_to_string(x * self.x_data_to_display_scale,
                                          delta_x * self.x_data_to_display_scale)
        self.center_line.label.setFormat(label + self.x_unit_suffix)

        self.left_line.setPos(x - delta_x)
        self.center_line.setPos(x)
        self.right_line.setPos(x + delta_x)


class XY1DPlotWidget(AlternateMenuPlotWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: ScanModel, get_alternate_plot_names):
        super().__init__(get_alternate_plot_names)

        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.points_appended.connect(self._update_points)

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

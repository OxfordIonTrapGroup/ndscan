import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import logging
import numpy as np
from oitg import uncertainty_to_string
import pyqtgraph
from quamash import QtWidgets, QtCore

from ndscan.auto_fit import FIT_OBJECTS
from .cursor import LabeledCrosshairCursor
from .utils import (extract_linked_datasets, extract_scalar_channels, setup_axis_item,
                    FIT_COLORS, SERIES_COLORS)

logger = logging.getLogger(__name__)


class _XYSeries(QtCore.QObject):
    def __init__(self,
                 plot,
                 data_name,
                 data_item,
                 error_bar_name,
                 error_bar_item,
                 plot_left_to_right,
                 fit_spec=None,
                 fit_item=None,
                 fit_pois=[]):
        super().__init__(plot)

        self.plot = plot
        self.data_item = data_item
        self.data_name = data_name
        self.error_bar_item = error_bar_item
        self.error_bar_name = error_bar_name
        self.plot_left_to_right = plot_left_to_right
        self.num_current_points = 0
        self.fit_obj = None

        if fit_spec:
            self._install_fit(fit_spec, fit_item, fit_pois)

    def update(self, x_data, data):
        def channel(name):
            return data.get("ndscan.points.channel_" + name, (False, []))[1]

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

        if self.fit_obj and self.num_current_points >= len(
                self.fit_obj.parameter_names):
            self._trigger_recompute_fit.emit()

    _trigger_recompute_fit = QtCore.pyqtSignal()

    def _install_fit(self, spec, item, pois):
        self.fit_type = spec["fit_type"]
        self.fit_obj = FIT_OBJECTS[self.fit_type]
        self.fit_item = item
        self.fit_pois = pois
        self.fit_item_added = False

        self.last_fit_params = None

        self.recompute_fit_limiter = pyqtgraph.SignalProxy(
            self._trigger_recompute_fit,
            slot=lambda: asyncio.ensure_future(self._recompute_fit()),
            rateLimit=30)
        self.recompute_in_progress = False
        self.fit_executor = ProcessPoolExecutor(max_workers=1)

        self.redraw_fit_limiter = pyqtgraph.SignalProxy(
            self.plot.getPlotItem().getViewBox().sigXRangeChanged,
            slot=self._redraw_fit,
            rateLimit=30)

    async def _recompute_fit(self):
        if self.recompute_in_progress:
            # Run at most one fit computation at a time. To make sure we don't
            # leave a few final data points completely disregarded, just
            # re-emit the signal – even for long fits, repeated checks aren't
            # expensive, as long as the SignalProxy rate is slow enough.
            self._trigger_recompute_fit.emit()
            return

        self.recompute_in_progress = True

        xs, ys = self.data_item.getData()
        y_errs = None
        if self.error_bar_item:
            y_errs = self.error_bar_item.opts['height'] / 2

        loop = asyncio.get_event_loop()
        self.last_fit_params, self.last_fit_errors = await loop.run_in_executor(
            self.fit_executor, _run_fit, self.fit_type, xs, ys, y_errs)
        self.redraw_fit_limiter.signalReceived()

        self.recompute_in_progress = False

    def _redraw_fit(self, *args):
        if not self.last_fit_params:
            return

        if not self.fit_item_added:
            self.plot.addItem(self.fit_item, ignoreBounds=True)
            for f in self.fit_pois:
                f.add_to_plot(self.plot)
            self.fit_item_added = True

        # Choose horizontal range based on currently visible area.
        view_box = self.plot.getPlotItem().getViewBox()
        x_range, _ = view_box.state["viewRange"]
        ext = (x_range[1] - x_range[0]) / 10
        x_lims = (x_range[0] - ext, x_range[1] + ext)

        # Choose number of points based on width of plot on screen (in pixels).
        fit_xs = np.linspace(*x_lims, view_box.width())

        fit_ys = self.fit_obj.fitting_function(fit_xs, self.last_fit_params)

        self.fit_item.setData(fit_xs, fit_ys)

        for f in self.fit_pois:
            f.update(self.last_fit_params, self.last_fit_errors)


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


def _run_fit(fit_type, xs, ys, y_errs=None):
    """Fits the given data with the chosen method.

    This function is intended to be executed on a worker process, hence the
    primitive API.
    """
    try:
        return FIT_OBJECTS[fit_type].fit(xs, ys, y_errs)
    except Exception:
        return None, None


class XY1DPlotWidget(pyqtgraph.PlotWidget):
    error = QtCore.pyqtSignal(str)

    def __init__(self, x_schema, set_dataset):
        super().__init__()

        self.set_dataset = set_dataset

        self.series_initialised = False
        self.series = []

        path = x_schema["path"]
        if not path:
            path = "/"
        identity_string = x_schema["param"]["fqn"] + "@" + path
        self.x_unit_suffix, self.x_data_to_display_scale = setup_axis_item(
            self.getAxis("bottom"), x_schema["param"]["description"], identity_string,
            x_schema["param"]["spec"])

        self._install_context_menu(x_schema)
        self.crosshair = None
        self.showGrid(x=True, y=True)

    def data_changed(self, data, mods):
        def d(name):
            return data.get("ndscan." + name, (False, None))[1]

        if not self.series_initialised:
            channels_json = d("channels")
            if not channels_json:
                return

            channels = json.loads(channels_json)

            try:
                data_names, error_bar_names = extract_scalar_channels(channels)
            except ValueError as e:
                self.error.emit(str(e))

            # KLUDGE: We rely on fit specs to be set before channels in order
            # for them to be displayed at all.
            fit_specs = json.loads(d("auto_fit") or "[]")

            for i, name in enumerate(data_names):
                color = SERIES_COLORS[i % len(SERIES_COLORS)]
                data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color, size=5)

                error_bar_name = error_bar_names.get(name, None)
                error_bar_item = pyqtgraph.ErrorBarItem(
                    pen=color) if error_bar_name else None

                # TODO: Multiple fit specs, error bars from other channels.
                fit_spec = None
                fit_item = None
                fit_pois = []
                for spec in fit_specs:
                    if spec["data"]["x"] != "axis_0":
                        continue
                    if spec["data"]["y"] != "channel_" + name:
                        continue
                    err = spec["data"].get("y_err", None)
                    if err and err != ("channel_" + error_bar_name):
                        continue

                    fit_spec = spec
                    fit_color = FIT_COLORS[i % len(FIT_COLORS)]
                    pen = pyqtgraph.mkPen(fit_color, width=3)
                    fit_item = pyqtgraph.PlotCurveItem(pen=pen)

                    for p in spec.get("pois", []):
                        # TODO: Support horizontal lines, points, ...
                        if p.get("x", None):
                            fit_pois.append(
                                _VLineFitPOI(p["x"], fit_color,
                                             self.x_data_to_display_scale,
                                             self.x_unit_suffix))
                    break

                self.series.append(
                    _XYSeries(self, name, data_item, error_bar_name, error_bar_item,
                              False, fit_spec, fit_item, fit_pois))

            if len(data_names) == 1:
                # If there is only one series, set label/scaling accordingly.
                # TODO: Add multiple y axis for additional channels.
                c = channels[data_names[0]]

                label = c["description"]
                if not label:
                    label = c["path"].split("/")[-1]

                # TODO: Change result channel schema and move properties accessed here
                # into "spec" field to match parameters?
                self.y_unit_suffix, self.y_data_to_display_scale = setup_axis_item(
                    self.getAxis("left"), label, c["path"], c)
            else:
                self.y_unit_suffix = ""
                self.y_data_to_display_scale = 1.0

            self.crosshair = LabeledCrosshairCursor(
                self, self, self.x_unit_suffix, self.x_data_to_display_scale,
                self.y_unit_suffix, self.y_data_to_display_scale)
            self.series_initialised = True

        x_data = d("points.axis_0")
        if not x_data:
            return

        for s in self.series:
            s.update(x_data, data)

    def _install_context_menu(self, x_schema):
        entries = []

        for d in extract_linked_datasets(x_schema["param"]):
            action = QtWidgets.QAction("Set '{}' from crosshair".format(d), self)
            action.triggered.connect(lambda: self._set_dataset_from_crosshair_x(d))
            entries.append(action)

        if entries:
            separator = QtWidgets.QAction("", self)
            separator.setSeparator(True)
            entries.append(separator)

        self.plotItem.getContextMenus = lambda ev: entries

    def _set_dataset_from_crosshair_x(self, dataset):
        if not self.crosshair:
            logger.warning("Plot not initialised yet, ignoring set dataset request")
            return
        self.set_dataset(dataset, self.crosshair.last_x)

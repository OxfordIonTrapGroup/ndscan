import asyncio
import json
import logging
import pyqtgraph
import numpy as np

from artiq.applets.simple import SimpleApplet
from artiq.protocols.pc_rpc import AsyncioClient
from artiq.protocols.sync_struct import Subscriber
from concurrent.futures import ProcessPoolExecutor
from oitg import uncertainty_to_string
from quamash import QtWidgets, QtCore
from .auto_fit import FIT_OBJECTS
from .utils import eval_param_default

logger = logging.getLogger(__name__)

# Colours to use for data series (RGBA) and associated fit curves.
SERIES_COLORS = [
    "#d9d9d999", "#fdb46299", "#80b1d399", "#fb807299", "#bebeada99", "#ffffb399"
]
FIT_COLORS = [
    "#ff333399", "#fdb462dd", "#80b1d3dd", "#fb8072dd", "#bebeadadd", "#ffffb3dd"
]


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
    except Exception as e:
        return None, None


class _XYPlotWidget(pyqtgraph.PlotWidget):
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
        self.x_unit_suffix, self.x_data_to_display_scale = _setup_axis_item(
            self.getAxis("bottom"), x_schema["param"]["description"], identity_string,
            x_schema["param"]["spec"])

        self.showGrid(x=True, y=True)

        # Crosshair cursor with coordinate display. The TextItems for displaying
        # the coordinates are updated on a timer to avoid a lag trail of buffered
        # redraws when there are a lot of points.
        #
        # TODO: Abstract out, use for other plots as well.
        self.getPlotItem().getViewBox().hoverEvent = self._on_viewbox_hover
        self.setCursor(QtCore.Qt.CrossCursor)
        self.crosshair_timer = QtCore.QTimer(self)
        self.crosshair_timer.timeout.connect(self._update_crosshair_text)
        self.crosshair_timer.setSingleShot(True)
        self.crosshair_x_text = None
        self.crosshair_y_text = None

        self._install_context_menu(x_schema)

    def _on_viewbox_hover(self, event):
        if event.isExit():
            self.removeItem(self.crosshair_x_text)
            self.crosshair_x_text = None
            self.removeItem(self.crosshair_y_text)
            self.crosshair_y_text = None

            self.crosshair_timer.stop()
            return

        self.last_hover_event = event
        self.crosshair_timer.start(0)

    def _update_crosshair_text(self):
        vb = self.getPlotItem().getViewBox()
        data_coords = vb.mapSceneToView(self.last_hover_event.scenePos())

        # TODO: Draw text directly to graphics scene rather than going through
        # pyqtgraph for performance - don't need any of the fancy interaction
        # or layouting features that come with being a plot item.

        def make_text():
            text = pyqtgraph.TextItem()
            # Don't take text item into account for auto-scaling; otherwise
            # there will be positive feedback if the cursor is towards the
            # bottom right of the screen.
            text.setFlag(text.ItemHasNoContents)
            self.addItem(text)
            return text

        if not self.crosshair_x_text:
            self.crosshair_x_text = make_text()

        if not self.crosshair_y_text:
            self.crosshair_y_text = make_text()

        x_range, y_range = vb.state["viewRange"]
        x_range = np.array(x_range) * self.x_data_to_display_scale
        y_range = np.array(y_range) * self.y_data_to_display_scale

        def num_digits_after_point(r):
            # We want to be able to resolve at least 1000 points in the displayed
            # range.
            smallest_digit = np.floor(np.log10(r[1] - r[0])) - 3
            return int(-smallest_digit) if smallest_digit < 0 else 0

        self.crosshair_x_text.setText("{0:.{width}f}{1}".format(
            data_coords.x() * self.x_data_to_display_scale,
            self.x_unit_suffix,
            width=num_digits_after_point(x_range)))
        self.crosshair_x_text.setPos(data_coords)

        self.last_crosshair_x = data_coords.x()

        y_text_pos = QtCore.QPointF(self.last_hover_event.scenePos())
        y_text_pos.setY(self.last_hover_event.scenePos().y() + 10)
        self.crosshair_y_text.setText("{0:.{width}f}{1}".format(
            data_coords.y() * self.y_data_to_display_scale,
            self.y_unit_suffix,
            width=num_digits_after_point(y_range)))
        self.crosshair_y_text.setPos(vb.mapSceneToView(y_text_pos))

    def data_changed(self, data, mods):
        def d(name):
            return data.get("ndscan." + name, (False, None))[1]

        if not self.series_initialised:
            channels_json = d("channels")
            if not channels_json:
                return

            channels = json.loads(channels_json)

            try:
                data_names, error_bar_names = _extract_scalar_channels(channels)
            except ValueError as e:
                self.emit.error(str(e))

            sorted_data_names = list(data_names)
            sorted_data_names.sort(key=lambda n: channels[n]["path"])

            # KLUDGE: We rely on fit specs to be set before channels in order
            # for them to be displayed at all.
            fit_specs = json.loads(d("auto_fit") or "[]")

            for i, name in enumerate(sorted_data_names):
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
                    e = spec["data"].get("y_err", None)
                    if e and e != ("channel_" + error_bar_name):
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

            if len(sorted_data_names) == 1:
                # If there is only one series, set label/scaling accordingly.
                # TODO: Add multiple y axis for additional channels.
                c = channels[sorted_data_names[0]]

                label = c["description"]
                if not label:
                    label = c["path"].split("/")[-1]
                self.y_unit_suffix, self.y_data_to_display_scale = _setup_axis_item(
                    self.getAxis("left"),
                    label,
                    c["path"],
                    c  # TODO: Change result channel schema and move this into "spec" field?
                )
            else:
                self.y_unit_suffix = ""
                self.y_data_to_display_scale = 1.0

            self.series_initialised = True

        x_data = d("points.axis_0")
        if not x_data:
            return

        for s in self.series:
            s.update(x_data, data)

    def _install_context_menu(self, x_schema):
        entries = []

        for d in _extract_linked_datasets(x_schema["param"]):
            action = QtWidgets.QAction("Set '{}' from crosshair".format(d), self)
            action.triggered.connect(lambda: self._set_dataset_from_crosshair_x(d))
            entries.append(action)

        if not entries:
            return

        separator = QtWidgets.QAction("", self)
        separator.setSeparator(True)
        entries.append(separator)
        self.plotItem.getContextMenus = lambda ev: entries + [self.getMenu()]

    def _set_dataset_from_crosshair_x(self, dataset):
        self.set_dataset(dataset, self.last_crosshair_x)


def _setup_axis_item(axis_item, description, identity_string, spec):
    unit_suffix = ""
    unit = spec.get("unit", "")
    if unit:
        unit_suffix = " " + unit
        unit = "/ " + unit + " "

    label = "<b>{} {}</b>".format(description, unit)
    if identity_string:
        label += "<i>({})</i>".format(identity_string)
    axis_item.setLabel(label)

    data_to_display_scale = 1 / spec["scale"]
    axis_item.setScale(data_to_display_scale)
    axis_item.autoSIPrefix = False

    return unit_suffix, data_to_display_scale


def _extract_linked_datasets(param_schema):
    datasets = []
    try:

        def log_datasets(dataset, default):
            datasets.append(dataset)
            return default

        eval_param_default(param_schema["default"], log_datasets)
    except Exception as e:
        # Ignore default parsing errors here; the user will get warnings from the
        # experiment dock and on the core device anyway.
        pass
    return datasets


def _extract_scalar_channels(channels):
    data_names = set(
        name for name, spec in channels.items() if spec["type"] in ["int", "float"])

    # Build map from "primary" channel names to error bar names.
    error_bar_names = {}
    for name in data_names:
        spec = channels[name]
        display_hints = spec.get("display_hints", {})
        eb = display_hints.get("error_bar_for", "")
        if eb:
            if eb in error_bar_names:
                raise ValueError(
                    "More than one set of error bars specified for channel '{}'".format(
                        eb))
            error_bar_names[eb] = name

    data_names -= set(error_bar_names.values())

    return data_names, error_bar_names


class _Rolling1DSeries:
    def __init__(self, plot, data_name, data_item, error_bar_name, error_bar_item,
                 history_length):
        self.plot = plot
        self.data_item = data_item
        self.data_name = data_name
        self.error_bar_item = error_bar_item
        self.error_bar_name = error_bar_name

        self.values = np.array([]).reshape((0, 2))
        self.set_history_length(history_length)

    def append(self, data):
        new_data = data["ndscan.point." + self.data_name][1]
        if self.error_bar_item:
            new_error_bar = data["ndscan.point." + self.error_bar_name][1]

        p = [new_data, 2 * new_error_bar] if self.error_bar_item else [new_data]

        is_first = (self.values.shape[0] == 0)
        if is_first:
            self.values = np.array([p])
        else:
            if self.values.shape[0] == len(self.x_indices):
                self.values = np.roll(self.values, -1, axis=0)
                self.values[-1, :] = p
            else:
                self.values = np.vstack((self.values, p))

        num_to_show = self.values.shape[0]
        self.data_item.setData(self.x_indices[-num_to_show:], self.values[:, 0].T)
        if self.error_bar_item:
            self.error_bar_item.setData(
                x=self.x_indices[-num_to_show:],
                y=self.values[:, 0].T,
                height=self.values[:, 1].T)

        if is_first:
            self.plot.addItem(self.data_item)
            if self.error_bar_item:
                self.plot.addItem(self.error_bar_item)

    def set_history_length(self, n):
        assert n > 0, "Invalid history length"
        self.x_indices = np.arange(-n, 0)
        if self.values.shape[0] > n:
            self.values = self.values[-n:, :]


class _RollingPlotWidget(pyqtgraph.PlotWidget):
    error = QtCore.pyqtSignal(str)

    def __init__(self):
        super().__init__()

        self.series_initialised = False
        self.series = []

        self.point_phase = False

        self.showGrid(x=True, y=True)

        self._install_context_menu()

    def data_changed(self, data, mods):
        def d(name):
            return data.get("ndscan." + name, (False, None))[1]

        if not self.series_initialised:
            channels_json = d("channels")
            if not channels_json:
                return

            channels = json.loads(channels_json)

            try:
                data_names, error_bar_names = _extract_scalar_channels(channels)
            except ValueError as e:
                self.emit.error(str(e))

            sorted_data_names = list(data_names)
            sorted_data_names.sort(key=lambda n: channels[n]["path"])

            for i, data_name in enumerate(sorted_data_names):
                color = SERIES_COLORS[i % len(SERIES_COLORS)]
                data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color)

                error_bar_name = error_bar_names.get(data_name, None)
                error_bar_item = pyqtgraph.ErrorBarItem(
                    pen=color) if error_bar_name else None

                self.series.append(
                    _Rolling1DSeries(self, data_name, data_item, error_bar_name,
                                     error_bar_item, self.num_history_box.value()))

            if len(sorted_data_names) == 1:
                # If there is only one series, set label/scaling accordingly.
                # TODO: Add multiple y axis for additional channels.
                c = channels[sorted_data_names[0]]

                label = c["description"]
                if not label:
                    label = c["path"].split("/")[-1]
                _setup_axis_item(
                    self.getAxis("left"),
                    label,
                    c["path"],
                    c  # TODO: Change result channel schema and move this into "spec" field?
                )

            self.series_initialised = True

        # FIXME: Phase check will miss points when using mod buffering - need
        # to check mods for more than one change.
        phase = d("point_phase")
        if phase is not None and phase != self.point_phase:
            for s in self.series:
                s.append(data)
            self.point_phase = phase

    def set_history_length(self, n):
        for s in self.series:
            s.set_history_length(n)

    def _install_context_menu(self):
        self.num_history_box = QtWidgets.QSpinBox()
        self.num_history_box.setMinimum(1)
        self.num_history_box.setMaximum(2**16)
        self.num_history_box.setValue(100)
        self.num_history_box.valueChanged.connect(self.set_history_length)

        container = QtWidgets.QWidget()

        layout = QtWidgets.QHBoxLayout()
        container.setLayout(layout)

        label = QtWidgets.QLabel("N: ")
        layout.addWidget(label)

        layout.addWidget(self.num_history_box)

        action = QtWidgets.QWidgetAction(self)
        action.setDefaultWidget(container)

        separator = QtWidgets.QAction("", self)
        separator.setSeparator(True)
        entries = [action, separator]
        self.plotItem.getContextMenus = lambda ev: entries + [self.getMenu()]


class _MainWidget(QtWidgets.QWidget):
    def __init__(self, args):
        super().__init__()
        self.args = args

        self.setWindowTitle("ndscan plot")
        self.resize(800, 500)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        self.widget_stack = QtWidgets.QStackedWidget()
        self.message_label = QtWidgets.QLabel(
            "Waiting for ndscan metadata for rid {}…".format(self.args.rid))
        self.widget_stack.addWidget(self.message_label)
        self.layout.addWidget(self.widget_stack)

        self.title_set = False
        self.plot_initialised = False

    def data_changed(self, data, mods):
        def d(name):
            return data.get("ndscan." + name, (False, None))[1]

        if not self.title_set:
            fqn = d("fragment_fqn")
            if not fqn:
                return
            self.setWindowTitle("{} – ndscan".format(fqn))
            self.title_set = True

        if not self.plot_initialised:
            axes_json = d("axes")
            if not axes_json:
                return
            axes = json.loads(axes_json)
            if len(axes) == 0:
                self.plot = _RollingPlotWidget()
            elif len(axes) == 1:
                self.plot = _XYPlotWidget(axes[0], self.set_dataset)
            else:
                self.message_label.setText(
                    "{}-dimensional scans are not yet supported".format(len(axes)))
                self._show(self.message_label)
                return

            self.plot.error.connect(self._show_error)
            self.widget_stack.addWidget(self.plot)
            self._show(self.plot)

            self.plot_initialised = True

        self.plot.data_changed(data, mods)

    def _show(self, widget):
        self.widget_stack.setCurrentIndex(self.widget_stack.indexOf(widget))

    def _show_error(self, message):
        self.message_label.setText("Error: " + message)
        self._show(self.message_label)

    def set_dataset(self, key, value):
        asyncio.ensure_future(self._set_dataset_impl(key, value))

    async def _set_dataset_impl(self, key, value):
        logger.info("Setting '%s' to %s", key, value)
        try:
            remote = AsyncioClient()
            await remote.connect_rpc(self.args.server, self.args.port_control,
                                     "master_dataset_db")
            try:
                await remote.set(key, value, persist=True)
            finally:
                remote.close_rpc()
        except:
            logger.error("Failed to set dataset '%s'", key, exc_info=True)


class NdscanApplet(SimpleApplet):
    def __init__(self):
        # Use a small update delay by default to avoid lagging out the UI by
        # continuous redraws for plots with a large number of points. (20 ms
        # is a pretty arbitrary choice for a latency not perceptible by the
        # user in a normal use case).
        super().__init__(_MainWidget, default_update_delay=20e-3)

        self.argparser.add_argument(
            "--port-control",
            default=3251,
            type=int,
            help="TCP port for master control commands")
        self.argparser.add_argument("--rid", help="RID of the experiment to plot")

    def subscribe(self):
        # We want to subscribe only to the experiment-local datasets for our RID
        # (but always, even if using IPC – this can be optimised later).
        self.subscriber = Subscriber("datasets_rid_{}".format(self.args.rid),
                                     self.sub_init, self.sub_mod)
        self.loop.run_until_complete(
            self.subscriber.connect(self.args.server, self.args.port))

        # Make sure we still respond to non-dataset messages like `terminate` in
        # embed mode.
        if self.embed is not None:

            def ignore(*args):
                pass

            self.ipc.subscribe([], ignore, ignore)

    def unsubscribe(self):
        self.loop.run_until_complete(self.subscriber.close())

    def filter_mod(self, *args):
        return True


def main():
    pyqtgraph.setConfigOptions(antialias=True)

    applet = NdscanApplet()
    applet.run()


if __name__ == "__main__":
    main()

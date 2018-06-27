import json
import logging
import pyqtgraph
import numpy as np

from artiq.applets.simple import SimpleApplet
from artiq.protocols.sync_struct import Subscriber
from quamash import QtWidgets, QtCore

logger = logging.getLogger(__name__)


class _XYSeries:
    def __init__(self, plot, y_name, data_item, y_err_name, error_bar_item, plot_left_to_right):
        self.plot = plot
        self.data_item = data_item
        self.y_name = y_name
        self.error_bar_item = error_bar_item
        self.y_err_name = y_err_name
        self.plot_left_to_right = plot_left_to_right
        self.num_current_points = 0

    def update(self, x_data, data):
        def channel(name):
            return data.get("ndscan.points.channel_" + name, (False, []))[1]

        y_data = channel(self.y_name)
        num_to_show = min(len(x_data), len(y_data))

        if self.error_bar_item:
            y_err = channel(self.y_err_name)
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
                self.error_bar_item.setData(x=x_data[order], y=y_data[order], height=y_err[order])
                if self.num_current_points == 0:
                    self.plot.addItem(self.error_bar_item)
        else:
            self.data_item.setData(x_data[:num_to_show], y_data[:num_to_show])
            if self.num_current_points == 0:
                self.plot.addItem(self.data_item)

            if self.error_bar_item:
                self.error_bar_item.setData(x=x_data[:num_to_show], y=y_data[:num_to_show],
                    height=(2 * np.array(y_err[:num_to_show])))
                if self.num_current_points == 0:
                    self.plot.addItem(self.error_bar_item)

        self.num_current_points = num_to_show

class _XYPlotWidget(pyqtgraph.PlotWidget):
    error = QtCore.pyqtSignal(str)

    def __init__(self, x_schema):
        super().__init__()

        self.series_initialised = False
        self.series = []

        path = x_schema["path"]
        if not path:
            path = "/"
        param = x_schema["param"]["fqn"] + "@" + path

        description = x_schema["param"]["description"]
        label = "{} ({})".format(description, param) if description else param
        self.setLabel("bottom", label, x_schema["param"].get("units", ""))

        self.showGrid(x=True, y=True)

    def data_changed(self, data, mods):
        def d(name):
            return data.get("ndscan." + name, (False, None))[1]

        if not self.series_initialised:
            channels_json = d("channels")
            if not channels_json:
                return

            channels = json.loads(channels_json)

            data_names = set(name for name, spec in channels.items() if spec["type"] in ["int", "float"])

            # Build map from "primary" channel names to error bar names.
            error_bar_names = {}
            for name in data_names:
                spec = channels[name]
                display_hints = spec.get("display_hints", {})
                eb = display_hints.get("error_bar_for", "")
                if eb:
                    if eb in error_bar_names:
                        self.error.emit("More than one set of error bars specified for channel '{}'".format(eb))
                        return
                    error_bar_names[eb] = name

            data_names -= set(error_bar_names.values())

            colors = ["#d9d9d9aa", "#fdb462aa", "#80b1d3aa", "#fb8072aa", "#bebeadaaa", "#ffffb3aa"]
            for i, name in enumerate(data_names):
                color = colors[i % len(colors)]
                data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color)

                error_bar_name = error_bar_names.get(name, None)
                error_bar_item = None
                if error_bar_name:
                    error_bar_item = pyqtgraph.ErrorBarItem(pen=color)

                self.series.append(_XYSeries(self, name, data_item, error_bar_name, error_bar_item, False))

            self.series_initialised = True

        x_data = d("points.axis_0")
        if not x_data:
            return

        for s in self.series:
            s.update(x_data, data)


class _MainWidget(QtWidgets.QWidget):
    def __init__(self, args):
        super().__init__()
        self.args = args

        self.setWindowTitle("ndscan plot")

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
                # Show rolling plot.
                pass
            elif len(axes) == 1:
                # Show 1D plot.
                self.plot = _XYPlotWidget(axes[0])
                self.plot.error.connect(self._show_error)
                self.widget_stack.addWidget(self.plot)
                self._show(self.plot)
            else:
                self.message_label.setText(
                    "{}-dimensional scans are not yet supported".format(len(axes)))
                self._show(self.message_label)
            self.plot_initialised = True

        self.plot.data_changed(data, mods)

    def _show(self, widget):
        self.widget_stack.setCurrentIndex(self.widget_stack.indexOf(widget))

    def _show_error(self, message):
        self.message_label.setText("Error: " + message)
        self._show(self.message_label)


class NdscanApplet(SimpleApplet):
    def __init__(self):
        super().__init__(_MainWidget)
        self.argparser.add_argument("--rid", help="RID of the experiment to plot")

    def subscribe(self):
        # We want to subscribe only to the experiment-local datasets for our RID
        # (but always, even if using IPC – this can be optimised later).
        self.subscriber = Subscriber("datasets_rid_{}".format(self.args.rid),
                                     self.sub_init, self.sub_mod)
        self.loop.run_until_complete(self.subscriber.connect(
            self.args.server, self.args.port))

    def filter_mod(self, *args):
        return True


def main():
    pyqtgraph.setConfigOptions(antialias=True)

    applet = NdscanApplet()
    applet.run()

if __name__ == "__main__":
    main()

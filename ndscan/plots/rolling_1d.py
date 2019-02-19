import numpy as np
import pyqtgraph
from quamash import QtWidgets, QtCore

from .model import ContinuousScanModel
from .utils import extract_scalar_channels, setup_axis_item, SERIES_COLORS


class _Series:
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
        new_data = data[self.data_name]
        if self.error_bar_item:
            new_error_bar = data[self.error_bar_name]

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

    def remove_items(self):
        if self.values.shape[0] == 0:
            return
        self.plot.removeItem(self.data_item)
        if self.error_bar_item:
            self.plot.removeItem(self.error_bar_item)

    def set_history_length(self, n):
        assert n > 0, "Invalid history length"
        self.x_indices = np.arange(-n, 0)
        if self.values.shape[0] > n:
            self.values = self.values[-n:, :]


class Rolling1DPlotWidget(pyqtgraph.PlotWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: ContinuousScanModel):
        super().__init__()

        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.new_point_complete.connect(self._append_point)

        self.series = []

        self.showGrid(x=True, y=True)

        self._install_context_menu()

    def _initialise_series(self):
        for s in self.series:
            s.remove_items()
        self.series.clear()

        channels = self.model.get_channel_schemata()
        try:
            data_names, error_bar_names = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))
            return

        for i, data_name in enumerate(data_names):
            color = SERIES_COLORS[i % len(SERIES_COLORS)]
            data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color)

            error_bar_name = error_bar_names.get(data_name, None)
            error_bar_item = pyqtgraph.ErrorBarItem(
                pen=color) if error_bar_name else None

            self.series.append(
                _Series(self, data_name, data_item, error_bar_name, error_bar_item,
                        self.num_history_box.value()))

        if len(data_names) == 1:
            # If there is only one series, set label/scaling accordingly.
            # TODO: Add multiple y axis for additional channels.
            c = channels[data_names[0]]

            label = c["description"]
            if not label:
                label = c["path"].split("/")[-1]
            setup_axis_item(self.getAxis("left"), label, c["path"], c)

        self.ready.emit()

    def _append_point(self, point):
        for s in self.series:
            s.append(point)

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
        self.plotItem.getContextMenus = lambda ev: entries

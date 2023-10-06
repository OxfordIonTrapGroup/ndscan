import numpy as np
import pyqtgraph

from .._qt import QtCore, QtWidgets
from .model import SinglePointModel
from .plot_widgets import add_source_id_label, AlternateMenuPanesWidget
from .utils import (extract_scalar_channels, group_channels_into_axes,
                    group_axes_into_panes, setup_axis_item, SERIES_COLORS)


class _Series:
    def __init__(self, view_box, data_name, data_item, error_bar_name, error_bar_item,
                 history_length):
        self.view_box = view_box
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
            self.error_bar_item.setData(x=self.x_indices[-num_to_show:],
                                        y=self.values[:, 0].T,
                                        height=self.values[:, 1].T)

        if is_first:
            self.view_box.addItem(self.data_item)
            if self.error_bar_item:
                self.view_box.addItem(self.error_bar_item)

    def remove_items(self):
        if self.values.shape[0] == 0:
            return
        self.view_box.removeItem(self.data_item)
        if self.error_bar_item:
            self.view_box.removeItem(self.error_bar_item)

    def set_history_length(self, n):
        assert n > 0, "Invalid history length"
        self.x_indices = np.arange(-n, 0)
        if self.values.shape[0] > n:
            self.values = self.values[-n:, :]


class Rolling1DPlotWidget(AlternateMenuPanesWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: SinglePointModel, get_alternate_plot_names):
        super().__init__(get_alternate_plot_names)

        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.point_changed.connect(self._append_point)

        self.series = []
        self._history_length = 1024

    def _initialise_series(self):
        for s in self.series:
            s.remove_items()
        self.series.clear()
        self.clear()

        channels = self.model.get_channel_schemata()
        try:
            data_names, error_bar_names = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))
            return

        axes = group_channels_into_axes(channels, data_names)
        plots_axes = group_axes_into_panes(channels, axes)
        for axes_names in plots_axes:
            pane = self.add_pane()
            pane.showGrid(x=True, y=True)
            series_idx = 0
            for names in axes_names:
                axis, view_box = pane.new_y_axis()

                info = []
                for name in names:
                    color = SERIES_COLORS[series_idx % len(SERIES_COLORS)]
                    data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color, size=6)

                    error_bar_item = None
                    error_bar_name = error_bar_names.get(name, None)
                    if error_bar_name:
                        error_bar_item = pyqtgraph.ErrorBarItem(pen=color)

                    self.series.append(
                        _Series(view_box, name, data_item, error_bar_name,
                                error_bar_item, self._history_length))

                    channel = channels[name]
                    label = channel["description"]
                    if not label:
                        label = channel["path"].split("/")[-1]
                    info.append((label, channel["path"], color, channel))

                    series_idx += 1

                setup_axis_item(axis, info)
        if len(self.panes) > 1:
            self.link_x_axes()
        if self.series:
            add_source_id_label(self.series[-1].view_box, self.model.context)

        self.ready.emit()

    def _append_point(self, point):
        for s in self.series:
            s.append(point)

    def set_history_length(self, n):
        self._history_length = n
        for s in self.series:
            s.set_history_length(n)

    def build_context_menu(self, pane_idx: int, builder):
        if self.model.context.is_online_master():
            # If no new data points are coming in, setting the history size wouldn't do
            # anything.
            # TODO: is_online_master() should really be something like
            # SinglePointModel.ever_updates().

            num_history_box = QtWidgets.QSpinBox()
            num_history_box.setMinimum(1)
            num_history_box.setMaximum(2**16)
            num_history_box.setValue(self._history_length)
            num_history_box.valueChanged.connect(self.set_history_length)

            container = QtWidgets.QWidget()

            layout = QtWidgets.QHBoxLayout()
            container.setLayout(layout)

            label = QtWidgets.QLabel("N: ")
            layout.addWidget(label)

            layout.addWidget(num_history_box)

            action = builder.append_widget_action()
            action.setDefaultWidget(container)
        builder.ensure_separator()
        super().build_context_menu(pane_idx, builder)

import logging
import numpy as np
import pyqtgraph
from collections import deque

from .._qt import QtCore, QtWidgets
from .model import SinglePointModel
from .model.subscan import create_subscan_roots
from .plot_widgets import (SubplotMenuPanesWidget, build_channel_selection_context_menu,
                           add_source_id_label)
from .utils import (extract_scalar_channels, get_default_hidden_channels,
                    group_channels_into_axes, group_axes_into_panes,
                    hide_series_from_groups, setup_axis_item, SERIES_COLORS)

logger = logging.getLogger(__name__)


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


class Rolling1DPlotWidget(SubplotMenuPanesWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: SinglePointModel):
        super().__init__()

        self.series = []
        self._history_length = 1024
        self._points = deque(maxlen=self._history_length)

        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.point_changed.connect(self._append_point)

        # List of all scalar channel names for the context menu.
        self.data_names = None
        # Set of channel names that are currently hidden.
        self.hidden_channels = None

    def _initialise_series(self):
        self.clear_panes()
        self.clear()
        for s in self.series:
            s.remove_items()
        self.series.clear()

        channels = self.model.get_channel_schemata()
        try:
            self.data_names, error_bar_names = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))
            return

        axes = group_channels_into_axes(channels, self.data_names)
        panes_axes = group_axes_into_panes(channels, axes)
        if self.hidden_channels is None:
            self.hidden_channels = get_default_hidden_channels(
                channels, self.data_names)
        panes_axes_shown = hide_series_from_groups(panes_axes, self.hidden_channels)

        for axes_series in panes_axes_shown:
            pane = self.add_pane()
            pane.showGrid(x=True, y=True)
            for series in axes_series:
                axis, view_box = pane.new_y_axis()

                info = []
                for (series_idx, name) in series:
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
                    info.append(
                        (label, channel["path"], channel["type"], color, channel))

                setup_axis_item(axis, info)

        if len(self.panes) > 1:
            self.link_x_axes()
            add_source_id_label(self.panes[-1].getViewBox(), self.model.context)

        self.subscan_roots = create_subscan_roots(self.model)

        self.ready.emit()

    def _append_point(self, point):
        self._points.append(point)
        for s in self.series:
            s.append(point)

    def _rewrite(self):
        self._initialise_series()
        for point in self._points:
            for s in self.series:
                s.append(point)

    def set_history_length(self, n):
        self._history_length = n
        for s in self.series:
            s.set_history_length(n)
        points = deque(maxlen=self._history_length)
        points.extend(self._points)
        self._points = points

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

        if len(self.data_names) > 1:
            build_channel_selection_context_menu(builder, self._rewrite,
                                                 self.data_names, self.hidden_channels)

        super().build_context_menu(pane_idx, builder)

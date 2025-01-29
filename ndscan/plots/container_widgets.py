import logging
import pyqtgraph.dockarea as pgda
from collections import OrderedDict

from .image_2d import Image2DPlotWidget
from .model import Context, Root, SinglePointModel, ScanModel
from .plot_widgets import VerticalPanesWidget
from .rolling_1d import Rolling1DPlotWidget
from .xy_1d import XY1DPlotWidget
from .._qt import QtCore, QtWidgets

logger = logging.getLogger(__name__)


def make_plot_for_dimensional_model(model: ScanModel) -> VerticalPanesWidget:
    dim = len(model.axes)
    if dim == 1:
        return XY1DPlotWidget(model)
    if dim == 2:
        return Image2DPlotWidget(model)
    raise NotImplementedError(
        f"Plots for {dim}-dimensional data are not yet implemented")


class PlotAreaWidget(pgda.DockArea):
    def __init__(self, root: Root, context: Context):
        super().__init__()

        self.root = root
        self.context = context
        self.context.title_changed.connect(self._set_window_title)

        self._root_widget = RootWidget(root)
        self._root_widget.new_dock_requested.connect(self._add_dock)

        self._root_dock = pgda.Dock("main plot")
        self._root_dock.hideTitleBar()
        self.addDock(self._root_dock)

        self._root_dock.addWidget(self._root_widget)

    def _add_dock(self, widget: VerticalPanesWidget, title: str):
        dock = pgda.Dock(title, autoOrientation=False, closable=True, widget=widget)
        widget.was_closed.connect(lambda: self._was_closed_cb(dock))
        dock.sigClosed.connect(widget.close)

        _, docks = self.findAll()
        if len(docks) > 1:
            self.addDock(dock, position="bottom", relativeTo=next(reversed(docks)))
        else:
            self.addDock(dock, position="right")

    def _was_closed_cb(self, dock):
        # If container() is not None, then the dock is still open (e.g. when the dock
        # was closed from a context menu). Close it in that case. If it is already None,
        # the dock was closed with the close button, so we don't need to do anything.
        if dock.container():
            dock.close()

    def _set_window_title(self, title):
        self.setWindowTitle(f"{title} – ndscan")


class RootWidget(QtWidgets.QWidget):
    """Displays the main plot for a given :class:`.Root` instance.

    Shows a message while the model is ``None`` (i.e. no point selected/data still
    loading), the plot after that.
    """

    new_dock_requested = QtCore.pyqtSignal(object, str)
    was_closed = QtCore.pyqtSignal()

    def __init__(self, root: Root):
        super().__init__()

        self.root = root
        self.root.model_changed.connect(self._change_model)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)

        self.setLayout(self.layout)

        # TODO: Use context info/… to identify plot to user in message.
        self.message_label = QtWidgets.QLabel("No data.")

        self.widget_stack = QtWidgets.QStackedWidget()
        self.widget_stack.addWidget(self.message_label)
        self.layout.addWidget(self.widget_stack)

        self.plot_widget: VerticalPanesWidget = None

        if self.root.get_model() is not None:
            self._change_model()

    def closeEvent(self, ev):
        if self.plot_widget is None:
            self.was_closed.emit()
        else:
            # This will also cause plot_widget to emit was_closed, which we then
            # forward.
            self.plot_widget.close()
        super().closeEvent(ev)

    def _change_model(self):
        if self.plot_widget:
            self._show_message("No data.")
            self.widget_stack.removeWidget(self.plot_widget)
            self.plot_widget = None
        model = self.root.get_model()
        if model is not None:
            self._show_message("Waiting for channel metadata for scan…")

            if isinstance(model, SinglePointModel):
                self.plot_widget = Rolling1DPlotWidget(model)
            else:
                try:
                    self.plot_widget = make_plot_for_dimensional_model(model)
                except NotImplementedError as err:
                    self._show_message("Error: " + str(err))
                    return
            self.widget_stack.addWidget(self.plot_widget)
            self.plot_widget.error.connect(self._show_message)
            self.plot_widget.ready.connect(lambda: self._show(self.plot_widget))
            self.plot_widget.new_dock_requested.connect(self.new_dock_requested)
            self.plot_widget.was_closed.connect(self.was_closed)

    def _show(self, widget):
        self.widget_stack.setCurrentIndex(self.widget_stack.indexOf(widget))

    def _show_message(self, message):
        self.message_label.setText(message)
        self._show(self.message_label)


class PlotAreaTabWidget(QtWidgets.QWidget):
    """Window with tabs for multiple plot roots."""
    def __init__(self, roots, context):
        super().__init__()

        self.plot_area_widgets = OrderedDict(
            (label, PlotAreaWidget(root, context)) for label, root in roots.items())

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        self.tab_widget = QtWidgets.QTabWidget()
        for label, widget in self.plot_area_widgets.items():
            self.tab_widget.addTab(widget, label)
        self.layout.addWidget(self.tab_widget)

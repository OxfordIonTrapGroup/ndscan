import logging
from collections import OrderedDict

from .model import Context, Model, Root, SinglePointModel, ScanModel
from .model.subscan import create_subscan_roots
from .image_2d import Image2DPlotWidget
from .rolling_1d import Rolling1DPlotWidget
from .xy_1d import XY1DPlotWidget
from .._qt import QtWidgets

logger = logging.getLogger(__name__)


def _make_dimensional_plot(model: ScanModel, get_alternate_names):
    dim = len(model.axes)
    if dim == 1:
        return XY1DPlotWidget(model, get_alternate_names)
    if dim == 2:
        return Image2DPlotWidget(model, get_alternate_names)
    raise NotImplementedError(
        "Plots for {}-dimensional data are not yet implemented".format(dim))


class RootWidget(QtWidgets.QWidget):
    """Top-level widget that displays the plot(s) for a given :class:`.Root` instance.

    Shows a message while the model is ``None`` (i.e. no point selected/data still
    loading), the plot after that.
    """
    def __init__(self, root: Root, context: Context):
        super().__init__()

        self.root = root
        self.root.model_changed.connect(self._update_plot)

        self.context = context
        self.context.title_changed.connect(self._set_window_title)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        # TODO: Use context info/… to identify plot to user in message.
        self.message_label = QtWidgets.QLabel("No data.")

        self.widget_stack = QtWidgets.QStackedWidget()
        self.widget_stack.addWidget(self.message_label)
        self.layout.addWidget(self.widget_stack)

        self.plot_container = None

    def _set_window_title(self, title):
        self.setWindowTitle("{} – ndscan".format(title))

    def _update_plot(self):
        if self.plot_container:
            self.widget_stack.setCurrentIndex(
                self.widget_stack.indexOf(self.message_label))
            self.widget_stack.removeWidget(self.plot_container)
            self.plot_container = None

        model = self.root.get_model()
        if model is not None:
            self.plot_container = PlotContainerWidget(model)
            self.widget_stack.addWidget(self.plot_container)
            self.widget_stack.setCurrentIndex(
                self.widget_stack.indexOf(self.plot_container))


class PlotContainerWidget(QtWidgets.QWidget):
    """Displays the plot for a given :class:`.Model`.

    For single point models, also registers any subscans as alternate plots to the
    top-level rolling plot.
    """
    def __init__(self, model: Model):
        super().__init__()

        self._alternate_plots = OrderedDict()

        self.model = model

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        self.widget_stack = QtWidgets.QStackedWidget()
        self.message_label = QtWidgets.QLabel("Waiting for channel metadata for scan…")
        self.widget_stack.addWidget(self.message_label)
        self.layout.addWidget(self.widget_stack)

        if isinstance(self.model, SinglePointModel):
            self.plot = Rolling1DPlotWidget(self.model, self._get_alternate_names)
            self.model.channel_schemata_changed.connect(self._create_subscan_roots)
        else:
            try:
                self.plot = _make_dimensional_plot(self.model,
                                                   self._get_alternate_names)
            except NotImplementedError as err:
                self._show_error(str(err))
                return
        self.widget_stack.addWidget(self.plot)
        self._alternate_plots["main plot"] = self.plot
        self.plot.error.connect(self._show_error)
        self.plot.ready.connect(lambda: self._show(self.plot))
        self.plot.alternate_plot_requested.connect(self._show_alternate_plot)

        # All subscan roots, to keep the objects alive.
        self._subscan_roots = None

    def _show(self, widget):
        self.widget_stack.setCurrentIndex(self.widget_stack.indexOf(widget))

    def _show_alternate_plot(self, name):
        self._show(self._alternate_plots[name])

    def _show_error(self, message):
        self.message_label.setText("Error: " + message)
        self._show(self.message_label)

    def _create_subscan_roots(self):
        # TODO: Think whether it makes sense to support this more than once.
        self.model.channel_schemata_changed.disconnect(self._create_subscan_roots)

        self._subscan_roots = create_subscan_roots(self.model)
        for name, root in self._subscan_roots.items():
            root.model_changed.connect(
                lambda model: self._set_subscan_plot(name, model))

    def _set_subscan_plot(self, name, model):
        old_plot = self._alternate_plots.get(name, None)
        if old_plot:
            self.widget_stack.removeWidget(old_plot)

        try:
            plot = _make_dimensional_plot(model, self._get_alternate_names)
        except NotImplementedError as err:
            logger.info("Ignoring subscan '%s': %s", name, str(err))
            return
        self._alternate_plots["subscan '{}'".format(name)] = plot
        self.widget_stack.addWidget(plot)
        plot.error.connect(self._show_error)
        plot.alternate_plot_requested.connect(self._show_alternate_plot)

        # TODO: Heuristics for choosing which one to display by default.
        self._show(plot)

    def _get_alternate_names(self):
        return list(self._alternate_plots.keys())


class MultiRootWidget(QtWidgets.QWidget):
    """Window with tabs for multiple plot roots."""
    def __init__(self, roots, context):
        super().__init__()

        self.root_widgets = OrderedDict(
            (label, RootWidget(root, context)) for label, root in roots.items())

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        self.tab_widget = QtWidgets.QTabWidget()
        for label, widget in self.root_widgets.items():
            self.tab_widget.addTab(widget, label)
        self.layout.addWidget(self.tab_widget)

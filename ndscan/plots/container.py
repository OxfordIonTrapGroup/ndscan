import logging
from collections import OrderedDict
from quamash import QtWidgets

from .model import Model, SinglePointModel, ScanModel
from .model_subscan import SubscanRoot
from .image_2d import Image2DPlotWidget
from .rolling_1d import Rolling1DPlotWidget
from .xy_1d import XY1DPlotWidget

logger = logging.getLogger(__name__)


def _make_dimensional_plot(model: ScanModel, get_alternate_names):
    dim = len(model.axes)
    if dim == 1:
        return XY1DPlotWidget(model, get_alternate_names)
    if dim == 2:
        return Image2DPlotWidget(model, get_alternate_names)
    raise NotImplementedError(
        "Plots for {}-dimensional data are not yet implemented".format(dim))


class PlotContainerWidget(QtWidgets.QWidget):
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

        for name, schema in self.model.get_channel_schemata().items():
            if schema["type"] != "subscan":
                continue
            root = SubscanRoot(self.model, name)
            root.model_changed.connect(lambda model: self._add_subscan_plot(
                name, model))

    def _add_subscan_plot(self, name, model):
        old_plot = self._alternate_plots.get(name, None)
        if old_plot:
            self.widget_stack.removeWidget(old_plot)

        try:
            plot = _make_dimensional_plot(model, self._get_alternate_names)
        except NotImplementedError as err:
            logger.info("Ignoring subscan '%s': %s", name, str(err))
        self._alternate_plots["subscan '{}'".format(name)] = plot
        self.widget_stack.addWidget(plot)
        plot.error.connect(self._show_error)
        plot.alternate_plot_requested.connect(self._show_alternate_plot)

    def _get_alternate_names(self):
        return list(self._alternate_plots.keys())

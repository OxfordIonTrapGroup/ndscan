from quamash import QtWidgets
from typing import Union

from .model import ContinuousScanModel, DimensionalScanModel
from .image_2d import Image2DPlotWidget
from .rolling_1d import Rolling1DPlotWidget
from .xy_1d import XY1DPlotWidget


class PlotContainerWidget(QtWidgets.QWidget):
    def __init__(self, model: Union[ContinuousScanModel, DimensionalScanModel]):
        super().__init__()

        self.model = model

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        self.widget_stack = QtWidgets.QStackedWidget()
        self.message_label = QtWidgets.QLabel("Waiting for channel metadata for scan…")
        self.widget_stack.addWidget(self.message_label)
        self.layout.addWidget(self.widget_stack)

        if isinstance(self.model, ContinuousScanModel):
            self.plot = Rolling1DPlotWidget(self.model)
        else:
            dim = len(self.model.axes)
            if dim == 1:
                self.plot = XY1DPlotWidget(self.model)
            elif dim == 2:
                self.plot = Image2DPlotWidget(self.model)
            else:
                self._show_error(
                    "Plots for {}-dimensional data are not yet implemented".format(dim))
                return
        self.widget_stack.addWidget(self.plot)
        self.plot.error.connect(self._show_error)
        self.plot.ready.connect(lambda: self._show(self.plot))

    def _show(self, widget):
        self.widget_stack.setCurrentIndex(self.widget_stack.indexOf(widget))

    def _show_error(self, message):
        self.message_label.setText("Error: " + message)
        self._show(self.message_label)

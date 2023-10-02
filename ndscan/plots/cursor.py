import numpy as np
import pyqtgraph
from .._qt import QtCore, QtWidgets


class LabeledCrosshairCursor(QtCore.QObject):
    """
    Manages a crosshair cursor for a PlotWidget, with adjacient labels giving the data
    coordinates corresponding to its position.

    The TextItems for displaying the coordinates are updated on a timer to avoid a lag
    trail of buffered redraws when there are a lot of points.
    """
    def __init__(self, cursor_target_widget: QtWidgets.QWidget,
                 plot_item: pyqtgraph.PlotItem, x_unit_suffix: str,
                 x_data_to_display_scale: float, y_unit_suffix: str,
                 y_data_to_display_scale: float):
        """
        :param cursor_target_widget: Widget to apply the cursor icon to.
        :param plot_item: Linked pyqtgraph plot.
        """
        super().__init__()

        self.plot_item = plot_item

        self.x_unit_suffix = x_unit_suffix
        self.x_data_to_display_scale = x_data_to_display_scale
        self.y_unit_suffix = y_unit_suffix
        self.y_data_to_display_scale = y_data_to_display_scale

        self.plot_item.getViewBox().hoverEvent = self._on_viewbox_hover
        cursor_target_widget.setCursor(QtCore.Qt.CursorShape.CrossCursor)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self._update_text)
        self.timer.setSingleShot(True)
        self.x_text = None
        self.y_text = None
        self.last_x = None
        self.last_y = None

    def _on_viewbox_hover(self, event):
        if event.isExit():
            self.plot_item.removeItem(self.x_text)
            self.x_text = None
            self.plot_item.removeItem(self.y_text)
            self.y_text = None

            self.timer.stop()
            return

        self.last_hover_event = event
        self.timer.start(0)

    def _update_text(self):
        vb = self.plot_item.getViewBox()
        data_coords = vb.mapSceneToView(self.last_hover_event.scenePos())

        # TODO: Draw text directly to graphics scene rather than going through
        # pyqtgraph for performance - don't need any of the fancy interaction
        # or layouting features that come with being a plot item.

        def make_text():
            text = pyqtgraph.TextItem()
            # Don't take text item into account for auto-scaling; otherwise
            # there will be positive feedback if the cursor is towards the
            # bottom right of the screen.
            text.setFlag(QtWidgets.QGraphicsItem.GraphicsItemFlag.ItemHasNoContents)
            self.plot_item.addItem(text)
            return text

        if not self.x_text:
            self.x_text = make_text()

        if not self.y_text:
            self.y_text = make_text()

        x_range, y_range = vb.state["viewRange"]
        x_range = np.array(x_range) * self.x_data_to_display_scale
        y_range = np.array(y_range) * self.y_data_to_display_scale

        def num_digits_after_point(r):
            # We want to be able to resolve at least 1000 points in the displayed
            # range.
            smallest_digit = np.floor(np.log10(r[1] - r[0])) - 3
            return int(-smallest_digit) if smallest_digit < 0 else 0

        self.x_text.setText("{0:.{width}f}{1}".format(
            data_coords.x() * self.x_data_to_display_scale,
            self.x_unit_suffix,
            width=num_digits_after_point(x_range)))
        self.x_text.setPos(data_coords)

        self.last_x = data_coords.x()

        y_text_pos = QtCore.QPointF(self.last_hover_event.scenePos())
        y_text_pos.setY(self.last_hover_event.scenePos().y() + 10)
        self.y_text.setText("{0:.{width}f}{1}".format(
            data_coords.y() * self.y_data_to_display_scale,
            self.y_unit_suffix,
            width=num_digits_after_point(y_range)))
        self.y_text.setPos(vb.mapSceneToView(y_text_pos))

        self.last_y = data_coords.y()

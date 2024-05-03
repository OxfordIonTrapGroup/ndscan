import numpy as np
import pyqtgraph
from .._qt import QtCore, QtWidgets
from .utils import SERIES_COLORS


class CrosshairLabel(QtWidgets.QGraphicsSimpleTextItem):
    """Text item to be displayed alongside the cursor when hovering a plot"""
    def __init__(self,
                 view_box: pyqtgraph.ViewBox,
                 unit_suffix: str = "",
                 data_to_display_scale: float = 1.0,
                 color: str = SERIES_COLORS[0]):
        super().__init__()

        self.setBrush(pyqtgraph.mkBrush(color))

        self.view_box = view_box
        self.unit_suffix = unit_suffix
        self.data_to_display_scale = data_to_display_scale

    def update(self, last_scene_pos: QtCore.QPointF):
        data_coords = self.view_box.mapSceneToView(last_scene_pos)
        self.update_coords(data_coords)

    def update_coords(self, data_coords):
        raise NotImplementedError

    def set_value(self, value: float, limits: tuple[float, float]):
        # Base case: we want to resolve at least milli-units on the data's scale.
        span = self.data_to_display_scale
        if limits[1] > limits[0]:
            # Preferred case: we want to resolve >1000 points in the displayed range.
            span *= (limits[1] - limits[0])
        elif np.abs(value) > 0:
            # Fallback case: we want to resolve >3 significant figures of the value.
            span *= value
        smallest_digit = np.floor(np.log10(span)) - 3
        precision = int(-smallest_digit) if smallest_digit < 0 else 0

        self.setText("{0:.{n}f}{1}".format(value * self.data_to_display_scale,
                                           self.unit_suffix,
                                           n=precision))


class CrosshairAxisLabel(CrosshairLabel):
    """Crosshair label for axis coordinates
    """
    def __init__(self,
                 view_box: pyqtgraph.ViewBox,
                 unit_suffix: str = "",
                 data_to_display_scale: float = 1.0,
                 color: str = SERIES_COLORS[0],
                 is_x: bool = False):
        super().__init__(view_box, unit_suffix, data_to_display_scale, color)
        self.is_x = is_x
        self.last_value = None

    def update_coords(self, data_coords):
        x_range, y_range = self.view_box.state["viewRange"]
        coord = data_coords.x() if self.is_x else data_coords.y()
        limits = tuple(x_range if self.is_x else y_range)
        self.set_value(coord, limits)
        self.last_value = coord


class LabeledCrosshairCursor(QtCore.QObject):
    """
    Manages a crosshair cursor for a PlotWidget, with adjacient labels giving the data
    coordinates corresponding to its position.

    The TextItems for displaying the coordinates are updated on a timer to avoid a lag
    trail of buffered redraws when there are a lot of points.
    """
    def __init__(self, cursor_target_widget: QtWidgets.QWidget,
                 plot_item: pyqtgraph.PlotItem, crosshair_items: list[CrosshairLabel]):
        """
        :param cursor_target_widget: Widget to apply the cursor icon to.
        :param plot_item: Linked pyqtgraph plot.
        """
        super().__init__()

        self.plot_item = plot_item
        self.crosshair_items = crosshair_items
        for item in self.crosshair_items:
            item.setParentItem(self.plot_item)

        self.plot_item.getViewBox().hoverEvent = self._on_viewbox_hover
        cursor_target_widget.setCursor(QtCore.Qt.CursorShape.CrossCursor)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self._update_text)
        self.timer.setSingleShot(True)

    def _on_viewbox_hover(self, event):
        if event.isExit():
            for item in self.crosshair_items:
                item.setVisible(False)

            self.timer.stop()
            return

        self.last_hover_event = event
        self.timer.start(0)

    def _update_text(self):
        for (i, item) in enumerate(self.crosshair_items):
            # Update text using last coordinate information.
            scene_pos = self.last_hover_event.scenePos()
            item.update(scene_pos)
            # Move label to the last cursor position.
            text_pos = scene_pos - item.sceneBoundingRect().topLeft()
            item.moveBy(text_pos.x() + 4, text_pos.y() + 10 * i + 2)
            # Show label.
            item.setVisible(True)

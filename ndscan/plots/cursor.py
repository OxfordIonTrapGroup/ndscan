import numpy as np
import pyqtgraph
from .._qt import QtCore, QtGui, QtWidgets
from .utils import SERIES_COLORS


class CrosshairLabel:
    """Text to be displayed alongside the cursor when hovering a plot

    To keep the text readable on all backgrounds, the label is drawn with a black
    outline. QGraphicsScene does not support ``CompositionMode``s, so this is the next
    best thing. To implement this, we need to render the text twice, as Qt does not
    seem to support drawing an outline that does not obstruct the actual character
    shape. The best way to implement this appears to be to simply create two
    ``QGraphicsSimpleTextItem``s placed on top of each other (overriding
    ``QGraphicsSimpleTextItem.paint()`` to render the item twice with different pen
    settings performs badly for some reason).
    """
    def __init__(self,
                 view_box: pyqtgraph.ViewBox,
                 unit_suffix: str = "",
                 data_to_display_scale: float = 1.0,
                 color: str = SERIES_COLORS[0]) -> None:
        bg, fg = self.text_items = [
            QtWidgets.QGraphicsSimpleTextItem(),
            QtWidgets.QGraphicsSimpleTextItem(),
        ]
        bg.setPen(
            QtGui.QPen(
                QtGui.QColor(QtCore.Qt.GlobalColor.black),
                3,
                QtCore.Qt.PenStyle.SolidLine,
                QtCore.Qt.PenCapStyle.RoundCap,
                QtCore.Qt.PenJoinStyle.RoundJoin,
            ))
        fg.setBrush(pyqtgraph.mkBrush(color))

        self.view_box = view_box
        self.unit_suffix = unit_suffix
        self.data_to_display_scale = data_to_display_scale

    def set_parent_item(self, parent):
        for label in self.text_items:
            label.setParentItem(parent)

    def update(self, last_scene_pos: QtCore.QPointF, y_idx: int):
        data_coords = self.view_box.mapSceneToView(last_scene_pos)
        self.update_coords(data_coords)
        for label in self.text_items:
            text_pos = last_scene_pos - label.sceneBoundingRect().topLeft()
            label.moveBy(text_pos.x() + 3, text_pos.y() + 11 * y_idx + 2)

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

        text = "{0:.{n}f}{1}".format(value * self.data_to_display_scale,
                                     self.unit_suffix,
                                     n=precision)
        for label in self.text_items:
            label.setText(text)

    def set_visible(self, visible: bool):
        for label in self.text_items:
            label.setVisible(visible)


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
    """Manages a crosshair cursor for a PlotWidget, with adjacent labels giving the data
    coordinates corresponding to its position.

    The TextItems for displaying the coordinates are updated on a timer to avoid a lag
    trail of buffered redraws when there are a lot of points.
    """
    def __init__(self, cursor_target_widget: QtWidgets.QWidget,
                 plot_item: pyqtgraph.PlotItem, labels: list[CrosshairLabel]):
        """
        :param cursor_target_widget: Widget to apply the cursor icon to.
        :param plot_item: Linked pyqtgraph plot.
        """
        super().__init__()

        self.plot_item = plot_item
        self.labels = labels
        for item in self.labels:
            item.set_parent_item(self.plot_item)

        self.plot_item.getViewBox().hoverEvent = self._on_viewbox_hover
        cursor_target_widget.setCursor(QtCore.Qt.CursorShape.CrossCursor)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self._update_text)
        self.timer.setSingleShot(True)

    def _on_viewbox_hover(self, event):
        if event.isExit():
            for item in self.labels:
                item.set_visible(False)
            self.timer.stop()
            return

        self.last_hover_event = event
        self.timer.start(0)

    def _update_text(self):
        for (i, item) in enumerate(self.labels):
            scene_pos = self.last_hover_event.scenePos()
            item.update(scene_pos, i)
            item.set_visible(True)

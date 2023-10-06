import numpy as np
import pyqtgraph
from .._qt import QtCore, QtWidgets
from .utils import SERIES_COLORS


class CrosshairLabel(pyqtgraph.TextItem):
    def __init__(self,
                 view_box: pyqtgraph.ViewBox,
                 unit_suffix: float,
                 data_to_display_scale: float,
                 color: str = SERIES_COLORS[0],
                 is_x: bool = False):
        super().__init__(color=color)
        # Don't take text item into account for auto-scaling; otherwise
        # there will be positive feedback if the cursor is towards the
        # bottom right of the screen.
        self.setFlag(QtWidgets.QGraphicsItem.GraphicsItemFlag.ItemHasNoContents)

        self.view_box = view_box
        self.unit_suffix = unit_suffix
        self.data_to_display_scale = data_to_display_scale
        self.is_x = is_x

        self.last_value = None

    def update(self, last_scene_pos: QtCore.QPointF):
        data_coords = self.view_box.mapSceneToView(last_scene_pos)

        x_range, y_range = self.view_box.state["viewRange"]
        # We want to be able to resolve at least 1000 points in the displayed
        # range.
        r = np.array(x_range if self.is_x else y_range) * self.data_to_display_scale
        smallest_digit = np.floor(np.log10(r[1] - r[0])) - 3
        width = int(-smallest_digit) if smallest_digit < 0 else 0

        coord = data_coords.x() if self.is_x else data_coords.y()
        self.setText("{0:.{n}f}{1}".format(coord * self.data_to_display_scale,
                                           self.unit_suffix,
                                           n=width))
        self.last_value = coord


class LabeledCrosshairCursor(QtCore.QObject):
    """
    Manages a crosshair cursor for a PlotWidget, with adjacient labels giving the data
    coordinates corresponding to its position.

    The TextItems for displaying the coordinates are updated on a timer to avoid a lag
    trail of buffered redraws when there are a lot of points.
    """
    def __init__(self, cursor_target_widget: QtWidgets.QWidget,
                 plot_item: pyqtgraph.PlotItem,
                 crosshair_items: list[CrosshairLabel]):
        """
        :param cursor_target_widget: Widget to apply the cursor icon to.
        :param plot_item: Linked pyqtgraph plot.
        """
        super().__init__()

        self.plot_item = plot_item
        self.crosshair_items = crosshair_items

        self.plot_item.getViewBox().hoverEvent = self._on_viewbox_hover
        cursor_target_widget.setCursor(QtCore.Qt.CursorShape.CrossCursor)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self._update_text)
        self.timer.setSingleShot(True)
        self._text_shown = False

    def _on_viewbox_hover(self, event):
        if event.isExit():
            for item in self.crosshair_items:
                self.plot_item.removeItem(item)
            self._text_shown = False

            self.timer.stop()
            return

        self.last_hover_event = event
        self.timer.start(0)

    def _update_text(self):
        vb = self.plot_item.getViewBox()
        # TODO: Draw text directly to graphics scene rather than going through
        # pyqtgraph for performance - don't need any of the fancy interaction
        # or layouting features that come with being a plot item.

        for (i, crosshair_item) in enumerate(self.crosshair_items):
            if not self._text_shown:
                self.plot_item.addItem(crosshair_item)

            last_scene_pos = self.last_hover_event.scenePos()
            crosshair_item.update(last_scene_pos)

            text_pos = QtCore.QPointF(last_scene_pos)
            text_pos.setY(last_scene_pos.y() + i * 10)
            crosshair_item.setPos(vb.mapSceneToView(text_pos))

        self._text_shown = True

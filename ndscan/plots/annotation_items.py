"""Plot annotations, which are rendered as one or more pyqtgraph graphics objects.

This includes, for instance, fit curves and lines indicating fit results.
"""

import logging
import numbers

import numpy
import pyqtgraph
from oitg import uncertainty_to_string

from .._qt import QtCore
from ..utils import FIT_OBJECTS
from .model import AnnotationDataSource

logger = logging.getLogger(__name__)


class AnnotationItem(QtCore.QObject):
    def remove(self) -> None:
        """Remove any pyqtgraph graphics items from target plot and stop listening to
        changes."""
        raise NotImplementedError


class ComputedCurveItem(AnnotationItem):
    """Shows a curve (pyqtgraph.LineItem) that is computed from a given fit function,
    dynamically adapting to the coordinate region displayed.

    :param function_name: The name of the function (see :data:`FIT_OBJECTS`) to
        evaluate.
    :param data_sources: A dictionary giving the parameters for the curve.
    :param view_box: The :class:`pyqtgraph.ViewBox` to add the line item to once there
        is data.
    :param curve_item: The target line item to draw the curve into. This will have been
        set up by the client with the appropriate styling and will be added to
        ``view_box`` as soon as there is data.
    :param x_limits: Limits to restrict the drawn horizontal range to even if the
        viewport extends beyond them.
    """

    @staticmethod
    def is_function_supported(function_name: str) -> bool:
        return function_name in FIT_OBJECTS

    def __init__(
        self,
        function_name: str,
        data_sources: dict[str, AnnotationDataSource],
        view_box,
        curve_item,
        x_limits: tuple[float | None, float | None],
    ):
        super().__init__(view_box)  # Automatically disconnect when view_box is deleted.
        self._function = FIT_OBJECTS[function_name].fitting_function
        self._data_sources = data_sources
        self._view_box = view_box
        self._curve_item = curve_item
        self._x_limits = x_limits
        self._curve_item_added = False

        self.redraw_limiter = pyqtgraph.SignalProxy(
            self._view_box.sigXRangeChanged, slot=self._redraw, rateLimit=30
        )

        for source in self._data_sources.values():
            source.changed.connect(self.redraw_limiter.signalReceived)

        self.redraw_limiter.signalReceived()

    def remove(self):
        for source in self._data_sources.values():
            source.changed.disconnect(self.redraw_limiter.signalReceived)
        if self._curve_item_added:
            self._view_box.removeItem(self._curve_item)

    def _redraw(self, *args):
        params = {}
        for name, source in self._data_sources.items():
            value = source.get()
            if value is None:
                # Don't have enough data yet.
                # TODO: Use exception instead of None for cleanliness?
                return
            params[name] = value

        if not self._curve_item_added:
            # Ignore bounding box of newly added line for auto-range computation, as we
            # choose its range based on the visible area.
            self._view_box.addItem(self._curve_item, ignoreBounds=True)
            self._curve_item_added = True

        # Choose horizontal range based on currently visible area (extending slightly
        # beyond it to ensure a visually smooth border).
        x_range, _ = self._view_box.state["viewRange"]
        ext = (x_range[1] - x_range[0]) / 10
        x_lims = [x_range[0] - ext, x_range[1] + ext]

        if self._x_limits[0] is not None:
            x_lims[0] = max(x_lims[0], self._x_limits[0])
        if self._x_limits[1] is not None:
            x_lims[1] = min(x_lims[1], self._x_limits[1])

        # Choose number of points based on width of plot on screen (in pixels).
        fn_xs = numpy.linspace(*x_lims, int(self._view_box.width()))

        fn_ys = self._function(fn_xs, params)
        self._curve_item.setData(fn_xs, fn_ys)


class CurveItem(AnnotationItem):
    """Shows a curve between the given x/y coordinate pairs."""

    def __init__(
        self,
        x_source: AnnotationDataSource,
        y_source: AnnotationDataSource,
        view_box,
        curve_item,
    ):
        super().__init__(view_box)  # Automatically disconnect when view_box is deleted.
        self._x_source = x_source
        self._y_source = y_source
        self._view_box = view_box
        self._curve_item = curve_item
        self._curve_item_added = False

        for source in [self._x_source, self._y_source]:
            source.changed.connect(self._redraw)

        self._redraw()

    def remove(self):
        for source in [self._x_source, self._y_source]:
            source.changed.disconnect(self._redraw)
        if self._curve_item_added:
            self._view_box.removeItem(self._curve_item)

    def _redraw(self):
        xs = self._x_source.get()
        if xs is None:
            return

        ys = self._y_source.get()
        if ys is None:
            return

        if len(xs) != len(ys):
            logger.warning(
                "Mismatching data for 'curve' annotation, ignoring "
                + "(len(xs) = %s vs. len(ys) = %s).",
                len(xs),
                len(ys),
            )
            return

        if not self._curve_item_added:
            self._view_box.addItem(self._curve_item)
            self._curve_item_added = True
        self._curve_item.setData(xs, ys)


class VLineItem(AnnotationItem):
    """Vertical line marking a given x coordinate, with optional confidence interval."""

    def __init__(
        self,
        position_source: AnnotationDataSource,
        uncertainty_source: AnnotationDataSource | None,
        view_box,
        base_color,
        x_data_to_display_scale,
        x_unit_suffix,
        show_label,
    ):
        super().__init__(view_box)  # Automatically disconnect when view_box is deleted.
        self._position_source = position_source
        self._uncertainty_source = uncertainty_source
        self._view_box = view_box
        self._x_data_to_display_scale = x_data_to_display_scale
        self._x_unit_suffix = x_unit_suffix
        self._added_to_plot = False
        self._show_label = show_label

        # Position label within initial view range.
        ypos_label = (view_box.height() - 7) / view_box.height()

        self._left_line = pyqtgraph.InfiniteLine(
            movable=False,
            angle=90,
            pen={"color": base_color, "style": QtCore.Qt.PenStyle.DotLine},
        )
        self._center_line = pyqtgraph.InfiniteLine(
            movable=False,
            angle=90,
            label="",
            labelOpts={"position": ypos_label, "color": base_color, "movable": True},
            pen={"color": base_color, "style": QtCore.Qt.PenStyle.SolidLine},
        )
        self._right_line = pyqtgraph.InfiniteLine(
            movable=False,
            angle=90,
            pen={"color": base_color, "style": QtCore.Qt.PenStyle.DotLine},
        )

        self._position_source.changed.connect(self._redraw)
        if self._uncertainty_source:
            self._uncertainty_source.changed.connect(self._redraw)

        self._redraw()

    def remove(self):
        self._position_source.changed.disconnect(self._redraw)
        if self._uncertainty_source:
            self._uncertainty_source.changed.disconnect(self._redraw)
        if self._added_to_plot:
            for line in (self._left_line, self._center_line, self._right_line):
                self._view_box.removeItem(line)

    def _redraw(self):
        x = self._position_source.get()
        if x is None:
            return

        if not self._added_to_plot:
            self._view_box.addItem(self._left_line, ignoreBounds=True)
            self._view_box.addItem(self._center_line, ignoreBounds=True)
            self._view_box.addItem(self._right_line, ignoreBounds=True)
            self._added_to_plot = True

        delta_x = None
        if self._uncertainty_source:
            delta_x = self._uncertainty_source.get()

        if delta_x is None or numpy.isnan(delta_x) or delta_x == 0.0:
            # If the covariance extraction failed, just don't display the
            # confidence interval at all.
            delta_x = 0.0
            label = str(x * self._x_data_to_display_scale)
        else:
            label = uncertainty_to_string(
                x * self._x_data_to_display_scale,
                delta_x * self._x_data_to_display_scale,
            )

        if self._show_label:
            self._center_line.label.setFormat(label + self._x_unit_suffix)

        self._left_line.setPos(x - delta_x)
        self._center_line.setPos(x)
        self._right_line.setPos(x + delta_x)


class TextAnnotationStack(QtCore.QObject):
    """Arranges text items in a column pinned to the top right corner of a view box.

    The items are positioned in screen space (i.e. they do not move as the view range
    changes), in the order they were added. Hidden items do not take up any space.
    """

    #: Distance from the view box border, in pixels.
    MARGIN = 2

    def __init__(self, view_box):
        super().__init__(view_box)  # Automatically disconnect when view_box is deleted.
        self._view_box = view_box
        self._items = list[pyqtgraph.TextItem]()
        self._view_box.sigResized.connect(self.relayout)

    def add_item(self, item: pyqtgraph.TextItem) -> None:
        # Parent the item to the view box directly instead of using addItem(), which
        # would place it in the child group transformed according to the view range.
        item.setParentItem(self._view_box)
        item.setZValue(1000)
        self._items.append(item)
        self.relayout()

    def remove_item(self, item: pyqtgraph.TextItem) -> None:
        self._items.remove(item)
        if (scene := item.scene()) is not None:
            scene.removeItem(item)
        self.relayout()

    def relayout(self) -> None:
        x = self._view_box.width() - self.MARGIN
        y = self.MARGIN
        for item in self._items:
            if not item.isVisibleTo(self._view_box):
                continue
            item.setPos(x, y)
            y += item.boundingRect().height()


class _StackedTextItem(AnnotationItem):
    """Base class for annotations shown as a line of text in a
    :class:`TextAnnotationStack`, updated whenever the data sources change (and hidden
    while there is no data yet).

    Subclasses call :meth:`_connect_sources` once set up, and implement
    :meth:`_format_text`.
    """

    def __init__(self, stack: TextAnnotationStack, color):
        super().__init__(stack)  # Automatically disconnect when stack is deleted.
        self._stack = stack
        self._sources = []
        # Use a translucent background to keep the text legible on top of any data
        # points/grid lines, and a small margin to visually group the lines together.
        fill = pyqtgraph.mkColor(pyqtgraph.getConfigOption("background"))
        fill.setAlpha(160)
        self._text_item = pyqtgraph.TextItem(color=color, anchor=(1, 0), fill=fill)
        self._text_item.textItem.document().setDocumentMargin(1)
        self._text_item.setVisible(False)
        self._stack.add_item(self._text_item)

    def _connect_sources(self, sources: list[AnnotationDataSource | None]) -> None:
        self._sources = [s for s in sources if s is not None]
        for source in self._sources:
            source.changed.connect(self._redraw)
        self._redraw()

    def remove(self):
        for source in self._sources:
            source.changed.disconnect(self._redraw)
        self._stack.remove_item(self._text_item)

    def _format_text(self) -> str | None:
        """Return the text to display, or ``None`` if there is no data yet."""
        raise NotImplementedError

    def _redraw(self):
        text = self._format_text()
        if text is None:
            self._text_item.setVisible(False)
        else:
            self._text_item.setText(text)
            self._text_item.setVisible(True)
        self._stack.relayout()


class NoteItem(_StackedTextItem):
    """Shows a free-form line of text."""

    def __init__(
        self, text_source: AnnotationDataSource, stack: TextAnnotationStack, color
    ):
        super().__init__(stack, color)
        self._text_source = text_source
        self._connect_sources([text_source])

    def _format_text(self) -> str | None:
        text = self._text_source.get()
        return None if text is None else str(text)


class NamedValueItem(_StackedTextItem):
    """Shows a value with optional uncertainty as ``<label>: <value>(<error>) <unit>``
    (see :func:`format_named_value`).
    """

    def __init__(
        self,
        label: str,
        value_source: AnnotationDataSource,
        error_source: AnnotationDataSource | None,
        unit: str,
        scale: float,
        stack: TextAnnotationStack,
        color,
    ):
        super().__init__(stack, color)
        self._label = label
        self._value_source = value_source
        self._error_source = error_source
        self._unit = unit
        self._scale = scale
        self._connect_sources([value_source, error_source])

    def _format_text(self) -> str | None:
        value = self._value_source.get()
        if value is None:
            return None
        error = self._error_source.get() if self._error_source else None
        return format_named_value(self._label, value, error, self._unit, self._scale)


def format_named_value(label: str, value, error, unit: str, scale: float) -> str:
    """Format a named value as ``<label>: <value>(<error>) <unit>``.

    :param value: The value, in data (SI base) units. Non-numerical values are shown
        as-is (without unit).
    :param error: The uncertainty associated with the value, in data units. If
        ``None``, ``NaN`` or zero, it is omitted.
    :param unit: The name of the display unit, if any.
    :param scale: The unit scale; values are divided by this for display.
    """
    if not isinstance(value, numbers.Real) or isinstance(value, bool):
        # Unit can only be meaningfully attached to numbers.
        return f"{label}: {value}"

    if scale != 1:
        value /= scale

    if not isinstance(error, numbers.Real) or numpy.isnan(error) or error == 0.0:
        # As for VLineItem, just omit the uncertainty if it is not available (e.g.
        # because the covariance extraction failed in a fit).
        value_str = str(value)
    else:
        value_str = uncertainty_to_string(value, error / scale)

    unit_suffix = (" " + unit) if unit else ""
    return f"{label}: {value_str}{unit_suffix}"

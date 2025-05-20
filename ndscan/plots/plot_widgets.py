"""PlotWidgets with context menus to switch to alternate plots and/or open subplots."""

# TODO: This was implemented using inheritance before it was clear that extensive
# monkey patching of pyqtgraph would be required to get the context menus right. The
# functionality should really be integrated into a plot widget using composition
# instead, as there might be other, similar functionality that plot widgets want to mix
# in.

import logging
import pyqtgraph
import pyqtgraph.exporters
from .._qt import QtCore, QtGui, QtWidgets
from .model import Context, Root
from typing import Callable

logger = logging.getLogger(__name__)


class MultiYAxisPlotItem(pyqtgraph.PlotItem):
    """Wraps PlotItem with the ability to create multiple y axes linked to the same x
    axis.

    This is somewhat of a hack following the MultiplePlotAxes pyqtgraph example.
    """
    def __init__(self):
        super().__init__()
        self._num_y_axes = 0
        self._additional_view_boxes = []
        self._additional_right_axes = []

    def show_border(self):
        self.getViewBox().setBorder(
            pyqtgraph.functions.mkPen(pyqtgraph.getConfigOption("foreground")))

    def new_y_axis(self):
        self._num_y_axes += 1

        if self._num_y_axes == 1:
            return self.getAxis("left"), self.getViewBox()

        vb = pyqtgraph.ViewBox()

        if self._num_y_axes == 2:
            # With more than one axis, we need to start resizing the linked views.
            self.getViewBox().sigResized.connect(self._update_additional_view_boxes)

            self.showAxis("right")
            axis = self.getAxis("right")
        else:
            axis = pyqtgraph.AxisItem("right")
            # FIXME: Z value setting is cargo-culted in from the pyqtgraph example –
            # what should the correct value be?
            axis.setZValue(-10000)
            self._additional_right_axes.append(axis)
            self.layout.addItem(axis, 2, self._num_y_axes)

        self.scene().addItem(vb)
        axis.linkToView(vb)
        axis.setGrid(False)
        vb.setXLink(self)
        self._additional_view_boxes.append(vb)
        self._update_additional_view_boxes()
        return axis, vb

    def reset_y_axes(self):
        # TODO: Do we need to unlink anything else to avoid leaking memory?
        for vb in self._additional_view_boxes:
            self.removeItem(vb)
        self._additional_view_boxes = []
        for axis in self._additional_right_axes:
            self.layout.removeItem(axis)
        self._additional_right_axes = []
        self._num_y_axes = 0

    def _update_additional_view_boxes(self):
        for vb in self._additional_view_boxes:
            vb.setGeometry(self.getViewBox().sceneBoundingRect())
        for vb in self._additional_view_boxes:
            vb.linkedViewChanged(self.getViewBox(), vb.XAxis)


class VerticalPanesWidget(pyqtgraph.GraphicsLayoutWidget):
    """A vertical stack of (potentially) multiple plot panes with a single shared
    x axis.

    For the sake of clarity, the concept of one such subplot is consistently referred to
    as a "pane" throughout the code.
    """

    #: Emitted when the plot is fully constructed and ready to be displayed to the user.
    #: Implementations must emit this *after the object was initially constructed* (e.g.
    #: using call_later() if the channel schemata were already available).
    ready = QtCore.pyqtSignal()

    #: Emitted when erroneous model data was encountered and the error message from the
    #: argument should be displayed to the user instead of this widget.
    error = QtCore.pyqtSignal(str)

    #: Emitted when the user opened a subplot/… and the containing widget should show
    #: the given widget in a new dock. Arguments are (VerticalPanesWidget to show,
    #: dock title).
    new_dock_requested = QtCore.pyqtSignal(object, str)

    #: Emitted after the dock containing this widget was closed by the user (if a
    #: subplot/…), whether through a context menu or the docking area UI.
    was_closed = QtCore.pyqtSignal()

    def __init__(self):
        super().__init__()
        self.layout: QtGui.QGraphicsGridLayout = self.ci.layout
        self.layout.setContentsMargins(3, 3, 3, 3)
        self.layout.setVerticalSpacing(3)
        self.panes = list[MultiYAxisPlotItem]()

        self.copy_shortcut = QtGui.QShortcut(QtGui.QKeySequence.StandardKey.Copy, self)
        self.copy_shortcut.activated.connect(self.copy_to_clipboard)
        self._flash_overlay = None

        # We don't need any scroll gestures, etc., and this avoids "qt.pointer.dispatch:
        # skipping QEventPoint(…) : no target window" stderr spam on macOS from within
        # Qt itself.
        self.viewport().setAttribute(QtCore.Qt.WidgetAttribute.WA_AcceptTouchEvents,
                                     False)

    def add_pane(self) -> MultiYAxisPlotItem:
        """Extend layout vertically by one :class:`.MultiYAxisPlotItem`."""
        plot = MultiYAxisPlotItem()
        if self.panes:
            self.nextRow()
        self.addItem(plot)
        self.panes.append(plot)

        # KLUDGE: We want all the plot panes (the actual square regions) to be the same
        # height. For whatever reason, calling either setStyle(showValues=False) in
        # link_x_axes() or adding the x axis label (in the derived plot widgets) causes,
        # for two panes, the top one to be significantly taller. The discrepancy is even
        # larger than if the screen space was just divided into two without regard for
        # the extra space needed for the axis or tick labels. Setting the row stretch
        # factor to 0 for all the rows leads to equal, but of course way too small
        # panes. Leaving the factors at the default (all 1), but setting the preferred
        # height to a large value, for whatever reason, seems to give the desired
        # results. We will want to revisit this in the future when it inevitably breaks
        # again.
        self.layout.setRowPreferredHeight(len(self.panes) - 1, 10000)

        return plot

    def link_x_axes(self) -> None:
        """Fold all x axes into one shared one on the bottom.

        Call after all panes have been added.
        """
        if len(self.panes) < 2:
            # Nothing to link; just leave everything to the default pyqtgraph layout,
            # which has better (responsive) y-axis width scaling.
            return

        # Ensure left spines of all panes are aligned by forcing the y axes to the same
        # width.
        # FIXME: This should be dynamic (e.g. if the number of tick digits changes).
        max_axis_width = max(p.getAxis("left").width() for p in self.panes)
        for pane in self.panes:
            pane.getAxis("left").setWidth(max_axis_width)

            # With more than one stacked plot with grids, having a complete border
            # instead of drawing only the axes looks nicer.
            pane.show_border()

            if pane is not self.panes[-1]:
                pane.setXLink(self.panes[-1])
                # We can't completely hide the bottom axis, as the vertical grid lines
                # are also part of it.
                pane.getAxis("bottom").setStyle(showValues=False)

    def clear_panes(self):
        for pane in self.panes:
            pane.reset_y_axes()
        self.panes.clear()

    def copy_to_clipboard(self):
        if self._flash_overlay is not None:
            # Don't allow multiple clipboard copies extremely quickly after each other;
            # writing robust animation overlap logic is not worth the effort.
            return

        pyqtgraph.exporters.ImageExporter(self.scene()).export(copy=True)

        # Create a semi-transparent white overlay covering the entire plot, fading it
        # in and out to create a smooth flashing effect.
        self._flash_overlay = QtWidgets.QGraphicsRectItem(self.scene().sceneRect())
        self._flash_overlay.setBrush(QtGui.QColor(230, 230, 255, 160))
        self.scene().addItem(self._flash_overlay)
        self.scene().update()

        # I tried using a QtWidgets.QGraphicsOpacityEffect() and a
        # QtCore.QPropertyAnimation, which works for simple (?) cases (e.g. one
        # image_2d plot), but for more complex (?) plots (e.g. three panes), the
        # underlying graphics engine seems to be swapped, causing inactive QPainter
        # error. Just use a generic animation and set the opacity manually.
        self._flash_animation = QtCore.QVariantAnimation(self)
        self._flash_animation.setDuration(180)
        self._flash_animation.setEasingCurve(QtCore.QEasingCurve.Type.OutQuad)
        self._flash_animation.setStartValue(0.0)
        self._flash_animation.setKeyValueAt(0.08, 1.0)
        self._flash_animation.setEndValue(0.0)
        self._flash_animation.setLoopCount(1)
        self._flash_animation.valueChanged.connect(self._fade_flash_overlay)
        self._flash_animation.finished.connect(self._remove_flash_overlay)
        self._flash_animation.start()

    def _fade_flash_overlay(self, opacity):
        self._flash_overlay.setOpacity(opacity)
        self.scene().update()

    def _remove_flash_overlay(self):
        self.scene().removeItem(self._flash_overlay)
        self._flash_overlay = None


class ContextMenuBuilder:
    """Builds a list of QActions and separators to display in a QMenu context menu.

    Elides multiple separators in a row.
    """
    def __init__(self, target_menu):
        self._last_was_no_separator = False
        self._entries = []
        self._target_menu = target_menu

    def finish(self) -> list[QtGui.QAction]:
        return self._entries

    def ensure_separator(self):
        if self._last_was_no_separator:
            separator = self.append_action("")
            separator.setSeparator(True)
            self._last_was_no_separator = False

    def append_action(self, title) -> QtGui.QAction:
        action = QtGui.QAction(title, self._target_menu)
        self._append(action)
        return action

    def append_widget_action(self) -> QtWidgets.QWidgetAction:
        action = QtWidgets.QWidgetAction(self._target_menu)
        self._append(action)
        return action

    def append_menu(self, title) -> QtWidgets.QMenu:
        menu = QtWidgets.QMenu(title, parent=self._target_menu)
        self._append(menu)
        return menu

    def _append(self, action):
        self._last_was_no_separator = True
        self._entries.append(action)


class ContextMenuPanesWidget(VerticalPanesWidget):
    """VerticalPanesWidget with support for dynamically populated context menus."""
    def add_pane(self) -> MultiYAxisPlotItem:
        pane = super().add_pane()

        # The pyqtgraph getContextMenus() mechanism by default isn't very useful –
        # returned entries are appended to the menu every time the function is called.
        # This just happens to work out in the most common case where menus are static,
        # as QMenu ignores appended actions that are already part of the menu.
        #
        # To make menus with dynamic entries work, we monkey-patch the ViewBox
        # raiseContextMenu() implementation to create a new QMenu (ViewBoxMenu) instance
        # every time. This is slightly wasteful, but context menus should be created
        # seldomly enough for the slight increase in latency not to matter.

        def get_context_menu(*args, pane_idx=len(self.panes) - 1):
            builder = ContextMenuBuilder(pane.getViewBox().menu)
            self.build_context_menu(pane_idx, builder)
            return builder.finish()

        pane.getContextMenus = get_context_menu

        vb = pane.getViewBox()
        orig_raise_context_menu = vb.raiseContextMenu

        def raiseContextMenu(ev):
            vb.menu = pyqtgraph.graphicsItems.ViewBox.ViewBoxMenu.ViewBoxMenu(vb)
            return orig_raise_context_menu(ev)

        vb.raiseContextMenu = raiseContextMenu

        return pane

    def build_context_menu(self, pane_idx: int, builder: ContextMenuBuilder) -> None:
        pass


class SubplotMenuPanesWidget(ContextMenuPanesWidget):
    """PlotWidget with a context menu to open new windows for subplots.
    """
    def __init__(self):
        super().__init__()

        #: Maps subscan names to model Root instances. Populated once we have the
        #: channel schemata; also keeps the Root objects alive.
        self.subscan_roots: dict[str, Root] = {}

        #: Maps subplot names to active plot widgets.
        self.subscan_plots: dict[str, VerticalPanesWidget] = {}

    def closeEvent(self, ev):
        # Hide subplots as well when hiding the parent plot (i.e. self).
        for w in self.subscan_plots.values():
            w.close()
        super().closeEvent(ev)

    def build_context_menu(self, pane_idx: int, builder: ContextMenuBuilder) -> None:
        if len(self.subscan_roots) > 0:
            builder.ensure_separator()
            for name in self.subscan_roots.keys():
                action = builder.append_action(f"Subscan '{name}'")
                action.setCheckable(True)
                action.setChecked(name in self.subscan_plots)
                action.triggered.connect(
                    lambda *a, name=name: self._toggle_subscan_plot(name))
        super().build_context_menu(pane_idx, builder)

    def _toggle_subscan_plot(self, name):
        toggle_all = (QtWidgets.QApplication.keyboardModifiers()
                      & QtCore.Qt.KeyboardModifier.ShiftModifier)
        if name in self.subscan_plots:
            if toggle_all:
                # This will also end up removing the plots from self.subscan_plots; take
                # list() to not depend on the details of the signal dispatch timing.
                for key in list(self.subscan_plots.keys()):
                    self.close_subscan_plot(key)
            else:
                # Just close the one plot.
                self.close_subscan_plot(name)
        else:
            if toggle_all:
                for name in self.subscan_roots.keys():
                    if name not in self.subscan_plots:
                        self.open_subscan_plot(name)
            else:
                self.open_subscan_plot(name)

    def open_subscan_plot(self, name):
        assert name not in self.subscan_plots
        try:
            from .container_widgets import RootWidget
            plot = RootWidget(self.subscan_roots[name])
        except NotImplementedError as err:
            logger.info("Ignoring subscan '%s': %s", name, str(err))
            return
        self.subscan_plots[name] = plot
        plot.new_dock_requested.connect(self.new_dock_requested)
        plot.was_closed.connect(lambda: self.subscan_plots.pop(name).deleteLater())
        self.new_dock_requested.emit(plot, f"subscan '{name}'")

    def close_subscan_plot(self, name):
        # This triggers the plot widget's closeEvent, which in turn emits was_closed(),
        # which in causes the dock to be removed.
        self.subscan_plots[name].close()


def add_source_id_label(view_box: pyqtgraph.ViewBox,
                        context: Context) -> pyqtgraph.TextItem:
    """Add a translucent TextItem pinned to the bottom left of the view box displaying
    the context source id string.
    """
    text_item = pyqtgraph.TextItem(text="",
                                   anchor=(0, 1),
                                   color=(255, 255, 255),
                                   fill=(0, 0, 0))
    text_item.setZValue(1000)
    text_item.setOpacity(0.3)
    view_box.addItem(text_item, ignoreBounds=True)

    def update_text(*args):
        text_item.setText(" " + context.get_source_id() + " ")

    context.source_id_changed.connect(update_text)
    update_text()

    def update_text_pos(*args):
        ((x, _), (y, _)) = view_box.viewRange()
        text_item.setPos(x, y)

    view_box.sigRangeChanged.connect(update_text_pos)
    update_text_pos()

    return text_item


def build_channel_selection_context_menu(builder: ContextMenuBuilder,
                                         state_changed_callback: Callable[[], None],
                                         data_names: list[str],
                                         hidden_channels: set[str]):
    """Create a submenu of checkboxes to control the set of `hidden_channels`.

    :param builder: Instance of ``ContextMenuBuilder``.
    :param state_changed_callback: Method to call when ``hidden_channels`` has been
        changed.
    :param data_names: List of channel names to select from.
    :param hidden_channels: The set of hidden channels, modified in-place depending
        on the state of the checkboxes created here.
    """
    container = QtWidgets.QWidget()
    layout = QtWidgets.QVBoxLayout()
    container.setLayout(layout)
    submenu = builder.append_menu("Channels")
    action = QtWidgets.QWidgetAction(submenu)
    action.setDefaultWidget(container)
    submenu.addAction(action)

    checkboxes = [QtWidgets.QCheckBox(name) for name in data_names]

    def update_checkboxes_enabled():
        # Prevent the user from hiding all channels.
        if sum(cb.isChecked() for cb in checkboxes) == 1:
            for cb in checkboxes:
                if cb.isChecked():
                    cb.setEnabled(False)
                    break
        else:
            for cb in checkboxes:
                cb.setEnabled(True)

    def state_changed(state, name):
        if state == 0:
            hidden_channels.add(name)
        else:
            hidden_channels.discard(name)
        update_checkboxes_enabled()
        state_changed_callback()

    for name, checkbox in zip(data_names, checkboxes):
        checkbox.setTristate(False)
        checkbox.setChecked(name not in hidden_channels)
        checkbox.stateChanged.connect(lambda a, n=name: state_changed(a, n))
        layout.addWidget(checkbox)
    update_checkboxes_enabled()

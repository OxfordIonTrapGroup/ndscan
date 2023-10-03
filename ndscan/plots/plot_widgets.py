"""PlotWidgets with context menus to switch to alternate plots and/or open subplots."""

# TODO: This was implemented using inheritance before it was clear that extensive
# monkey patching of pyqtgraph would be required to get the context menus right. The
# functionality should really be integrated into a plot widget using composition
# instead, as there might be other, similar functionality that plot widgets want to mix
# in.

import pyqtgraph
from .._qt import QtCore, QtGui, QtWidgets
from .model import Context


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
        self._borderPen = pyqtgraph.functions.mkPen(
            pyqtgraph.getConfigOption("foreground"), width=0.7)
        self.draw_border = False

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

    def paint(self, painter, *args):
        super().paint(painter, *args)
        if self.draw_border:
            # In what is a bit of a hack, add in a border for the top and right borders
            # as well to make stacked plots look a bit nicer.
            painter.setPen(self._borderPen)
            painter.drawRect(
                self.mapRectFromScene(self.getViewBox().sceneBoundingRect()))


class VerticalPanesWidget(pyqtgraph.GraphicsLayoutWidget):
    """A vertical stack of (potentially) multiple plot panes with a single shared
    x axis.

    For the sake of clarity, the concept of one such subplot is consistently referred to
    as a "pane" throughout the code.
    """
    def __init__(self):
        super().__init__()
        self.panes = list[MultiYAxisPlotItem]()

    def add_pane(self) -> MultiYAxisPlotItem:
        """Extend layout vertically by one :class:`.MultiYAxisPlotItem`."""
        plot = MultiYAxisPlotItem()
        self.addItem(plot)
        self.nextRow()
        self.panes.append(plot)
        return plot

    def link_x_axes(self) -> None:
        """Fold all x axes into one shared one on the bottom.

        Call after all panes have been added.
        """
        max_axis_width = max(p.getAxis("left").width() for p in self.panes)
        for pane in self.panes:
            pane.getAxis("left").setWidth(max_axis_width)
            pane.draw_border = True

        for pane in self.panes[:-1]:
            pane.setXLink(self.panes[-1])
            # We can't completely hide the bottom axis, as the vertical grid lines are
            # also part of it.
            pane.getAxis("bottom").setStyle(showValues=False)

    def clear(self) -> None:
        for pane in self.panes:
            pane.reset_y_axes()
        self.panes.clear()
        super().clear()


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

    def _append(self, action):
        self._last_was_no_separator = True
        self._entries.append(action)


class ContextMenuPanesWidget(VerticalPanesWidget):
    """PlotWidget with support for dynamically populated context menus."""
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

        def get_context_menu(pane_idx=len(self.panes) - 1):
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


class AlternateMenuPanesWidget(ContextMenuPanesWidget):
    """PlotWidget with context menu for integration with the
    .container_widget.PlotContainerWidget alternate plot switching functionality.

    Alternate plots are shown *instead* of the main plot (as compared to subplots, which
    are shown separately).
    """

    alternate_plot_requested = QtCore.pyqtSignal(str)

    def __init__(self, get_alternate_plot_names):
        super().__init__()
        self._get_alternate_plot_names = get_alternate_plot_names

    def build_context_menu(self, pane_idx: int, builder: ContextMenuBuilder) -> None:
        alternate_plot_names = self._get_alternate_plot_names()
        if len(alternate_plot_names) > 1:
            for name in alternate_plot_names:
                action = builder.append_action("Show " + name)
                action.triggered.connect(
                    lambda *args, name=name: self.alternate_plot_requested.emit(name))
        builder.ensure_separator()


class SubplotMenuPanesWidget(AlternateMenuPanesWidget):
    """PlotWidget with a context menu to open new windows for subplots (in addition to
    AlternateMenuPanesWidget functionality).
    """
    def __init__(self, context: Context, get_alternate_plot_names):
        super().__init__(get_alternate_plot_names)
        self._context = context

        #: Maps subscan names to model Root instances.
        self.subscan_roots = {}

        #: Maps subplot names to active plot widgets.
        self.subplot_widgets = {}

    def hideEvent(self, *args):
        # Hide subplots as well when hiding the parent plot (i.e. self). This in
        # particular also handles the case where the main window is closed. Arguably,
        # closeEvent() would be the better place to do this, but that only works for
        # top-level windows.
        for w in self.subplot_widgets.values():
            w.hide()
        super().hideEvent(*args)

    def build_context_menu(self, pane_idx: int, builder: ContextMenuBuilder) -> None:
        for name in self.subscan_roots.keys():
            action = builder.append_action(f"Open subscan '{name}'")
            action.triggered.connect(lambda *args, name=name: self.open_subplot(name))
        builder.ensure_separator()
        super().build_context_menu(pane_idx, builder)

    def open_subplot(self, name: str):
        widget = self.subplot_widgets.get(name, None)
        if widget is not None:
            widget.show()
            widget.activateWindow()
            return

        import ndscan.plots.container_widgets as containers
        widget = containers.RootWidget(self.subscan_roots[name], self._context)
        self.subplot_widgets[name] = widget
        # TODO: Save window geometry.
        widget.resize(600, 400)
        widget.setWindowFlag(QtCore.Qt.WindowType.WindowStaysOnTopHint)
        widget.show()


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

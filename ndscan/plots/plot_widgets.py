"""PlotWidgets with context menus to switch to alternate plots and/or open subplots."""

# TODO: This was implemented using inheritance before it was clear that extensive
# monkey patching of pyqtgraph would be required to get the context menus right. The
# functionality should really be integrated into a plot widget using composition
# instead, as there might be other, similar functionality that plot widgets want to mix
# in.

import pyqtgraph
from typing import List
from quamash import QtCore, QtWidgets


class ContextMenuBuilder:
    """Builds a list of QActions and separators to display in a QMenu context menu.

    Elides multiple separators in a row.
    """

    def __init__(self, target_menu):
        self._last_was_no_separator = False
        self._entries = []
        self._target_menu = target_menu

    def finish(self) -> List[QtWidgets.QAction]:
        return self._entries

    def ensure_separator(self):
        if self._last_was_no_separator:
            separator = self.append_action("")
            separator.setSeparator(True)
            self._last_was_no_separator = False

    def append_action(self, title) -> QtWidgets.QAction:
        action = QtWidgets.QAction(title, self._target_menu)
        self._append(action)
        return action

    def append_widget_action(self) -> QtWidgets.QWidgetAction:
        action = QtWidgets.QWidgetAction(self._target_menu)
        self._append(action)
        return action

    def _append(self, action):
        self._last_was_no_separator = True
        self._entries.append(action)


class ContextMenuPlotWidget(pyqtgraph.PlotWidget):
    """PlotWidget with support for dynamically populated context menus."""

    def __init__(self):
        super().__init__()
        self._monkey_patch_context_menu()

    def _monkey_patch_context_menu(self):
        # The pyqtgraph getContextMenus() mechanism by default isn't very useful –
        # returned entries are appended to the menu every time the function is called.
        # This just happens to work out in the most common case where menus are static,
        # as QMenu ignores appended actions that are already part of the menu.
        #
        # To make menus with dynamic entries work, we monkey-patch the ViewBox
        # raiseContextMenu() implementation to create a new QMenu (ViewBoxMenu) instance
        # every time. This is slightly wasteful, but context menus should be created
        # seldomly enough for the slight increase in latency not to matter.
        self.plotItem.getContextMenus = self._get_context_menus

        vb = self.plotItem.getViewBox()
        orig_raise_context_menu = vb.raiseContextMenu

        def raiseContextMenu(ev):
            vb.menu = pyqtgraph.graphicsItems.ViewBox.ViewBoxMenu.ViewBoxMenu(vb)
            return orig_raise_context_menu(ev)

        vb.raiseContextMenu = raiseContextMenu

    def _get_context_menus(self, event):
        builder = ContextMenuBuilder(self.plotItem.getViewBox().menu)
        self.build_context_menu(builder)
        return builder.finish()

    def build_context_menu(self, builder: ContextMenuBuilder) -> None:
        pass


class AlternateMenuPlotWidget(ContextMenuPlotWidget):
    """PlotWidget with context menu for integration with the
    .container_widget.PlotContainerWidget alternate plot switching functionality."""

    alternate_plot_requested = QtCore.pyqtSignal(str)

    def __init__(self, get_alternate_plot_names):
        super().__init__()
        self._get_alternate_plot_names = get_alternate_plot_names

    def build_context_menu(self, builder: ContextMenuBuilder) -> None:
        alternate_plot_names = self._get_alternate_plot_names()
        if len(alternate_plot_names) > 1:
            for name in alternate_plot_names:
                action = builder.append_action("Show " + name)
                action.triggered.connect(lambda *args, name=name: self.
                                         alternate_plot_requested.emit(name))
        builder.ensure_separator()


class SubplotMenuPlotWidget(AlternateMenuPlotWidget):
    """PlotWidget with a context menu to open new windows for subplots (in addition to
    AlternateMenuPlotWidget functionality)."""

    def __init__(self, context, get_alternate_plot_names):
        super().__init__(get_alternate_plot_names)
        self._context = context

        #: Maps subplot names to active plot widgets.
        self.subplots = {}

        #: Maps subscan names to model Root instances.
        self.subscan_roots = {}

    def build_context_menu(self, builder: ContextMenuBuilder) -> None:
        for name in self.subscan_roots.keys():
            action = builder.append_action("Open subscan '{}'".format(name))
            action.triggered.connect(lambda *args, name=name: self.open_subplot(name))
        builder.ensure_separator()
        super().build_context_menu(builder)

    def open_subplot(self, name: str):
        widget = self.subplots.get(name, None)
        if widget is not None:
            widget.show()
            widget.activateWindow()
            return

        import ndscan.plots.container_widgets as containers
        widget = containers.RootWidget(self.subscan_roots[name], self._context)
        self.subplots[name] = widget
        # TODO: Save window geometry.
        widget.resize(600, 400)
        widget.show()

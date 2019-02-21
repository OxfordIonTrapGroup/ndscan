import pyqtgraph
from quamash import QtCore, QtWidgets
from typing import Any, Dict, List, Tuple
from ..utils import eval_param_default

# ColorBrewer-inspired to use for data series (RGBA) and associated fit curves.
SERIES_COLORS = [
    "#d9d9d999", "#fdb46299", "#80b1d399", "#fb807299", "#bebada99", "#ffffb399"
]
FIT_COLORS = [
    "#ff333399", "#fdb462dd", "#80b1d3dd", "#fb8072dd", "#bebadadd", "#ffffb3dd"
]


def extract_scalar_channels(channels):
    data_names = set(
        name for name, spec in channels.items() if spec["type"] in ["int", "float"])

    # Build map from "primary" channel names to error bar names.
    error_bar_names = {}
    for name in data_names:
        spec = channels[name]
        display_hints = spec.get("display_hints", {})
        eb = display_hints.get("error_bar_for", "")
        if not eb:
            continue
        if eb in error_bar_names:
            raise ValueError(
                "More than one set of error bars specified for channel '{}'".format(eb))
        error_bar_names[eb] = name

    data_names -= set(error_bar_names.values())

    # Sort by descending priority and then path (the latter for stable order).
    def priority_key(name):
        return (-channels[name].get("display_hints", {}).get("priority", 0),
                channels[name]["path"])

    data_names = list(data_names)
    data_names.sort(key=priority_key)

    return data_names, error_bar_names


def extract_linked_datasets(param_schema):
    datasets = []
    try:
        # Intercept dataset() to build up list of accessed keys.
        def log_datasets(dataset, default):
            datasets.append(dataset)
            return default

        eval_param_default(param_schema["default"], log_datasets)
    except Exception:
        # Ignore default parsing errors here; the user will get warnings from the
        # experiment dock and on the core device anyway.
        pass
    return datasets


def setup_axis_item(axis_item, axes: List[Tuple[str, str, str, Dict[str, Any]]]):
    def label_html(description, identity_string, color, spec):
        result = ""
        if color is not None:
            # KLUDGE: Truncate alpha, as it renders in weird colors (RGBA vs. ARGB)?
            color = color[:7]
            result += "<span style='color: \"{}\"'>".format(color)
        unit = spec.get("unit", "")
        if unit:
            unit = "/ " + unit + " "
        result += "<b>{} {}</b>".format(description, unit)
        if identity_string:
            result += "<i>({})</i>".format(identity_string)
        if color is not None:
            result += "</span>"
        return result

    axis_item.setLabel("<br>".join(label_html(*a) for a in axes))

    if len(axes) != 1:
        return "", 1.0

    _, _, _, spec = axes[0]
    unit_suffix = ""
    unit = spec.get("unit", "")
    if unit:
        unit_suffix = " " + unit

    data_to_display_scale = 1 / spec["scale"]
    axis_item.setScale(data_to_display_scale)
    axis_item.autoSIPrefix = False
    return unit_suffix, data_to_display_scale


class ContextMenuBuilder:
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
    .container.PlotContainerWidget alternate plot switching functionality."""

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

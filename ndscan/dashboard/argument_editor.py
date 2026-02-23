import asyncio
import logging
from collections import Counter, OrderedDict
from functools import partial
from typing import Any

from artiq.gui.entries import procdesc_to_entry
from artiq.gui.fuzzy_select import FuzzySelectWidget
from artiq.gui.tools import LayoutWidget, WheelFilter
from sipyco import pyon

from .._qt import QtCore, QtGui, QtWidgets
from ..utils import (
    PARAMS_ARG_KEY,
    NoAxesMode,
    eval_param_default,
    shorten_to_unambiguous_suffixes,
)
from .scan_options import list_scan_option_types
from .utils import format_override_identity, load_icon_cached, set_column_resize_mode

logger = logging.getLogger(__name__)


def _try_extract_ndscan_params(
    arguments: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """From a passed dictionary of upstream ARTIQ arguments, extracts the ndscan
    arguments, if there are any.

    :return: A tuple of the (decoded) ndscan parameter schema (``None`` if there
        wasn't one), and the remaining dictionary with that argument (if any) removed.
    """
    if not arguments:
        return None, arguments
    arg = arguments.get(PARAMS_ARG_KEY, None)
    if not arg:
        return None, arguments
    if arg["desc"]["ty"] != "PYONValue":
        return None, arguments

    state = arg.get("state", None)
    params = pyon.decode(state if state else arg["desc"]["default"])
    vanilla_args = arguments.copy()
    del vanilla_args[PARAMS_ARG_KEY]
    return params, vanilla_args


def _update_ndscan_params(arguments, params):
    arguments[PARAMS_ARG_KEY]["state"] = pyon.encode(params)


class ScanOptions:
    """Bundles together the widgets for the scan options section at the bottom of the
    argument editor area.

    This is not itself a QWidget, as the widgets will be added to the QTreeWidget used
    to render the entire editor area.
    """

    def __init__(self, current_scan: dict[str, Any]):
        self.num_repeats_container = QtWidgets.QWidget()
        num_repeats_layout = QtWidgets.QHBoxLayout()
        num_repeats_layout.setContentsMargins(5, 5, 5, 5)
        self.num_repeats_container.setLayout(num_repeats_layout)

        num_repeats_label = QtWidgets.QLabel("Number of repeats of scan: ")
        num_repeats_layout.addWidget(num_repeats_label)
        num_repeats_layout.setStretchFactor(num_repeats_label, 0)

        self.num_repeats_box = QtWidgets.QSpinBox()
        self.num_repeats_box.setMinimum(1)
        # A gratuitous, but hopefully generous restriction
        self.num_repeats_box.setMaximum(2**16)
        self.num_repeats_box.setValue(current_scan.get("num_repeats", 1))
        num_repeats_layout.addWidget(self.num_repeats_box)
        num_repeats_layout.setStretchFactor(self.num_repeats_box, 0)

        self.infinite_repeat_box = QtWidgets.QCheckBox("∞")
        self.infinite_repeat_box.setToolTip("Infinitely repeat scan (~2³¹ times)")
        self.infinite_repeat_box.stateChanged.connect(
            lambda checked: self.num_repeats_box.setEnabled(not checked)
        )
        num_repeats_layout.addWidget(self.infinite_repeat_box)
        num_repeats_layout.setStretchFactor(self.infinite_repeat_box, 0)
        num_repeats_layout.addStretch()

        #

        self.num_repeats_per_point_container = QtWidgets.QWidget()
        num_repeats_per_point_layout = QtWidgets.QHBoxLayout()
        num_repeats_per_point_layout.setContentsMargins(5, 5, 5, 5)
        self.num_repeats_per_point_container.setLayout(num_repeats_per_point_layout)

        num_repeats_per_point_label = QtWidgets.QLabel(
            "Number of consecutive repeats of each point: "
        )
        num_repeats_per_point_layout.addWidget(num_repeats_per_point_label)
        num_repeats_per_point_layout.setStretchFactor(num_repeats_per_point_label, 0)

        self.num_repeats_per_point_box = QtWidgets.QSpinBox()
        self.num_repeats_per_point_box.setMinimum(1)
        # A gratuitous, but hopefully generous restriction
        self.num_repeats_per_point_box.setMaximum(2**16)
        self.num_repeats_per_point_box.setValue(
            current_scan.get("num_repeats_per_point", 1)
        )
        num_repeats_per_point_layout.addWidget(self.num_repeats_per_point_box)
        num_repeats_per_point_layout.setStretchFactor(self.num_repeats_per_point_box, 0)
        num_repeats_per_point_layout.addStretch()

        #

        self.no_axis_container = QtWidgets.QWidget()
        no_axis_layout = QtWidgets.QHBoxLayout()
        no_axis_layout.setContentsMargins(5, 5, 5, 5)
        self.no_axis_container.setLayout(no_axis_layout)

        no_axis_label = QtWidgets.QLabel("No-axis mode: ")
        no_axis_layout.addWidget(no_axis_label)
        no_axis_layout.setStretchFactor(no_axis_label, 0)

        self.no_axes_box = QtWidgets.QComboBox()
        self.no_axes_box.addItems([m.value for m in NoAxesMode])
        mode = NoAxesMode[current_scan.get("no_axes_mode", "single")]
        self.no_axes_box.setCurrentText(mode.value)
        no_axis_layout.addWidget(self.no_axes_box)
        no_axis_layout.setStretchFactor(self.no_axes_box, 0)

        no_axis_layout.addStretch()

        #

        self.randomise_globally_container = QtWidgets.QWidget()
        randomise_globally_layout = QtWidgets.QHBoxLayout()
        randomise_globally_layout.setContentsMargins(5, 5, 5, 5)
        self.randomise_globally_container.setLayout(randomise_globally_layout)

        randomise_globally_label = QtWidgets.QLabel(
            "Randomise point order across axes: "
        )
        randomise_globally_layout.addWidget(randomise_globally_label)
        randomise_globally_layout.setStretchFactor(randomise_globally_label, 0)

        self.randomise_globally_box = QtWidgets.QCheckBox()
        self.randomise_globally_box.setChecked(
            current_scan.get("randomise_order_globally", False)
        )
        randomise_globally_layout.addWidget(self.randomise_globally_box)
        randomise_globally_layout.setStretchFactor(self.randomise_globally_box, 1)

        #

        self.skip_persistently_failing_container = QtWidgets.QWidget()
        skip_persistently_failing_layout = QtWidgets.QHBoxLayout()
        skip_persistently_failing_layout.setContentsMargins(5, 5, 5, 5)
        self.skip_persistently_failing_container.setLayout(
            skip_persistently_failing_layout
        )

        skip_persistently_failing_label = QtWidgets.QLabel(
            "Skip point if transitory errors persist: "
        )
        skip_persistently_failing_layout.addWidget(skip_persistently_failing_label)
        skip_persistently_failing_layout.setStretchFactor(
            skip_persistently_failing_label, 0
        )

        self.skip_persistently_failing_box = QtWidgets.QCheckBox()
        self.skip_persistently_failing_box.setChecked(
            current_scan.get("skip_on_persistent_transitory_error", False)
        )
        self.skip_persistently_failing_box.setToolTip(
            "If more than the configured limit of transitory errors occur for a "
            + "single scan point, skip it and attempt the next point instead of "
            + "terminating the entire scan. Does not affect regular exceptions."
        )
        skip_persistently_failing_layout.addWidget(self.skip_persistently_failing_box)
        skip_persistently_failing_layout.setStretchFactor(
            self.skip_persistently_failing_box, 1
        )

    def get_widgets(self) -> list[QtWidgets.QWidget]:
        return [
            self.num_repeats_container,
            self.num_repeats_per_point_container,
            self.no_axis_container,
            self.randomise_globally_container,
            self.skip_persistently_failing_container,
        ]

    def write_to_params(self, params: dict[str, Any]) -> None:
        scan = params.setdefault("scan", {})
        # For simplicity, we realise infinite repeats as int32.max, as this should take
        # many days even for very fast single-point scans, and in either case would
        # produce many GiB of data, to where it would be more practical to just schedule
        # multiple experiments if for whatever reason more repeats were required.
        scan["num_repeats"] = (
            2**31 - 1
            if self.infinite_repeat_box.isChecked()
            else self.num_repeats_box.value()
        )
        scan["num_repeats_per_point"] = self.num_repeats_per_point_box.value()
        scan["no_axes_mode"] = NoAxesMode(self.no_axes_box.currentText()).name
        scan["randomise_order_globally"] = self.randomise_globally_box.isChecked()
        scan["skip_on_persistent_transitory_error"] = (
            self.skip_persistently_failing_box.isChecked()
        )


class ArgumentEditor(QtWidgets.QTreeWidget):
    def __init__(self, manager, dock, expurl):
        super().__init__()

        self.manager = manager
        self.dock = expurl
        self.expurl = expurl

        self.setColumnCount(3)
        self.header().setStretchLastSection(False)

        set_column_resize_mode(
            self, 0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        set_column_resize_mode(self, 1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        set_column_resize_mode(
            self, 2, QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        self.header().setVisible(False)
        self.setSelectionMode(self.SelectionMode.NoSelection)
        self.setFocusPolicy(QtCore.Qt.FocusPolicy.NoFocus)
        self.setHorizontalScrollMode(self.ScrollMode.ScrollPerPixel)
        self.setVerticalScrollMode(self.ScrollMode.ScrollPerPixel)

        self.setStyleSheet(
            "QTreeWidget {background: "
            + self.palette().midlight().color().name()
            + " ;}"
        )

        self.viewport().installEventFilter(WheelFilter(self.viewport()))

        self._bg_gradient = QtGui.QLinearGradient(
            0, 0, 0, QtGui.QFontMetrics(self.font()).lineSpacing()
        )
        self._bg_gradient.setColorAt(0, self.palette().base().color())
        self._bg_gradient.setColorAt(1, self.palette().midlight().color())

        self._save_timer = QtCore.QTimer(self)
        self._save_timer.timeout.connect(self._save_to_argument)

        self._param_entries = OrderedDict()
        self._groups = dict()
        self._arg_to_widgets = dict()
        self._override_items = dict()

        self._add_override_icon = load_icon_cached("list-add-32.png")
        self._remove_override_icon = load_icon_cached("list-remove-32.png")
        self._default_value_icon = self.style().standardIcon(
            QtWidgets.QStyle.StandardPixmap.SP_BrowserReload
        )
        self._disable_scans_icon = self.style().standardIcon(
            QtWidgets.QStyle.StandardPixmap.SP_DialogResetButton
        )

        self._arguments = self.manager.get_submission_arguments(self.expurl)
        ndscan_params, vanilla_args = _try_extract_ndscan_params(self._arguments)

        if not ndscan_params:
            self.addTopLevelItem(
                QtWidgets.QTreeWidgetItem(["Error: Parameter metadata not found."])
            )
        else:
            self._ndscan_params = ndscan_params

            self.override_separator = None

            self._build_shortened_fqns()

            self.scan_options = None
            if "scan" in ndscan_params:
                self.scan_options = ScanOptions(ndscan_params["scan"])

            for fqn, path in ndscan_params["always_shown"]:
                self._make_param_items(fqn, path, True)

            for name, argument in vanilla_args.items():
                self._make_vanilla_argument_item(name, argument)

            self.override_separator = self._make_line_separator()

            self._make_add_override_prompt_item()
            self._set_override_line_idle()

            for ax in ndscan_params.get("scan", {}).get("axes", []):
                self._make_override_item(ax["fqn"], ax["path"])

            for fqn, overrides in ndscan_params["overrides"].items():
                for o in overrides:
                    self._make_override_item(fqn, o["path"])

            self._make_line_separator()

            if self.scan_options:
                scan_options_group = self._make_group_header_item("Scan options")
                self.addTopLevelItem(scan_options_group)
                for widget in self.scan_options.get_widgets():
                    twi = QtWidgets.QTreeWidgetItem()
                    scan_options_group.addChild(twi)
                    self.setItemWidget(twi, 1, widget)

        buttons_item = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(buttons_item)
        buttons_item.setFirstColumnSpanned(True)
        recompute_arguments = QtWidgets.QPushButton("Recompute all arguments")
        recompute_arguments.setIcon(self._default_value_icon)
        recompute_arguments.clicked.connect(dock._recompute_arguments_clicked)

        load_hdf5 = QtWidgets.QPushButton("Load HDF5")
        load_hdf5.setIcon(
            QtWidgets.QApplication.style().standardIcon(
                QtWidgets.QStyle.StandardPixmap.SP_DialogOpenButton
            )
        )
        load_hdf5.clicked.connect(dock._load_hdf5_clicked)

        disable_scans = QtWidgets.QPushButton("Disable all scans")
        disable_scans.setIcon(self._disable_scans_icon)
        disable_scans.clicked.connect(self.disable_all_scans)
        disable_scans.setShortcut("Ctrl+R")

        buttons = LayoutWidget()
        buttons.addWidget(recompute_arguments, col=1)
        buttons.addWidget(load_hdf5, col=2)
        buttons.addWidget(disable_scans, col=3)
        buttons.layout.setColumnStretch(0, 1)
        buttons.layout.setColumnStretch(1, 0)
        buttons.layout.setColumnStretch(2, 0)
        buttons.layout.setColumnStretch(3, 0)
        buttons.layout.setColumnStretch(4, 1)
        buttons.layout.setContentsMargins(3, 6, 3, 6)
        self.setItemWidget(buttons_item, 0, buttons)

    def save_state(self):
        expanded = []
        for k, v in self._groups.items():
            if v.isExpanded():
                expanded.append(k)
        return {"expanded": expanded, "scroll": self.verticalScrollBar().value()}

    def restore_state(self, state):
        for e in state["expanded"]:
            try:
                self._groups[e].setExpanded(True)
            except KeyError:
                pass
        self.verticalScrollBar().setValue(state["scroll"])

    def about_to_submit(self):
        self._save_to_argument()

    def about_to_close(self):
        self._save_to_argument()

    def disable_all_scans(self):
        for entry in self._param_entries.values():
            entry.disable_scan()

    def _make_param_items(self, fqn, path, show_always, insert_at_idx=-1):
        if (fqn, path) in self._param_entries:
            return
        schema = self._schema_for_fqn(fqn)

        added_item_count = 0

        def add_item(widget_item):
            nonlocal added_item_count
            group = schema.get("group", None)
            if not group:
                if insert_at_idx == -1:
                    self.addTopLevelItem(widget_item)
                else:
                    self.insertTopLevelItem(
                        insert_at_idx + added_item_count, widget_item
                    )
                added_item_count += 1
            else:
                self._ensure_group_widget(group).addChild(widget_item)

        #
        # First line: fqn@path.
        #

        id_string = self._param_display_name(fqn, path)
        id_item = QtWidgets.QTreeWidgetItem([id_string])
        add_item(id_item)
        for col in range(3):
            id_item.setBackground(col, self._bg_gradient)
        id_item.setFirstColumnSpanned(True)
        id_item.setForeground(0, self.palette().mid())

        #
        # Second line: Description, override entry widgets, reset/remove buttons.
        #

        main_item = QtWidgets.QTreeWidgetItem()
        add_item(main_item)

        # Render description in bold.
        label_container = LayoutWidget()
        label_container.layout.setContentsMargins(3, 1, 6, 6)

        label = QtWidgets.QLabel(schema["description"])
        font = label.font()
        font.setBold(True)
        label.setFont(font)
        label_container.addWidget(label)

        # For whatever reason, the auto-sized column is not wide enough to display the
        # whole label if displayed through a widget – whether through an extra
        # LayoutWidget or the QLabel directly. This does not occur when passing a string
        # directly to the QTreeWidgetItem constructor, but we cannot do that here, as
        # we need to apply an extra bottom margin here to keep the baseline assignment
        # with the other widgets. This only happens for the first column, and appears
        # to be a Qt bug (incorrect handling of the group expand arrows?). The fixed
        # extra horizontal margin was just determined visually and might be brittle
        # across platforms/…; a proper fix would be desirable.
        label_container.setMinimumSize(label.sizeHint() + QtCore.QSize(28, 0))

        self.setItemWidget(main_item, 0, label_container)

        entry = self._make_override_entry(fqn, path)
        entry.read_from_params(self._ndscan_params, self.manager.datasets)
        entry.layout.setContentsMargins(3, 1, 3, 6)

        entry.value_changed.connect(self._set_save_timer)
        self._param_entries[(fqn, path)] = entry
        self.setItemWidget(main_item, 1, entry)

        buttons = LayoutWidget()
        buttons.layout.setContentsMargins(3, 1, 3, 6)

        reset_default = QtWidgets.QToolButton()
        reset_default.setToolTip("Reset parameter to default value")
        reset_default.setIcon(
            QtWidgets.QApplication.style().standardIcon(
                QtWidgets.QStyle.StandardPixmap.SP_BrowserReload
            )
        )
        reset_default.clicked.connect(partial(self._reset_entry_to_default, fqn, path))
        buttons.addWidget(reset_default, col=0)

        remove_override = QtWidgets.QToolButton()
        remove_override.setIcon(self._remove_override_icon)
        remove_override.setToolTip("Remove this parameter override")
        remove_override.clicked.connect(partial(self._remove_override, fqn, path))
        buttons.addWidget(remove_override, col=1)

        self.setItemWidget(main_item, 2, buttons)

        if show_always:
            sp = remove_override.sizePolicy()
            sp.setRetainSizeWhenHidden(True)
            remove_override.setSizePolicy(sp)
            remove_override.setVisible(False)

        return id_item, main_item

    def _make_vanilla_argument_item(self, name, argument):
        if name in self._arg_to_widgets:
            logger.warning("Argument with name '%s' already exists, skipping.", name)
            return
        widgets = dict()
        self._arg_to_widgets[name] = widgets

        entry = procdesc_to_entry(argument["desc"])(argument)
        if entry.layout():
            # KLUDGE: For EnumerationEntry, avoid extra margins that misalign the
            # dropdown box with the ndscan variant.
            entry.layout().setContentsMargins(0, 0, 0, 0)
        entry.setContentsMargins(0, 3, 0, 3)
        widget_item = QtWidgets.QTreeWidgetItem([name])

        if argument["tooltip"]:
            widget_item.setToolTip(1, argument["tooltip"])
        widgets["entry"] = entry
        widgets["widget_item"] = widget_item

        for col in range(3):
            widget_item.setBackground(col, self._bg_gradient)
        font = widget_item.font(0)
        font.setBold(True)
        widget_item.setFont(0, font)

        if argument["group"] is None:
            self.addTopLevelItem(widget_item)
        else:
            self._ensure_group_widget(argument["group"]).addChild(widget_item)
        fix_layout = LayoutWidget()
        fix_layout.layout.setContentsMargins(3, 3, 3, 3)
        widgets["fix_layout"] = fix_layout
        fix_layout.addWidget(entry)
        self.setItemWidget(widget_item, 1, fix_layout)

        buttons = LayoutWidget()
        buttons.layout.setContentsMargins(3, 3, 3, 3)

        recompute_argument = QtWidgets.QToolButton()
        recompute_argument.setToolTip(
            "Re-run the experiment's build method and take the default value"
        )
        recompute_argument.setIcon(self._default_value_icon)
        recompute_argument.clicked.connect(
            partial(self._recompute_vanilla_argument_clicked, name)
        )
        buttons.addWidget(recompute_argument)

        # Even though there isn't actually a widget in the second column, this makes it
        # take up the slack, such that the recompute button lines up with the ndscan
        # override ones.
        buttons.layout.setColumnStretch(0, 0)
        buttons.layout.setColumnStretch(1, 1)

        self.setItemWidget(widget_item, 2, buttons)

    def _make_line_separator(self):
        f = QtWidgets.QFrame(self)
        f.setMinimumHeight(15)
        f.setFrameShape(QtWidgets.QFrame.Shape.HLine)
        f.setFrameShadow(QtWidgets.QFrame.Shadow.Sunken)
        f.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Preferred,
        )

        wi = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(wi)
        wi.setFirstColumnSpanned(True)
        self.setItemWidget(wi, 1, f)
        return wi

    def _make_override_item(self, fqn, path):
        items = self._make_param_items(
            fqn, path, False, self.indexOfTopLevelItem(self._override_prompt_item)
        )
        self._override_items[(fqn, path)] = items
        self._set_save_timer()

        # Make sure layout is updated to accommodate new row; without this, the
        # new item and the add prompt button overlap on Qt 5.6.2/Win64 until
        # the dock is resized for the first time.
        geom = self.geometry()
        self.resize(geom.width(), geom.height())

    def _make_add_override_prompt_item(self):
        self._override_prompt_item = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(self._override_prompt_item)

        # Layout to display button/prompt label, depending on which one is active.
        left = LayoutWidget()
        left.layout.setContentsMargins(3, 3, 3, 3)

        self._add_override_button = QtWidgets.QToolButton()
        self._add_override_button.setIcon(self._add_override_icon)
        self._add_override_button.clicked.connect(self._set_override_line_active)
        self._add_override_button.setShortcut("Ctrl+T")
        left.addWidget(self._add_override_button, 0, 0)

        self._add_override_prompt_label = QtWidgets.QLabel("Add parameter:")
        left.addWidget(self._add_override_prompt_label, 0, 0)

        left.layout.setColumnStretch(0, 0)
        left.layout.setColumnStretch(1, 1)
        self.setItemWidget(self._override_prompt_item, 0, left)

        prompt = LayoutWidget()
        self._add_override_prompt_box = FuzzySelectWidget([])
        self._add_override_prompt_box.finished.connect(
            lambda a: self._make_override_item(*self._param_choice_map[a])
        )
        self._add_override_prompt_box.aborted.connect(self._set_override_line_idle)
        prompt.addWidget(self._add_override_prompt_box)
        self.setItemWidget(self._override_prompt_item, 1, prompt)

    def _set_override_line_idle(self):
        self._add_override_button.setEnabled(True)
        self._add_override_button.setVisible(True)
        self._add_override_prompt_label.setVisible(False)
        self._add_override_prompt_box.setVisible(False)

    def _set_override_line_active(self):
        self._update_param_choice_map()
        self._add_override_prompt_box.set_choices(
            [(s, 0) for s in self._param_choice_map.keys()]
        )

        self._add_override_button.setEnabled(False)
        self._add_override_button.setVisible(False)
        self._add_override_prompt_label.setVisible(True)
        self._add_override_prompt_box.setVisible(True)

        # TODO: See whether I can't get focus proxies to work.
        self._add_override_prompt_box.line_edit.setFocus()

    def _make_group_header_item(self, name):
        group = QtWidgets.QTreeWidgetItem([name])
        for col in range(3):
            group.setBackground(col, self.palette().mid())
            group.setForeground(col, self.palette().brightText())
            font = group.font(col)
            font.setBold(True)
            group.setFont(col, font)
        return group

    def _ensure_group_widget(self, name):
        if name in self._groups:
            return self._groups[name]
        group = self._make_group_header_item(name)
        if self.override_separator:
            self.insertTopLevelItem(
                self.indexOfTopLevelItem(self.override_separator), group
            )
        else:
            self.addTopLevelItem(group)
        self._groups[name] = group
        return group

    def _recompute_vanilla_argument_clicked(self, name):
        asyncio.ensure_future(self._recompute_vanilla_argument(name))

    async def _recompute_vanilla_argument(self, name):
        try:
            class_desc, _ui_name = await self.manager.compute_expdesc(self.expurl)
            arginfo = class_desc["arginfo"]
        except Exception:
            logger.error(
                "Could not recompute argument '%s' of '%s'",
                name,
                self.expurl,
                exc_info=True,
            )
            return
        argument = self.manager.get_submission_arguments(self.expurl)[name]

        procdesc = arginfo[name][0]
        state = procdesc_to_entry(procdesc).default_state(procdesc)
        argument["desc"] = procdesc
        argument["state"] = state

        widgets = self._arg_to_widgets[name]
        widgets["entry"].deleteLater()
        widgets["entry"] = procdesc_to_entry(procdesc)(argument)
        widgets["fix_layout"].deleteLater()
        widgets["fix_layout"] = LayoutWidget()
        widgets["fix_layout"].addWidget(widgets["entry"])
        self.setItemWidget(widgets["widget_item"], 1, widgets["fix_layout"])
        self.updateGeometries()

        # apply_colors() was introduced in m-labs/artiq@52c07a2b145b (during ARTIQ 9
        # development); so while the rest of ndscan is generally backwards-compatible,
        # only call it if present.
        if hasattr(self.dock, "apply_colors"):
            self.dock.apply_colors()

    def _reset_entry_to_default(self, fqn, path):
        self._param_entries[(fqn, path)].read_from_params({}, self.manager.datasets)

    def _remove_override(self, fqn, path):
        items = self._override_items[(fqn, path)]
        for item in items:
            idx = self.indexOfTopLevelItem(item)
            self.takeTopLevelItem(idx)
        del self._param_entries[(fqn, path)]
        del self._override_items[(fqn, path)]
        self._set_save_timer()

    def _update_param_choice_map(self):
        self._param_choice_map = dict()

        def add(fqn, path):
            # Skip params already displayed.
            if (fqn, path) in self._param_entries:
                return
            schema = self._schema_for_fqn(fqn)
            display_string = "{} – {}".format(
                self._param_display_name(fqn, path), schema["description"]
            )
            self._param_choice_map[display_string] = (fqn, path)

        fqn_occurences = Counter()
        for path, fqns in self._ndscan_params["instances"].items():
            for fqn in fqns:
                add(fqn, path)
                fqn_occurences[fqn] += 1

        # TODO: Offer non-global wildcards for parameters used in multiple hierarchies.
        for fqn, count in fqn_occurences.items():
            if count > 1:
                add(fqn, "*")

    def _build_shortened_fqns(self):
        self.shortened_fqns = shorten_to_unambiguous_suffixes(
            self._ndscan_params["schemata"].keys(),
            lambda fqn, n: ".".join(fqn.split(".")[-(n + 1) :]),
        )

    def _param_display_name(self, fqn, path):
        if not path:
            path = "/"
        return self.shortened_fqns[fqn] + "@" + path

    def _schema_for_fqn(self, fqn):
        return self._ndscan_params["schemata"][fqn]

    def _set_save_timer(self):
        self._save_timer.start(500)

    def _save_to_argument(self):
        # Stop timer if it is still running.
        self._save_timer.stop()

        # Reset previous overrides/scan axes, repopulate with currently active ones.
        self._ndscan_params.setdefault("scan", {})["axes"] = []
        self._ndscan_params["overrides"] = {}
        for item in self._param_entries.values():
            item.write_to_params(self._ndscan_params)

        if self.scan_options is None:
            # Not actually a scannable experiment – delete the scan metadata key, which
            # we've set above to keep code straightforward.
            del self._ndscan_params["scan"]
        else:
            # Store scan parameters.
            self.scan_options.write_to_params(self._ndscan_params)

        _update_ndscan_params(self._arguments, self._ndscan_params)

    def _make_override_entry(self, fqn, path):
        schema = self._schema_for_fqn(fqn)

        is_scannable = (self.scan_options is not None) and schema.get("spec", {}).get(
            "is_scannable", True
        )
        options = list_scan_option_types(schema["type"], is_scannable)
        return OverrideEntry(options, schema, path)

    def apply_color(self, *args, **kwargs):
        # TODO: In ARTIQ 9, the ability to colour-code entire argument editor windows
        # was introduced. At the time of writing, this is unreleased and still
        # undergoing API changes; once it stabilises, should implement this (e.g. on top
        # of the newly factored-out artiq.gui.entries.EntryTreeWidget).
        pass


class OverrideEntry(LayoutWidget):
    value_changed = QtCore.pyqtSignal()

    def __init__(self, option_classes, schema, path, *args):
        super().__init__(*args)

        self.schema = schema
        self.path = path

        self.scan_type = QtWidgets.QComboBox()
        self.addWidget(self.scan_type, col=0)

        self.widget_stack = QtWidgets.QStackedWidget()

        # The non-scan option should be on the top.
        assert next(iter(option_classes.keys())) == "Fixed"
        if len(option_classes) == 1:
            self.scan_type.setEnabled(False)
        self.current_option_idx = 0

        self.options = []
        for name, option_cls in option_classes.items():
            self.scan_type.addItem(name)

            option = option_cls(self.schema, self.path)
            option.value_changed.connect(self.value_changed)
            container = QtWidgets.QWidget()
            layout = QtWidgets.QHBoxLayout()
            # For tight spacing, let other parts of entry line dominate margins.
            layout.setContentsMargins(0, 0, 0, 0)
            option.build_ui(layout)
            container.setLayout(layout)

            self.widget_stack.addWidget(container)
            self.options.append(option)
        self.scan_type.currentIndexChanged.connect(self._current_index_changed)
        self.addWidget(self.widget_stack, col=1)
        self.sync_values = {}

    def read_from_params(self, params: dict, manager_datasets) -> None:
        id_for_log = format_override_identity(self.schema["fqn"], self.path)

        # Check if this parameter is part of the scan axes
        for axis in params.get("scan", {}).get("axes", []):
            if axis["fqn"] == self.schema["fqn"] and axis["path"] == self.path:
                for idx, option in enumerate(self.options):
                    if option.attempt_read_from_axis(axis):
                        self.current_option_idx = idx
                        self._current_index_changed(idx)
                        self.scan_type.setCurrentIndex(idx)
                        return
                logger.warning(f"Failed to read scan params for {id_for_log}")

        for o in params.get("overrides", {}).get(self.schema["fqn"], []):
            if o["path"] == self.path:
                self._set_fixed_value(o["value"])
                return
        try:

            def get_dataset(key, default=None):
                try:
                    bs = manager_datasets.backing_store
                except AttributeError:
                    logger.error(
                        "Datasets still synchronising with master, "
                        + "cannot access '%s'",
                        key,
                    )
                    bs = {}
                try:
                    return bs[key][1]
                except KeyError:
                    if default is None:
                        raise KeyError(
                            f"Could not read dataset '{key}', but no "
                            + "fallback default value given"
                        ) from None
                    return default

            value = eval_param_default(self.schema["default"], get_dataset)
        except Exception as e:
            logger.error(
                'Failed to evaluate defaults string "%s" for %s: %s',
                self.schema["default"],
                id_for_log,
                e,
            )
            value = None
        self._set_fixed_value(value)
        self.disable_scan()

    def write_to_params(self, params: dict) -> None:
        self.options[self.scan_type.currentIndex()].write_to_params(params)

    def disable_scan(self) -> None:
        self.scan_type.setCurrentIndex(0)

    def _set_fixed_value(self, value) -> None:
        self.options[0].set_value(value)
        self.options[0].write_sync_values(self.sync_values)

    def _current_index_changed(self, new_idx) -> None:
        self.options[self.current_option_idx].write_sync_values(self.sync_values)
        self.options[new_idx].read_sync_values(self.sync_values)
        self.widget_stack.setCurrentIndex(new_idx)
        self.current_option_idx = new_idx

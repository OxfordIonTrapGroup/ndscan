import asyncio
from collections import Counter, OrderedDict
from functools import partial
import logging
import os
from typing import Any, Dict, List, Optional, Tuple
from PyQt5 import QtCore, QtGui, QtWidgets
from artiq.dashboard.experiments import _WheelFilter
from artiq.gui.entries import procdesc_to_entry
from artiq.gui.fuzzy_select import FuzzySelectWidget
from artiq.gui.scientific_spinbox import ScientificSpinBox
from artiq.gui.tools import LayoutWidget, disable_scroll_wheel
from sipyco import pyon

from ..utils import (NoAxesMode, PARAMS_ARG_KEY, eval_param_default,
                     shorten_to_unambiguous_suffixes)

logger = logging.getLogger(__name__)


def _try_extract_ndscan_params(
        arguments: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
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
    def __init__(self, current_scan: Dict[str, Any]):
        self.num_repeats_container = QtWidgets.QWidget()
        num_repeats_layout = QtWidgets.QHBoxLayout()
        self.num_repeats_container.setLayout(num_repeats_layout)

        num_repeats_label = QtWidgets.QLabel("Number of repeats: ")
        num_repeats_layout.addWidget(num_repeats_label)
        num_repeats_layout.setStretchFactor(num_repeats_label, 0)

        self.num_repeats_box = QtWidgets.QSpinBox()
        self.num_repeats_box.setMinimum(1)
        # A gratuitous, but hopefully generous restriction
        self.num_repeats_box.setMaximum(2**16)
        self.num_repeats_box.setValue(current_scan.get("num_repeats", 1))
        num_repeats_layout.addWidget(self.num_repeats_box)
        num_repeats_layout.setStretchFactor(self.num_repeats_box, 0)

        num_repeats_layout.addStretch()

        #

        self.no_axis_container = QtWidgets.QWidget()
        no_axis_layout = QtWidgets.QHBoxLayout()
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
        self.randomise_globally_container.setLayout(randomise_globally_layout)

        randomise_globally_label = QtWidgets.QLabel(
            "Randomise point order across axes: ")
        randomise_globally_layout.addWidget(randomise_globally_label)
        randomise_globally_layout.setStretchFactor(randomise_globally_label, 0)

        self.randomise_globally_box = QtWidgets.QCheckBox()
        self.randomise_globally_box.setChecked(
            current_scan.get("randomise_order_globally", False))
        randomise_globally_layout.addWidget(self.randomise_globally_box)
        randomise_globally_layout.setStretchFactor(self.randomise_globally_box, 1)

    def get_widgets(self) -> List[QtWidgets.QWidget]:
        return [
            self.num_repeats_container, self.no_axis_container,
            self.randomise_globally_container
        ]

    def write_to_params(self, params: Dict[str, Any]) -> None:
        scan = params.setdefault("scan", {})
        scan["num_repeats"] = self.num_repeats_box.value()
        scan["no_axes_mode"] = NoAxesMode(self.no_axes_box.currentText()).name
        scan["randomise_order_globally"] = self.randomise_globally_box.isChecked()


class ArgumentEditor(QtWidgets.QTreeWidget):
    def __init__(self, manager, dock, expurl):
        super().__init__()

        self.manager = manager
        self.expurl = expurl

        self.setColumnCount(3)
        self.header().setStretchLastSection(False)
        if hasattr(self.header(), "setSectionResizeMode"):
            set_resize_mode = self.header().setSectionResizeMode
        else:
            set_resize_mode = self.header().setResizeMode
        set_resize_mode(0, QtWidgets.QHeaderView.ResizeToContents)
        set_resize_mode(1, QtWidgets.QHeaderView.Stretch)
        set_resize_mode(2, QtWidgets.QHeaderView.ResizeToContents)
        self.header().setVisible(False)
        self.setSelectionMode(self.NoSelection)
        self.setHorizontalScrollMode(self.ScrollPerPixel)
        self.setVerticalScrollMode(self.ScrollPerPixel)

        self.setStyleSheet("QTreeWidget {background: " +
                           self.palette().midlight().color().name() + " ;}")

        self.viewport().installEventFilter(_WheelFilter(self.viewport()))

        self._bg_gradient = QtGui.QLinearGradient(
            0, 0, 0,
            QtGui.QFontMetrics(self.font()).lineSpacing())
        self._bg_gradient.setColorAt(0, self.palette().base().color())
        self._bg_gradient.setColorAt(1, self.palette().midlight().color())

        self._save_timer = QtCore.QTimer(self)
        self._save_timer.timeout.connect(self._save_to_argument)

        self._param_entries = OrderedDict()
        self._groups = dict()
        self._arg_to_widgets = dict()
        self._override_items = dict()

        def icon_path(name):
            return os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons",
                                name)

        self._add_override_icon = QtGui.QIcon(icon_path("list-add-32.png"))
        self._remove_override_icon = QtGui.QIcon(icon_path("list-remove-32.png"))
        self._randomise_scan_icon = QtGui.QIcon(
            icon_path("media-playlist-shuffle-32.svg"))
        self._default_value_icon = self.style().standardIcon(
            QtWidgets.QStyle.SP_BrowserReload)
        self._disable_scans_icon = self.style().standardIcon(
            QtWidgets.QStyle.SP_DialogResetButton)

        self._arguments = self.manager.get_submission_arguments(self.expurl)
        ndscan_params, vanilla_args = _try_extract_ndscan_params(self._arguments)

        if not ndscan_params:
            self.addTopLevelItem(
                QtWidgets.QTreeWidgetItem(["Error: Parameter metadata not found."]))
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
        load_hdf5.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_DialogOpenButton))
        load_hdf5.clicked.connect(dock._load_hdf5_clicked)

        disable_scans = QtWidgets.QPushButton("Disable all scans")
        disable_scans.setIcon(self._disable_scans_icon)
        disable_scans.clicked.connect(self.disable_all_scans)
        disable_scans.setShortcut(QtCore.Qt.CTRL + QtCore.Qt.Key_R)

        buttons = LayoutWidget()
        buttons.addWidget(recompute_arguments, col=1)
        buttons.addWidget(load_hdf5, col=2)
        buttons.addWidget(disable_scans, col=3)
        buttons.layout.setColumnStretch(0, 1)
        buttons.layout.setColumnStretch(1, 0)
        buttons.layout.setColumnStretch(2, 0)
        buttons.layout.setColumnStretch(3, 0)
        buttons.layout.setColumnStretch(4, 1)
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
                    self.insertTopLevelItem(insert_at_idx + added_item_count,
                                            widget_item)
                added_item_count += 1
            else:
                self._ensure_group_widget(group).addChild(widget_item)

        id_string = self._param_display_name(fqn, path)

        id_item = QtWidgets.QTreeWidgetItem([id_string])
        add_item(id_item)
        for col in range(3):
            id_item.setBackground(col, self._bg_gradient)
        id_item.setFirstColumnSpanned(True)
        id_item.setForeground(0, self.palette().mid())

        main_item = QtWidgets.QTreeWidgetItem([schema["description"]])
        add_item(main_item)

        # Render description in bold.
        font = main_item.font(0)
        font.setBold(True)
        main_item.setFont(0, font)

        entry = self._make_override_entry(fqn, path)
        entry.read_from_params(self._ndscan_params, self.manager.datasets)

        entry.value_changed.connect(self._set_save_timer)
        self._param_entries[(fqn, path)] = entry
        self.setItemWidget(main_item, 1, entry)

        buttons = LayoutWidget()

        reset_default = QtWidgets.QToolButton()
        reset_default.setToolTip("Reset parameter to default value")
        reset_default.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_BrowserReload))
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
        widgets["fix_layout"] = fix_layout
        fix_layout.addWidget(entry)
        self.setItemWidget(widget_item, 1, fix_layout)

        buttons = LayoutWidget()

        recompute_argument = QtWidgets.QToolButton()
        recompute_argument.setToolTip("Re-run the experiment's build "
                                      "method and take the default value")
        recompute_argument.setIcon(self._default_value_icon)
        recompute_argument.clicked.connect(
            partial(self._recompute_vanilla_argument_clicked, name))
        buttons.addWidget(recompute_argument)

        buttons.layout.setColumnStretch(0, 0)
        buttons.layout.setColumnStretch(1, 1)

        self.setItemWidget(widget_item, 2, buttons)

    def _make_line_separator(self):
        f = QtWidgets.QFrame(self)
        f.setMinimumHeight(15)
        f.setFrameShape(QtWidgets.QFrame.HLine)
        f.setFrameShadow(QtWidgets.QFrame.Sunken)
        f.setSizePolicy(QtWidgets.QSizePolicy.Expanding,
                        QtWidgets.QSizePolicy.Preferred)

        wi = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(wi)
        wi.setFirstColumnSpanned(True)
        self.setItemWidget(wi, 1, f)
        return wi

    def _make_override_item(self, fqn, path):
        items = self._make_param_items(
            fqn, path, False, self.indexOfTopLevelItem(self._override_prompt_item))
        self._override_items[(fqn, path)] = items
        self._set_save_timer()

        # Make sure layout is updated to accomodate new row; without this, the
        # new item and the add prompt button overlap on Qt 5.6.2/Win64 until
        # the dock is resized for the first time.
        geom = self.geometry()
        self.resize(geom.width(), geom.height())

    def _make_add_override_prompt_item(self):
        self._override_prompt_item = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(self._override_prompt_item)

        # Layout to display button/prompt label, depending on which one is active.
        left = LayoutWidget()

        self._add_override_button = QtWidgets.QToolButton()
        self._add_override_button.setIcon(self._add_override_icon)
        self._add_override_button.clicked.connect(self._set_override_line_active)
        self._add_override_button.setShortcut(QtCore.Qt.CTRL + QtCore.Qt.Key_T)
        left.addWidget(self._add_override_button, 0, 0)

        self._add_override_prompt_label = QtWidgets.QLabel("Add parameter:")
        left.addWidget(self._add_override_prompt_label, 0, 0)

        left.layout.setColumnStretch(0, 0)
        left.layout.setColumnStretch(1, 1)
        self.setItemWidget(self._override_prompt_item, 0, left)

        prompt = LayoutWidget()
        self._add_override_prompt_box = FuzzySelectWidget([])
        self._add_override_prompt_box.finished.connect(
            lambda a: self._make_override_item(*self._param_choice_map[a]))
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
        self._add_override_prompt_box.set_choices([
            (s, 0) for s in self._param_choice_map.keys()
        ])

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
            self.insertTopLevelItem(self.indexOfTopLevelItem(self.override_separator),
                                    group)
        else:
            self.addTopLevelItem(group)
        self._groups[name] = group
        return group

    def _recompute_vanilla_argument_clicked(self, name):
        asyncio.ensure_future(self._recompute_vanilla_argument(name))

    async def _recompute_vanilla_argument(self, name):
        try:
            arginfo, _ = await self.manager.examine_arginfo(self.expurl)
        except Exception:
            logger.error("Could not recompute argument '%s' of '%s'",
                         name,
                         self.expurl,
                         exc_info=True)
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
            display_string = "{} – {}".format(self._param_display_name(fqn, path),
                                              schema["description"])
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
            lambda fqn, n: ".".join(fqn.split(".")[-(n + 1):]))

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

        entry_class = FloatOverrideEntry
        if schema["type"] == "string":
            entry_class = StringOverrideEntry
        # TODO: Properly handle int, add errors (or default to PYON value).

        is_scannable = ((self.scan_options is not None)
                        and schema.get("spec", {}).get("is_scannable", True))
        return entry_class(schema, path, is_scannable, self._randomise_scan_icon)


class OverrideEntry(LayoutWidget):
    value_changed = QtCore.pyqtSignal()

    def __init__(self, schema, path, is_scannable: bool, randomise_icon, *args):
        super().__init__(*args)

        self.schema = schema
        self.path = path
        self.randomise_icon = randomise_icon

        self.scan_type = QtWidgets.QComboBox()
        self.addWidget(self.scan_type, col=0)

        self.widget_stack = QtWidgets.QStackedWidget()

        scan_type_names = self._scan_type_names() if is_scannable else ["Fixed"]
        if len(scan_type_names) == 1:
            self.scan_type.setEnabled(False)
        for name in scan_type_names:
            self.scan_type.addItem(name)
            container = QtWidgets.QWidget()
            self._build_scan_ui(name, container)
            self.widget_stack.addWidget(container)
        self.scan_type.currentIndexChanged.connect(self.widget_stack.setCurrentIndex)
        self.addWidget(self.widget_stack, col=1)

    def read_from_params(self, params: dict, manager_datasets) -> None:
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
                        "Datasets still synchronising with master, " +
                        "cannot access '%s'", key)
                    bs = {}
                try:
                    return bs[key][1]
                except KeyError:
                    if default is None:
                        raise KeyError(f"Could not read dataset '{key}', but no " +
                                       "fallback default value given") from None
                    return default
                return bs

            value = eval_param_default(self.schema["default"], get_dataset)
        except Exception as e:
            logger.error("Failed to evaluate defaults string \"%s\": %s",
                         self.schema["default"], e)
            value = None
        self._set_fixed_value(value)
        self.disable_scan()

    def write_to_params(self, params: dict) -> None:
        raise NotImplementedError

    def disable_scan(self) -> None:
        raise NotImplementedError

    def _scan_type_names(self) -> List[str]:
        raise NotImplementedError

    def _build_scan_ui(self, name: str, target: QtWidgets.QWidget) -> None:
        raise NotImplementedError

    def _set_fixed_value(self, value) -> None:
        raise NotImplementedError


def _parse_list_pyon(values: str) -> List[float]:
    return pyon.decode("[" + values + "]")


class FloatOverrideEntry(OverrideEntry):
    def __init__(self, schema, *args):
        self.scan_types = OrderedDict([
            ("Fixed", (self._build_fixed_ui, self._write_override)),
            ("Refining", (self._build_refining_ui, self._write_refining_scan)),
            ("Linear", (self._build_linear_ui, self._write_linear_scan)),
            ("List", (self._build_list_ui, self._write_list_scan))
        ])
        self.current_scan_type = None
        self.scale = schema.get("spec", {}).get("scale", 1.0)

        super().__init__(schema, *args)

    def write_to_params(self, params: dict) -> None:
        self.scan_types[self.scan_type.currentText()][1](params)

    def disable_scan(self) -> None:
        # TODO: Move this into parent class as well.
        self.scan_type.setCurrentIndex(0)

    def _write_override(self, params: dict) -> None:
        # TODO: Move Fixed/Scanning distinction into parent class, have a subclass
        # per possible scan type, and have the different float/int/bool/… entries just
        # provide the list of scan names and mapping to subclasses.
        o = {"path": self.path, "value": self.box_value.value() * self.scale}
        params["overrides"].setdefault(self.schema["fqn"], []).append(o)

    def _write_refining_scan(self, params: dict) -> None:
        spec = {
            "fqn": self.schema["fqn"],
            "path": self.path,
            "type": "refining",
            "range": {
                "lower": self.box_refining_lower.value() * self.scale,
                "upper": self.box_refining_upper.value() * self.scale,
                "randomise_order": self.box_refining_randomise.isChecked()
            }
        }
        params["scan"].setdefault("axes", []).append(spec)

    def _write_linear_scan(self, params: dict) -> None:
        spec = {
            "fqn": self.schema["fqn"],
            "path": self.path,
            "type": "linear",
            "range": {
                "start": self.box_linear_start.value() * self.scale,
                "stop": self.box_linear_stop.value() * self.scale,
                "num_points": self.box_linear_points.value(),
                "randomise_order": self.box_linear_randomise.isChecked(),
            }
        }
        params["scan"].setdefault("axes", []).append(spec)

    def _write_list_scan(self, params: dict) -> None:
        try:
            values = [
                v * self.scale for v in _parse_list_pyon(self.box_list_pyon.text())
            ]
        except Exception as e:
            logger.info(e)
            values = []
        spec = {
            "fqn": self.schema["fqn"],
            "path": self.path,
            "type": "list",
            "range": {
                "values": values,
                "randomise_order": self.box_list_randomise.isChecked(),
            }
        }
        params["scan"].setdefault("axes", []).append(spec)

    def _scan_type_names(self) -> List[str]:
        return list(self.scan_types.keys())

    def _build_scan_ui(self, name: str, target: QtWidgets.QWidget) -> None:
        layout = QtWidgets.QHBoxLayout()
        self.scan_types[name][0](layout)
        target.setLayout(layout)

    def _build_fixed_ui(self, layout: QtWidgets.QLayout) -> None:
        self.box_value = self._make_spin_box()
        layout.addWidget(self.box_value)

    def _build_refining_ui(self, layout: QtWidgets.QLayout) -> None:
        self.box_refining_lower = self._make_spin_box()
        layout.addWidget(self.box_refining_lower)
        layout.setStretchFactor(self.box_refining_lower, 1)

        layout.addWidget(self._make_divider())

        self.box_refining_randomise = self._make_randomise_box()
        layout.addWidget(self.box_refining_randomise)
        layout.setStretchFactor(self.box_refining_randomise, 0)

        layout.addWidget(self._make_divider())

        self.box_refining_upper = self._make_spin_box()
        layout.addWidget(self.box_refining_upper)
        layout.setStretchFactor(self.box_refining_upper, 1)

    def _build_linear_ui(self, layout: QtWidgets.QLayout) -> None:
        self.box_linear_start = self._make_spin_box()
        layout.addWidget(self.box_linear_start)
        layout.setStretchFactor(self.box_linear_start, 1)

        layout.addWidget(self._make_divider())

        self.box_linear_points = QtWidgets.QSpinBox()
        self.box_linear_points.setMinimum(2)
        self.box_linear_points.setValue(21)

        # Somewhat gratuitously restrict the number of scan points for sizing, and to
        # avoid the user accidentally pasting in millions of points, etc.
        self.box_linear_points.setMaximum(0xffff)

        self.box_linear_points.setSuffix(" pts")
        layout.addWidget(self.box_linear_points)
        layout.setStretchFactor(self.box_linear_points, 0)

        self.box_linear_randomise = self._make_randomise_box()
        layout.addWidget(self.box_linear_randomise)
        layout.setStretchFactor(self.box_linear_randomise, 0)

        layout.addWidget(self._make_divider())

        self.box_linear_stop = self._make_spin_box()
        layout.addWidget(self.box_linear_stop)
        layout.setStretchFactor(self.box_linear_stop, 1)

    def _build_list_ui(self, layout: QtWidgets.QLayout) -> None:
        class Validator(QtGui.QValidator):
            def validate(self, input, pos):
                try:
                    [float(f) for f in _parse_list_pyon(input)]
                    return QtGui.QValidator.Acceptable, input, pos
                except Exception:
                    return QtGui.QValidator.Intermediate, input, pos

        self.box_list_pyon = QtWidgets.QLineEdit()
        self.box_list_pyon.setValidator(Validator(self))
        layout.addWidget(self.box_list_pyon)

        layout.addWidget(self._make_divider())

        self.box_list_randomise = self._make_randomise_box()
        layout.addWidget(self.box_list_randomise)
        layout.setStretchFactor(self.box_list_randomise, 0)

    def _make_spin_box(self):
        box = ScientificSpinBox()
        disable_scroll_wheel(box)
        box.valueChanged.connect(self.value_changed)

        spec = self.schema.get("spec", {})
        step = spec.get("step", 1.0)

        box.setDecimals(8)
        box.setPrecision()
        box.setSingleStep(step / self.scale)
        box.setRelativeStep()

        box.setMinimum(spec.get("min", float("-inf")) / self.scale)
        box.setMaximum(spec.get("max", float("inf")) / self.scale)

        unit = spec.get("unit", "")
        if unit:
            box.setSuffix(" " + unit)

        return box

    def _make_randomise_box(self):
        box = QtWidgets.QCheckBox()
        box.setToolTip("Randomise scan point order")
        box.setIcon(self.randomise_icon)
        box.setChecked(True)
        return box

    def _make_divider(self):
        f = QtWidgets.QFrame()
        f.setFrameShape(QtWidgets.QFrame.VLine)
        f.setFrameShadow(QtWidgets.QFrame.Sunken)
        f.setSizePolicy(QtWidgets.QSizePolicy.Preferred,
                        QtWidgets.QSizePolicy.Expanding)
        return f

    def _set_fixed_value(self, value):
        if value is None:
            # Error evaluating defaults, no better guess.
            value = 0.0
        self.box_value.setValue(float(value) / self.scale)


class StringOverrideEntry(OverrideEntry):
    def write_to_params(self, params: dict) -> None:
        o = {"path": self.path, "value": self.box_value.text()}
        params["overrides"].setdefault(self.schema["fqn"], []).append(o)

    def disable_scan(self) -> None:
        pass

    def _scan_type_names(self) -> List[str]:
        return ["Fixed"]

    def _build_scan_ui(self, name: str, target: QtWidgets.QWidget) -> None:
        if name != self._scan_type_names()[0]:
            raise ValueError("Unknown scan type: '{}'".format(name))

        layout = QtWidgets.QHBoxLayout()
        target.setLayout(layout)

        self.box_value = QtWidgets.QLineEdit()
        layout.addWidget(self.box_value)

    def _set_fixed_value(self, value) -> None:
        self.box_value.setText(value)

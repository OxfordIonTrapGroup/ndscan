import asyncio
import logging
import os

from collections import Counter, OrderedDict
from enum import Enum, unique
from functools import partial
from typing import List
from PyQt5 import QtCore, QtGui, QtWidgets
from artiq.dashboard.experiments import _WheelFilter
from artiq.gui.entries import procdesc_to_entry, ScanEntry
from artiq.gui.scientific_spinbox import ScientificSpinBox
from artiq.gui.tools import LayoutWidget, disable_scroll_wheel
from artiq.protocols import pyon
from .experiment import PARAMS_ARG_KEY
from .fuzzy_select import FuzzySelectWidget
from .utils import eval_param_default, shorten_to_unambiguous_suffixes


logger = logging.getLogger(__name__)


def _try_extract_ndscan_params(arguments):
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
                0, 0, 0, QtGui.QFontMetrics(self.font()).lineSpacing())
        self._bg_gradient.setColorAt(0, self.palette().base().color())
        self._bg_gradient.setColorAt(1, self.palette().midlight().color())

        self._save_timer = QtCore.QTimer(self)
        self._save_timer.timeout.connect(self._save_to_argument)

        self._param_entries = dict()
        self._groups = dict()
        self._arg_to_widgets = dict()
        self._override_items = dict()

        # FIXME: Paths after installation.
        def icon_path(name):
            return os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons", name)
        self._add_override_icon = QtGui.QIcon(icon_path("list-add-32.png"))
        self._remove_override_icon = QtGui.QIcon(icon_path("list-remove-32.png"))
        self._randomise_scan_icon = QtGui.QIcon(icon_path("media-playlist-shuffle-32.svg"))
        self._default_value_icon = self.style().standardIcon(
                QtWidgets.QStyle.SP_BrowserReload)
        self._disable_scans_icon = self.style().standardIcon(
                QtWidgets.QStyle.SP_DialogResetButton)

        self._arguments = self.manager.get_submission_arguments(self.expurl)
        ndscan_params, vanilla_args = _try_extract_ndscan_params(self._arguments)

        if not ndscan_params:
            self.addTopLevelItem(QtWidgets.QTreeWidgetItem([
                "Error: Parameter metadata not found."]))
        else:
            self._ndscan_params = ndscan_params

            self._build_shortened_fqns()

            for fqn, path in ndscan_params["always_shown"]:
                self._make_param_items(fqn, path, True)

            for name, argument in vanilla_args.items():
                self._make_vanilla_argument_item(name, argument)

            self.override_separator = self._make_line_separator()

            self._make_add_override_prompt_item()
            self._set_override_line_idle()

            for ax in ndscan_params["scan"]["axes"]:
                self._make_override_item(ax["fqn"], ax["path"])

            for fqn, overrides in ndscan_params["overrides"].items():
                for o in overrides:
                    self._make_override_item(fqn, o["path"])

            self._make_line_separator()

            scan_options_group = self._make_group_header_item("Scan options")
            self.addTopLevelItem(scan_options_group)


            #

            num_repeats_container = QtWidgets.QWidget()
            num_repeats_layout = QtWidgets.QHBoxLayout()
            num_repeats_container.setLayout(num_repeats_layout)

            num_repeats_label = QtWidgets.QLabel("Number of repeats: ")
            num_repeats_layout.addWidget(num_repeats_label)
            num_repeats_layout.setStretchFactor(num_repeats_label, 0)

            self.num_repeats_box = QtWidgets.QSpinBox()
            self.num_repeats_box.setMinimum(1)
            self.num_repeats_box.setMaximum(2**16) # A gratuitous, but hopefully generous restriction
            self.num_repeats_box.setValue(ndscan_params["scan"].get("num_repeats", 1))
            num_repeats_layout.addWidget(self.num_repeats_box)
            num_repeats_layout.setStretchFactor(self.num_repeats_box, 0)

            num_repeats_layout.addStretch()

            num_repeats_item = QtWidgets.QTreeWidgetItem()
            scan_options_group.addChild(num_repeats_item)
            self.setItemWidget(num_repeats_item, 1, num_repeats_container)

            #

            cwa_container = QtWidgets.QWidget()
            cwa_layout = QtWidgets.QHBoxLayout()
            cwa_container.setLayout(cwa_layout)

            cwa_label = QtWidgets.QLabel("Repeat continuously without axes: ")
            cwa_layout.addWidget(cwa_label)
            cwa_layout.setStretchFactor(cwa_label, 0)

            self.cwa_box = QtWidgets.QCheckBox()
            self.cwa_box.setChecked(ndscan_params["scan"].get("continuous_without_axes", True))
            cwa_layout.addWidget(self.cwa_box)
            cwa_layout.setStretchFactor(self.cwa_box, 1)

            cwa_item = QtWidgets.QTreeWidgetItem()
            scan_options_group.addChild(cwa_item)
            self.setItemWidget(cwa_item, 1, cwa_container)

            #

            randomise_globally_container = QtWidgets.QWidget()
            randomise_globally_layout = QtWidgets.QHBoxLayout()
            randomise_globally_container.setLayout(randomise_globally_layout)

            randomise_globally_label = QtWidgets.QLabel("Randomise point order across axes: ")
            randomise_globally_layout.addWidget(randomise_globally_label)
            randomise_globally_layout.setStretchFactor(randomise_globally_label, 0)

            self.randomise_globally_box = QtWidgets.QCheckBox()
            self.randomise_globally_box.setChecked(ndscan_params["scan"].get("randomise_order_globally", False))
            randomise_globally_layout.addWidget(self.randomise_globally_box)
            randomise_globally_layout.setStretchFactor(self.randomise_globally_box, 1)

            randomise_globally_item = QtWidgets.QTreeWidgetItem()
            scan_options_group.addChild(randomise_globally_item)
            self.setItemWidget(randomise_globally_item, 1, randomise_globally_container)

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
        return {
            "expanded": expanded,
            "scroll": self.verticalScrollBar().value()
        }

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
                    self.insertTopLevelItem(insert_at_idx + added_item_count, widget_item)
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

        # KLUDGE: On dashboard startup, the datasets have not necessarily been
        # synced yet (self.manager.datasets is still an empty dict). However,
        # all experiments opened on startup were open previously, so all parameters
        # should have override values set.
        datasets = getattr(self.manager.datasets, "backing_store", {})
        entry.read_from_params(self._ndscan_params, datasets)

        entry.value_changed.connect(self._set_save_timer)
        self._param_entries[(fqn, path)] = entry
        self.setItemWidget(main_item, 1, entry)

        buttons = LayoutWidget()

        reset_default = QtWidgets.QToolButton()
        reset_default.setToolTip("Reset parameter to default value")
        reset_default.setIcon(
            QtWidgets.QApplication.style().standardIcon(
                QtWidgets.QStyle.SP_BrowserReload))
        reset_default.clicked.connect(
            partial(self._reset_entry_to_default, fqn, path))
        buttons.addWidget(reset_default, col=0)

        remove_override = QtWidgets.QToolButton()
        remove_override.setIcon(self._remove_override_icon)
        remove_override.setToolTip("Remove this parameter override")
        remove_override.clicked.connect(
            partial(self._remove_override, fqn, path))
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
        f.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Preferred)

        wi = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(wi)
        wi.setFirstColumnSpanned(True)
        self.setItemWidget(wi, 1, f)
        return wi

    def _make_override_item(self, fqn, path):
        items = self._make_param_items(fqn, path, False,
            self.indexOfTopLevelItem(self._override_prompt_item))
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
        self._add_override_prompt_box.set_choices(
            [(s, 0) for s in self._param_choice_map.keys()])

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
        self.insertTopLevelItem(self.indexOfTopLevelItem(self.override_separator), group)
        self._groups[name] = group
        return group

    def _recompute_vanilla_argument_clicked(self, name):
        asyncio.ensure_future(self._recompute_vanilla_argument(name))

    async def _recompute_vanilla_argument(self, name):
        try:
            arginfo, _ = await self.manager.examine_arginfo(self.expurl)
        except:
            logger.error("Could not recompute argument '%s' of '%s'",
                         name, self.expurl, exc_info=True)
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
        self._param_entries[(fqn, path)].read_from_params({},
            self.manager.datasets.backing_store)

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
            display_string = "{} – {}".format(self._param_display_name(fqn, path), schema["description"])
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

        # Reset previous overrides, repopulate with currently active ones.
        scan = self._ndscan_params["scan"]
        scan["axes"] = []
        self._ndscan_params["overrides"] = {}
        for item in self._param_entries.values():
            item.write_to_params(self._ndscan_params)

        # Store scan parameters.
        scan["num_repeats"] = self.num_repeats_box.value()
        scan["continuous_without_axes"] = self.cwa_box.isChecked()
        scan["randomise_order_globally"] = self.randomise_globally_box.isChecked()

        _update_ndscan_params(self._arguments, self._ndscan_params)

    def _make_override_entry(self, fqn, path):
        schema = self._schema_for_fqn(fqn)

        # TODO: Switch on schema["type"].
        return FloatOverrideEntry(schema, path, self._randomise_scan_icon)


class OverrideEntry(LayoutWidget):
    value_changed = QtCore.pyqtSignal()

    def __init__(self, schema, path, randomise_icon, *args):
        super().__init__(*args)

        self.schema = schema
        self.path = path
        self.randomise_icon = randomise_icon

        self.scan_type = QtWidgets.QComboBox()
        self.addWidget(self.scan_type, col=0)

        self.widget_stack = QtWidgets.QStackedWidget()
        for name in self._scan_type_names():
            self.scan_type.addItem(name)
            container = QtWidgets.QWidget()
            self._build_scan_ui(name, container)
            self.widget_stack.addWidget(container)
        self.scan_type.currentIndexChanged.connect(self.widget_stack.setCurrentIndex)
        self.addWidget(self.widget_stack, col=1)

    def read_from_params(self, params: dict, datasets) -> None:
        for o in params.get("overrides", {}).get(self.schema["fqn"], []):
            if o["path"] == self.path:
                self._set_fixed_value(o["value"])
                return
        try:
            def get_dataset(key, default):
                try:
                    return datasets[key][1]
                except KeyError:
                    return default
                return datasets
            value = eval_param_default(self.schema["default"], get_dataset)
        except Exception as e:
            logger.error("Failed to evaluate defaults string \"%s\": %s", self.schema["default"], e)
            # XXX: Fix for other types.
            value = 0.0
        self._set_fixed_value(value)

    def write_to_params(self, params: dict) -> None:
        raise NotImplementedError()

    def disable_scan(self) -> None:
        raise NotImplementedError()

    def _scan_type_names(self) -> List[str]:
        raise NotImplementedError()

    def _build_scan_ui(self, name: str, target: QtWidgets.QWidget) -> None:
        raise NotImplementedError()


class FloatOverrideEntry(OverrideEntry):
    def __init__(self, schema, *args):
        self.scan_types = OrderedDict([
            ("Fixed", (self._build_fixed_ui, self._write_override)),
            ("Refining", (self._build_refining_ui, self._write_refining_scan)),
            ("Linear", (self._build_linear_ui, self._write_linear_scan))
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
        self.box_linear_points.setMaximum(2**16) # A gratuitous, but probably generous restriction
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

    def _make_spin_box(self):
        box = ScientificSpinBox()
        disable_scroll_wheel(box)
        box.valueChanged.connect(self.value_changed)

        spec = self.schema.get("spec", {})
        step = spec.get("step", 1.0)

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
        f.setSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Expanding)
        return f

    def _set_fixed_value(self, value):
        self.box_value.setValue(float(value) / self.scale)

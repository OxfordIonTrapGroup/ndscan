import asyncio
import logging
import os

from collections import Counter
from functools import partial
from PyQt5 import QtCore, QtGui, QtWidgets
from artiq.dashboard.experiments import _WheelFilter
from artiq.gui.entries import procdesc_to_entry, ScanEntry
from artiq.gui.tools import LayoutWidget
from artiq.protocols import pyon
from .experiment import PARAMS_ARG_KEY
from .fuzzy_select import FuzzySelectWidget

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

        self.bg_gradient = QtGui.QLinearGradient(
                0, 0, 0, QtGui.QFontMetrics(self.font()).lineSpacing())
        self.bg_gradient.setColorAt(0, self.palette().base().color())
        self.bg_gradient.setColorAt(1, self.palette().midlight().color())

        self._shown_params = set()

        self._groups = dict()
        self._arg_to_widgets = dict()

        # FIXME: Paths after installation.
        def icon_path(name):
            return os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons", name)
        self.add_override_icon = QtGui.QIcon(icon_path("list-add-32.png"))
        self.remove_override_icon = QtGui.QIcon(icon_path("list-remove-32.png"))
        self.default_value_icon = QtWidgets.QApplication.style().standardIcon(
                QtWidgets.QStyle.SP_BrowserReload)

        arguments = self.manager.get_submission_arguments(self.expurl)
        ndscan_params, vanilla_args = _try_extract_ndscan_params(arguments)

        if not ndscan_params:
            self.addTopLevelItem(QtWidgets.QTreeWidgetItem([
                "Error: Parameter metadata not found."]))
        else:
            self.ndscan_params = ndscan_params

            self._build_shortened_fqns()

            for fqn, path in ndscan_params["always_shown_params"]:
                self._make_param_item(fqn, path, True)

            for name, argument in vanilla_args.items():
                self._make_vanilla_argument_item(name, argument)

            self.override_separator = self._make_line_separator()

            self._make_override_prompt_item()
            self._set_override_line_idle()

        self._make_line_separator()

        buttons_item = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(buttons_item)
        buttons_item.setFirstColumnSpanned(True)
        recompute_arguments = QtWidgets.QPushButton("Recompute all arguments")
        recompute_arguments.setIcon(self.default_value_icon)
        recompute_arguments.clicked.connect(dock._recompute_arguments_clicked)

        load_hdf5 = QtWidgets.QPushButton("Load HDF5")
        load_hdf5.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_DialogOpenButton))
        load_hdf5.clicked.connect(dock._load_hdf5_clicked)

        buttons = LayoutWidget()
        buttons.addWidget(recompute_arguments, 1, 1)
        buttons.addWidget(load_hdf5, 1, 2)
        buttons.layout.setColumnStretch(0, 1)
        buttons.layout.setColumnStretch(1, 0)
        buttons.layout.setColumnStretch(2, 0)
        buttons.layout.setColumnStretch(3, 1)
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

    def _make_param_item(self, fqn, path, show_always, insert_at_idx=-1):
        self._shown_params.add((fqn, path))

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
            id_item.setBackground(col, self.bg_gradient)
        id_item.setFirstColumnSpanned(True)
        id_item.setForeground(0, self.palette().mid())

        main_item = QtWidgets.QTreeWidgetItem([schema["description"]])
        add_item(main_item)

        # Render description in bold.
        font = main_item.font(0)
        font.setBold(True)
        main_item.setFont(0, font)

        entry = LayoutWidget()
        scan_type = QtWidgets.QComboBox()
        scan_type.addItem("Fixed")
        scan_type.addItem("Refining")
        entry.addWidget(scan_type, col=0)
        entry.addWidget(QtWidgets.QLineEdit(), col=1)
        self.setItemWidget(main_item, 1, entry)

        buttons = LayoutWidget()

        reset_default = QtWidgets.QToolButton()
        reset_default.setToolTip("Reset parameter to default value")
        reset_default.setIcon(
            QtWidgets.QApplication.style().standardIcon(
                QtWidgets.QStyle.SP_BrowserReload))
        reset_default.clicked.connect(
            partial(self._reset_param_to_default, fqn, path))
        buttons.addWidget(reset_default, col=0)

        remove_override = QtWidgets.QToolButton()
        remove_override.setIcon(self.remove_override_icon)
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
            widget_item.setBackground(col, self.bg_gradient)
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
        recompute_argument.setIcon(self.default_value_icon)
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
        f.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Preferred)

        wi = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(wi)
        wi.setFirstColumnSpanned(True)
        self.setItemWidget(wi, 1, f)
        return wi

    def _make_override_item(self, choice):
        fqn, path = self._param_choice_map[choice]
        self._make_param_item(fqn, path, False,
            self.indexOfTopLevelItem(self.override_prompt_item))

    def _make_override_prompt_item(self):
        self.override_prompt_item = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(self.override_prompt_item)

        # Layout to display button/prompt label, depending on which one is active.
        left = LayoutWidget()

        self.add_override_button = QtWidgets.QToolButton()
        self.add_override_button.setIcon(self.add_override_icon)
        self.add_override_button.clicked.connect(self._set_override_line_active)
        self.add_override_button.setShortcut(QtCore.Qt.CTRL + QtCore.Qt.Key_T)
        left.addWidget(self.add_override_button, 0, 0)

        self.add_override_prompt_label = QtWidgets.QLabel("Add parameter:")
        left.addWidget(self.add_override_prompt_label, 0, 0)

        left.layout.setColumnStretch(0, 0)
        left.layout.setColumnStretch(1, 1)
        self.setItemWidget(self.override_prompt_item, 0, left)

        prompt = LayoutWidget()
        self.add_override_prompt_box = FuzzySelectWidget([], self)
        self.add_override_prompt_box.finished.connect(self._make_override_item)
        self.add_override_prompt_box.aborted.connect(self._set_override_line_idle)
        prompt.addWidget(self.add_override_prompt_box)
        self.setItemWidget(self.override_prompt_item, 1, prompt)

    def _set_override_line_idle(self):
        self.add_override_button.setEnabled(True)
        self.add_override_button.setVisible(True)
        self.add_override_prompt_label.setVisible(False)
        self.add_override_prompt_box.setVisible(False)

    def _set_override_line_active(self):
        self._update_param_choice_map()
        self.add_override_prompt_box.set_choices(
            [(s, 0) for s in self._param_choice_map.keys()])

        self.add_override_button.setEnabled(False)
        self.add_override_button.setVisible(False)
        self.add_override_prompt_label.setVisible(True)
        self.add_override_prompt_box.setVisible(True)

        # TODO: See whether I can't get focus proxies to work.
        self.add_override_prompt_box.line_edit.setFocus()

    def _ensure_group_widget(self, name):
        if name in self._groups:
            return self._groups[name]
        group = QtWidgets.QTreeWidgetItem([name])
        for col in range(3):
            group.setBackground(col, self.palette().mid())
            group.setForeground(col, self.palette().brightText())
            font = group.font(col)
            font.setBold(True)
            group.setFont(col, font)
        self.addTopLevelItem(group)
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

    def _reset_param_to_default(self, fqn, path):
        # TODO: Implement.
        logger.info("Reset to default: %s@%s", fqn, path)

    def _remove_override(self, fqn, path):
        # TODO: Implement.
        logger.info("Remove override: %s@%s", fqn, path)

    def _update_param_choice_map(self):
        self._param_choice_map = dict()

        def add(fqn, path):
            # Skip params already displayed.
            if (fqn, path) in self._shown_params:
                return
            schema = self._schema_for_fqn(fqn)
            display_string = "{} – {}".format(self._param_display_name(fqn, path), schema["description"])
            self._param_choice_map[display_string] = (fqn, path)

        fqn_occurences = Counter()
        for path, fqns in self.ndscan_params["params"].items():
            for fqn in fqns:
                add(fqn, path)
                fqn_occurences[fqn] += 1

        # TODO: Offer non-global wildcards for parameters used in multiple hierarchies.
        for fqn, count in fqn_occurences.items():
            if count > 1:
                add(fqn, "*")

    def _build_shortened_fqns(self):
        short_to_fqns = dict()
        self.shortened_fqns = dict()

        for current in self.ndscan_params["schemata"].keys():
            if current in self.shortened_fqns:
                continue

            n = 1
            while True:
                candidate = last_n_parts(current, n)
                if candidate not in short_to_fqns:
                    short_to_fqns[candidate] = set([current])
                    self.shortened_fqns[current] = candidate
                    break

                # We have a conflict, disambiguate.
                existing_fqns = short_to_fqns[candidate]
                for old in existing_fqns:
                    if self.shortened_fqns[old] == candidate:
                        # This hasn't previously been moved to a higher n, so
                        # do it now.
                        self.shortened_fqns[old] = last_n_parts(old, n + 1)
                        break # Exits inner for loop.
                existing_fqns.add(current)
                n += 1

    def _param_display_name(self, fqn, path):
        if not path:
            path = "/"
        return self.shortened_fqns[fqn] + "@" + path

    def _schema_for_fqn(self, fqn):
        return self.ndscan_params["schemata"][fqn]

def last_n_parts(fqn, n):
    return ".".join(fqn.split(".")[-(n + 1):])

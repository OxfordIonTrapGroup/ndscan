import logging
from enum import Enum, unique
from typing import Any

from .._qt import QtCore, QtGui, QtWidgets
from .utils import (
    eval_default_using_local_datasets,
    load_icon_cached,
    set_column_resize_mode,
)

logger = logging.getLogger(__name__)


def format_value(schema: dict[str, Any], value: Any) -> str:
    """Format a parameter value for display.

    :param schema: The parameter schema, for unit/… information.
    :param value: The value evaluated into its Python representation as it would appear
        in PYON).
    """
    match schema["type"]:
        case "bool":
            return str(value)
        case "enum":
            return value  # Represented as a string on the PYON layer.
        case "string":
            return value
        case num if num in ("float", "int"):
            spec = schema.get("spec", {})
            scale = spec.get("scale", 1)

            if scale == 1:
                # Avoid converting int to float.
                formatted = str(value)
            else:
                formatted = str(value / scale)
            unit = spec.get("unit", "")
            if unit:
                formatted += " "
                formatted += unit
            return formatted
        case unknown:
            logger.warning(f"Unknown parameter type: '{unknown}'")
            return str(value)


@unique
class OverrideStatus(Enum):
    not_overridden = 0
    overriden = 1
    always_shown = 2


class OverrideProvider:
    """Interface to list of active overrides.

    Practically, this is the main argument editor, but this constrains the interface to
    reduce accidental coupling.
    """

    def override_status(self, fqn, path) -> OverrideStatus:
        raise NotImplementedError

    def add_override(self, fqn, path) -> None:
        raise NotImplementedError

    def remove_override(self, fqn, path) -> None:
        raise NotImplementedError


class ParamTreeDialog(QtWidgets.QDialog):
    def __init__(
        self,
        instances: dict[str, list[str]],
        schemata: dict[str, dict[str, Any]],
        override_provider: OverrideProvider,
        manager_datasets,
        add_override_icon: QtGui.QIcon,
        remove_override_icon: QtGui.QIcon,
        parent=None,
    ):
        super().__init__(parent=parent)
        self.instances = instances
        self.schemata = schemata
        self.override_provider = override_provider
        self.manager_datasets = manager_datasets

        self.fragment_icon = load_icon_cached("preferences-plugin-32.png")
        self.param_icon = load_icon_cached("applications-system-32.png")

        self.resize(1024, 600)

        self.splitter = QtWidgets.QSplitter(self)
        self.setLayout(QtWidgets.QVBoxLayout())
        self.layout().addWidget(self.splitter)
        self.layout().setContentsMargins(3, 3, 3, 3)

        self.left_pane = QtWidgets.QWidget(self)
        self.left_pane.setLayout(QtWidgets.QVBoxLayout())
        self.left_pane.layout().setContentsMargins(0, 0, 0, 0)
        self.tree_widget = QtWidgets.QTreeWidget(self)
        self.left_pane.layout().addWidget(self.tree_widget)
        self.splitter.addWidget(self.left_pane)

        self.bottom_line = QtWidgets.QWidget(self)
        self.bottom_line.setLayout(QtWidgets.QHBoxLayout())
        self.bottom_line.layout().setContentsMargins(0, 0, 0, 0)
        self.layout().addWidget(self.bottom_line)

        self.show_empty_checkbox = QtWidgets.QCheckBox(
            "Show fragments without parameters"
        )
        self.show_empty_checkbox.setChecked(False)
        self.show_empty_checkbox.stateChanged.connect(self._regenerate_tree_items)
        self.bottom_line.layout().addWidget(self.show_empty_checkbox)
        self.bottom_line.layout().addStretch()

        num_params = sum(len(v) for v in instances.values())
        self.stats_label = QtWidgets.QLabel(
            f"{num_params} parameters in {len(instances)} fragments"
        )
        self.bottom_line.layout().addWidget(self.stats_label)

        #
        # Initialise tree.
        #

        self.tree_widget.setColumnCount(2)
        set_column_resize_mode(
            self.tree_widget, 0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        set_column_resize_mode(
            self.tree_widget, 1, QtWidgets.QHeaderView.ResizeMode.Stretch
        )
        self._regenerate_tree_items()
        self.tree_widget.itemSelectionChanged.connect(self._update_detail_pane)
        self.tree_widget.header().setVisible(False)

        self.right_pane = QtWidgets.QWidget(self)
        self.right_pane.setLayout(QtWidgets.QVBoxLayout())
        self.right_pane.layout().setContentsMargins(3, 3, 3, 0)

        # Use a read-only QTextEdit for display rather than a QLabel due to better line wrapping customisation. I'm
        self.text_edit = QtWidgets.QTextEdit(self)
        self.text_edit.setReadOnly(True)
        self.text_edit.setViewportMargins(0, 0, 0, 0)
        self.text_edit.document().setDocumentMargin(0)
        self.text_edit.setStyleSheet("""
            QTextEdit {
                background: transparent;
                border: none;
            }
        """)
        self.text_edit.setWordWrapMode(QtGui.QTextOption.WrapMode.WrapAnywhere)
        self.text_edit.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Expanding,
        )
        self.right_pane.layout().addWidget(self.text_edit)

        self.add_override_button = QtWidgets.QPushButton("Add override for parameter")
        self.add_override_button.setIcon(add_override_icon)
        self.add_override_button.setEnabled(False)
        self.add_override_button.clicked.connect(self._add_override_for_selection)
        self.right_pane.layout().addWidget(self.add_override_button)
        self.remove_override_button = QtWidgets.QPushButton(
            "Remove override for parameter"
        )
        self.remove_override_button.setIcon(remove_override_icon)
        self.remove_override_button.setEnabled(False)
        self.remove_override_button.clicked.connect(self._remove_override_for_selection)
        self.right_pane.layout().addWidget(self.remove_override_button)
        self.splitter.addWidget(self.right_pane)
        self.splitter.setStretchFactor(0, 1)
        self.splitter.setStretchFactor(1, 0)
        self.splitter.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Expanding,
        )

    def _get_selection_identity(self) -> tuple[str, str] | None:
        selection = self.tree_widget.selectedItems()
        if not selection:
            return None
        assert len(selection) == 1, "Should not have multi-selection enabled"
        return selection[0].data(0, QtCore.Qt.ItemDataRole.UserRole)

    def _add_override_for_selection(self):
        self.override_provider.add_override(*self._get_selection_identity())
        self._update_detail_pane()

    def _remove_override_for_selection(self):
        self.override_provider.remove_override(*self._get_selection_identity())
        self._update_detail_pane()

    def _update_detail_pane(self):
        selected = self._get_selection_identity()
        if selected is None:
            self.text_edit.setHtml("")
            self.add_override_button.setEnabled(False)
            self.remove_override_button.setEnabled(False)
            return
        fqn, path = selected
        schema = self.schemata[fqn]

        html = f"""
            <p><b>{schema["description"]}</b></p>
            <p><i>{fqn}@{path}</i></p>
            <p>Default (specified): {schema["default"]}<br>
            Default (evaluated): {self._format_default(schema)}</p>
            <p>
        """
        extras = []
        unit_extras = []
        spec = schema.get("spec", {})
        if unit := spec.get("unit", ""):
            unit_extras.append(f"Unit: {unit}")
        if (scale := spec.get("scale", 1)) != 1:
            unit_extras.append(f"Scale: {scale}")
        if unit_extras:
            extras.append(unit_extras)
        range_extras = []
        if "min" in spec:
            range_extras.append(f"Minimum: {format_value(schema, spec['min'])}")
        if "max" in spec:
            range_extras.append(f"Maximum: {format_value(schema, spec['max'])}")
        if range_extras:
            extras.append(range_extras)
        html += "</p><p>".join("<br>".join(e) for e in extras)
        html += "</p>"
        self.text_edit.setHtml(html)

        override_status = self.override_provider.override_status(fqn, path)
        can_add = override_status == OverrideStatus.not_overridden
        self.add_override_button.setEnabled(can_add)
        self.add_override_button.setDefault(can_add)
        can_remove = override_status == OverrideStatus.overriden
        self.remove_override_button.setEnabled(can_remove)
        self.remove_override_button.setDefault(can_remove)

    def _regenerate_tree_items(self):
        self.tree_widget.setUpdatesEnabled(False)
        self.tree_widget.clear()

        self.fragment_tree_items = dict()
        # Lexicographic in particular also ensures that parents are first.
        paths = sorted(self.instances.keys())
        for path in paths:
            if not path:
                # This is the root anyway.
                continue
            parts = path.split("/")
            parent_path = "/".join(parts[:-1])
            parent = None
            title = None
            if not parent_path:
                # Child of root fragment.
                parent = self.tree_widget
                title = path
            elif parent_path not in self.fragment_tree_items:
                logger.warning(f"Missing instance metadata for '{parent}'")
                # Make a new top-level fragment to not drop any parameters that may
                # exist.
                parent = self.tree_widget
                title = path
            else:
                parent = self.fragment_tree_items[parent_path]
                title = parts[-1]
            assert parent is not None
            item = QtWidgets.QTreeWidgetItem(parent)
            item.setIcon(0, self.fragment_icon)
            item.setText(0, title)
            self.fragment_tree_items[path] = item

        for path, params in self.instances.items():
            fragment_fqn = None
            if path:
                parent = self.fragment_tree_items[path]
            else:
                parent = self.tree_widget

            for param_fqn in sorted(params):
                item = QtWidgets.QTreeWidgetItem(parent)
                item.setIcon(0, self.param_icon)
                parts = param_fqn.split(".")
                parent_fqn = ".".join(parts[:-1])
                if fragment_fqn is None:
                    if path:
                        parent.setText(1, parent_fqn)
                    fragment_fqn = parent_fqn
                else:
                    if parent_fqn != fragment_fqn:
                        logger.warning(
                            f"Mismatch in fragment FQN inferred for '{path}' "
                            f"('{parent_fqn}' vs '{fragment_fqn}')"
                        )
                item.setText(0, parts[-1])
                item.setText(1, self._format_default(self.schemata[param_fqn]))
                item.setData(0, QtCore.Qt.ItemDataRole.UserRole, (param_fqn, path))

        if not self.show_empty_checkbox.isChecked():
            while True:
                # Fixed-point iteration to recursively prune empty subtrees.
                is_unchanged = True
                for path in list(self.fragment_tree_items.keys()):
                    item = self.fragment_tree_items[path]
                    if item.childCount() == 0:
                        del self.fragment_tree_items[path]
                        # In C++, we could just drop the QTreeWidgetItem().
                        item.parent().removeChild(item)
                        is_unchanged = False
                if is_unchanged:
                    break

        self.tree_widget.setUpdatesEnabled(True)

    def _format_default(self, schema):
        try:
            return format_value(
                schema,
                eval_default_using_local_datasets(
                    schema["default"], self.manager_datasets
                ),
            )
        except Exception as e:
            return f"<error evaluating default: {e}>"

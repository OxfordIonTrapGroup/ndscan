import functools
import logging
import os
from typing import Any

from .._qt import QtGui, QtWidgets
from ..utils import eval_param_default

logger = logging.getLogger(__name__)


def format_override_identity(fqn: str, path_spec: str) -> str:
    """Return the canonical user-readable representation of a parameter specification.

    This is for instance used in error messages.
    """
    return fqn + "@" + (path_spec or "/")


def icon_path(filename: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons", filename)


@functools.cache
def load_icon_cached(filename: str) -> QtGui.QIcon:
    """Load image with the given filename from icons/ as a QIcon, with an unbounded
    cache.

    The runtime overhead for this can be noticeable, especially on Windows.
    """
    return QtGui.QIcon(icon_path(filename))


def set_column_resize_mode(
    tree_widget: QtWidgets.QTreeWidget, idx: int, mode: QtWidgets.QHeaderView.ResizeMode
):
    if hasattr(tree_widget.header(), "setSectionResizeMode"):
        tree_widget.header().setSectionResizeMode(idx, mode)
    else:
        tree_widget.header().setResizeMode(idx, mode)


def eval_default_using_local_datasets(default_str: str, manager_datasets) -> Any:
    def get_dataset(key, default=None):
        try:
            bs = manager_datasets.backing_store
        except AttributeError:
            logger.error(
                "Datasets still synchronising with master, cannot access '%s'",
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

    return eval_param_default(default_str, get_dataset)

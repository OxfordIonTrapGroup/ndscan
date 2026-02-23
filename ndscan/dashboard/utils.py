import functools
import os

from .._qt import QtGui, QtWidgets


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


def set_column_resize_mode(tree_widget: QtWidgets, idx, mode):
    if hasattr(tree_widget.header(), "setSectionResizeMode"):
        tree_widget.header().setSectionResizeMode(idx, mode)
    else:
        tree_widget.header().setResizeMode(idx, mode)

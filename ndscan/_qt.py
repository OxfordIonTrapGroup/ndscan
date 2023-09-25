"""Internal PyQt{5, 6} compatibility shim.

We just rely on the qasync Qt detection code to either adapt to what has already been
loaded into the current process (for the dashboard_editor), or otherwise auto-detect
available libraries (which can be overridden using the QT_API environment variable).
"""

from qasync import QtCore, QtGui, QtWidgets

__all__ = ["QtCore", "QtGui", "QtWidgets"]

# For PyQt5, monkey-patch a few classes to also be available from their new locations.
# This appears to be the only non-backwards-compatible change we came across during the
# migration; if more are discovered in the future, appropriate shims should be inserted
# here.
for name in ("QAction", "QActionGroup"):
    if not hasattr(QtGui, name):
        setattr(QtGui, name, getattr(QtWidgets, name))

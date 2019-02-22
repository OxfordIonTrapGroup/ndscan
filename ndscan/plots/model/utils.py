from quamash import QtCore


def call_later(func):
    QtCore.QTimer.singleShot(0, func)


def emit_later(signal, *args):
    call_later(lambda: signal.emit(*args))

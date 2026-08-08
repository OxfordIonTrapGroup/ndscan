from .._qt import QtCore, QtWidgets


class ScrubbableProgressBar(QtWidgets.QSlider):
    def __init__(self, parent=None):
        super().__init__(QtCore.Qt.Orientation.Horizontal, parent)
        self.setProperty("expanded", False)

        self._set_stylesheet()

        self.setFixedHeight(4)
        self.setTickInterval(1)

    def _set_stylesheet(self):
        self.setStyleSheet("""
            QSlider::sub-page:horizontal {
                background: #20B0CF;
            }
            QSlider[expanded="true"]::handle:horizontal {
                background: #AAD0EF;
                min-width: 1px;
                max-width: 1px;
                width: 1px;
                height: 1px;
                min-height: 1px;
                max-height: 1px;
                border-radius: 1px;
            }
            QSlider[expanded="false"]::handle:horizontal {
                background: transparent;
                border: none;
            }
            """)

    def setExpanded(self, expanded: bool):
        self.setProperty("expanded", expanded)

        # Force Qt to re-evaluate the stylesheet
        self.style().unpolish(self)
        self.style().polish(self)
        self.update()

    def enterEvent(self, event):
        self.setExpanded(True)
        self.setFixedHeight(self._height)
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.setExpanded(False)
        self.setFixedHeight(self._height)
        super().leaveEvent(event)

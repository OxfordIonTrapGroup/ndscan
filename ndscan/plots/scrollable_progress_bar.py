from .._qt import QtCore, QtWidgets


class ScrubbableProgressBar(QtWidgets.QSlider):
    rollback_target_changed = QtCore.pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(QtCore.Qt.Orientation.Horizontal, parent)
        self.setProperty("expanded", False)

        self._set_stylesheet()
        self._height = 6
        self.setFixedHeight(self._height)
        self.setTickInterval(1)
        self.setMinimum(0)
        self.valueChanged.connect(self.rollback_target)
        self.setValue(self.maximum())

    def update_points(self, point_data: dict[str, list]):
        num_points = len(next(iter(point_data.values()), []))
        self.setMaximum(max(0, num_points - 1))

        if self.target_idx == -1:
            self.setValue(self.maximum())

    def rollback_target(self, index):
        if index == self.maximum():
            self.target_idx = -1
        else:
            self.target_idx = index

        self.rollback_target_changed.emit(self.target_idx)

    def _set_stylesheet(self):
        self.setStyleSheet("""
            # QSlider::sub-page:horizontal {
            #     background: #20B0CF;
            # }
            # QSlider[expanded="true"]::handle:horizontal {
            #     background: #AAD0EF;
            # }
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

from .._qt import QtCore, QtWidgets


class SliderContainer(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.slider = ScrubbableProgressBar(self)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        new_width = event.size().width()
        self.slider.setExpanded(False, new_width)


class ScrubbableProgressBar(QtWidgets.QSlider):
    rollback_target_changed = QtCore.pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(QtCore.Qt.Orientation.Horizontal, parent)

        self._set_stylesheet()
        self._expanded_height = 6
        self.setFixedHeight(6)
        self.setTickInterval(1)
        self.setMinimum(0)
        self.valueChanged.connect(self.rollback_target)
        self.setValue(self.maximum())
        self.hover = False

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
            QSlider::add-page:horizontal {
                background: rgba(0, 0, 0, 200);
            }
            QSlider::handle:horizontal {
                background: transparent;
                border: none;
            }
            QSlider::sub-page:horizontal {
                background: rgba(170, 208, 239, 255);
            }
            QSlider::handle:horizontal::hover {
                background: #fff;
            }
        """)

    def setExpanded(self, expanded: bool, width):
        if expanded:
            self.setGeometry(0, 4, width, 6)
        else:
            self.setGeometry(0, 6, width, 4)

    def enterEvent(self, event):
        self.setExpanded(True, self.parent().width())
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.setExpanded(False, self.parent().width())
        super().leaveEvent(event)

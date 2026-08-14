from .._qt import QtCore, QtWidgets
from .utils import TIME_SLIDER_COLOR


class TimeSliderContainer(QtWidgets.QWidget):
    def __init__(self, container_height=10):
        super().__init__()
        self.slider = TimeSlider(self)

        self.setFixedHeight(container_height)
        self.slider._container_height = container_height
        self.setStyleSheet("background: transparent")

    def resizeEvent(self, event):
        super().resizeEvent(event)
        new_width = event.size().width()
        self.slider.setExpanded(False, new_width)


class TimeSlider(QtWidgets.QSlider):
    cutoff_changed = QtCore.pyqtSignal(int)

    def __init__(
        self,
        parent=None,
        expanded_height: int = 6,
        collapsed_height: int = 2,
        container_height: int = 10,
    ):
        super().__init__(QtCore.Qt.Orientation.Horizontal, parent)

        self._container_height = container_height
        self.expanded_height = expanded_height
        self.collapsed_height = collapsed_height

        self.valueChanged.connect(self.rollback_target)

        self.setTickInterval(1)
        self.setMinimum(0)
        self.setValue(self.maximum())

        self._set_stylesheet()

    def is_following_latest(self):
        """Return whether the slider is at its maximum, where it has the meaning of
        following points as they come in (cutoff -1).
        """
        return self.target_idx == -1

    def update_points(self, point_data: dict[str, list]):
        num_points = len(next(iter(point_data.values()), []))
        self.setMaximum(max(0, num_points - 1))

        if self.is_following_latest():
            self.setValue(self.maximum())

    def rollback_target(self, index):
        if index == self.maximum():
            # When scrubbing all the way to the right, switch to the "sticky" following
            # mode.
            self.target_idx = -1
        else:
            self.target_idx = index

        self.cutoff_changed.emit(self.target_idx)

    def _set_stylesheet(self):
        self.setStyleSheet(f"""
            QSlider::add-page:horizontal {{
            background: rgba(0, 0, 0, 200);
            }}
            QSlider::handle:horizontal {{
            background: transparent;
                border: none;
            }}
            QSlider::sub-page:horizontal {{
                background: {TIME_SLIDER_COLOR};
            }}
            QSlider::handle:horizontal::hover {{
            background: #fff;
                border-radius: 3px;
            }}
        """)

    def setExpanded(self, expanded: bool, width):
        height = self.expanded_height if expanded else self.collapsed_height
        y_pos = self._container_height - height

        self.setGeometry(0, y_pos, width, self.expanded_height)

    def enterEvent(self, event):
        self.setExpanded(True, self.parent().width())
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.setExpanded(False, self.parent().width())
        super().leaveEvent(event)

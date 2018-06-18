import logging
import re

from functools import partial
from typing import List, Tuple
from PyQt5 import QtCore, QtGui, QtWidgets
from artiq.gui.tools import LayoutWidget

logger = logging.getLogger(__name__)


class FuzzySelectWidget(LayoutWidget):
    aborted = QtCore.pyqtSignal()
    finished = QtCore.pyqtSignal(str)

    def __init__(self, choices: List[Tuple[str, int]] = [], *args):
        super().__init__(*args)
        self.choices = choices

        self.line_edit = QtWidgets.QLineEdit(self)
        self.layout.addWidget(self.line_edit)
        
        # self.setFocusProxy(self.line_edit)
        line_edit_focus_filter = _FocusEventFilter(self.line_edit)
        line_edit_focus_filter.focus_gained.connect(self._activate)
        line_edit_focus_filter.focus_lost.connect(self._line_edit_focus_lost)
        self.line_edit.installEventFilter(line_edit_focus_filter)
        self.line_edit.textChanged.connect(self._update_menu)

        escape_filter = _EscapeKeyFilter(self)
        escape_filter.escape_pressed.connect(self._abort)
        self.line_edit.installEventFilter(escape_filter)

        self.menu = None

        self.update_when_text_changed = True
        self.menu_typing_filter = None
        self.line_edit_up_down_filter = None
        self.abort_when_menu_hidden = False
        self.abort_when_line_edit_unfocussed = True

    def set_choices(self, choices):
        self.choices = choices
        if self.menu:
            self._update_menu()

    def _activate(self):
        self.update_when_text_changed = True
        if not self.menu:
            self._update_menu()

    def _ensure_menu(self):
        if self.menu:
            return

        self.menu = QtWidgets.QMenu(self)

        # Display menu with search results beneath line edit.
        menu_pos = self.line_edit.mapToGlobal(self.line_edit.pos())
        menu_pos.setY(menu_pos.y() + self.line_edit.height())

        self.menu.popup(menu_pos)
        self.menu.aboutToHide.connect(self._menu_hidden)

    def _menu_hidden(self):
        if self.abort_when_menu_hidden:
            self.abort_when_menu_hidden = False
            self._abort()

    def _line_edit_focus_lost(self):
        if self.abort_when_line_edit_unfocussed:
            self._abort()

    def _update_menu(self):
        if not self.update_when_text_changed:
            return

        filtered_choices = self._filter_choices()

        if not filtered_choices:
            # No matches, don't display menu at all.
            if self.menu:
                self.abort_when_menu_hidden = False
                self.menu.close()
            self.menu = None
            self.abort_when_line_edit_unfocussed = True
            self.line_edit.setFocus()
            return

        # We are going to end up with a menu shown and the line edit losing focus.
        self.abort_when_line_edit_unfocussed = False

        if self.menu:
            # Hide menu temporarily to avoid re-layouting on every added item.
            self.abort_when_menu_hidden = False
            self.menu.hide()
            self.menu.clear()

        self._ensure_menu()

        first_action = None
        last_action = None
        for choice in filtered_choices:
            action = QtWidgets.QAction(choice, self.menu)
            action.triggered.connect(partial(self._finish, choice))
            self.menu.addAction(action)
            if not first_action:
                first_action = action
            last_action = action

        if self.menu_typing_filter:
            self.menu.removeEventFilter(self.menu_typing_filter)
        self.menu_typing_filter = _NonUpDownKeyFilter(self.menu, self.line_edit)
        self.menu.installEventFilter(self.menu_typing_filter)

        if self.line_edit_up_down_filter:
            self.line_edit.removeEventFilter(self.line_edit_up_down_filter)
        self.line_edit_up_down_filter = _UpDownKeyFilter(self.line_edit, self.menu, first_action, last_action)
        self.line_edit.installEventFilter(self.line_edit_up_down_filter)

        self.abort_when_menu_hidden = True
        self.menu.show()
        self.menu.setActiveAction(first_action)
        self.menu.setFocus()

    def _filter_choices(self):
        # TODO: More SublimeText-like taking capital letters/punctuation into account.
        suggestions = []
        pat = '.*?'.join(map(re.escape, self.line_edit.text().lower()))
        regex = re.compile(pat)
        for (item, weight) in self.choices:
            r = regex.search(item.lower())
            if r:
                suggestions.append((len(r.group()) + weight, r.start(), item))
        return [x for _, _, x in sorted(suggestions)]

    def _close(self):
        if self.menu:
            self.menu.close()
            self.menu = None
        self.update_when_text_changed = False
        self.line_edit.clear()

    def _abort(self):
        self._close()
        self.aborted.emit()

    def _finish(self, name):
        self._close()
        self.finished.emit(name)


class _FocusEventFilter(QtCore.QObject):
    focus_gained = QtCore.pyqtSignal()
    focus_lost = QtCore.pyqtSignal()

    def eventFilter(self, obj, event):
        if event.type() == QtCore.QEvent.FocusIn:
            self.focus_gained.emit()
        elif event.type() == QtCore.QEvent.FocusOut:
            self.focus_lost.emit()
        return False


class _EscapeKeyFilter(QtCore.QObject):
    escape_pressed = QtCore.pyqtSignal()

    def eventFilter(self, obj, event):
        if event.type() == QtCore.QEvent.KeyPress:
            if event.key() == QtCore.Qt.Key_Escape:
                self.escape_pressed.emit()
        return False


class _UpDownKeyFilter(QtCore.QObject):
    def __init__(self, parent, menu, first_item, last_item):
        super().__init__(parent)
        self.menu = menu
        self.first_item = first_item
        self.last_item = last_item

    def eventFilter(self, obj, event):
        if event.type() == QtCore.QEvent.KeyPress:
            if event.key() == QtCore.Qt.Key_Down:
                self.menu.setActiveAction(self.first_item)
                self.menu.setFocus()
                return True

            if event.key() == QtCore.Qt.Key_Up:
                self.menu.setActiveAction(self.last_item)
                self.menu.setFocus()
                return True
        return False


class _NonUpDownKeyFilter(QtCore.QObject):
    def __init__(self, parent, target):
        super().__init__(parent)
        self.target = target

    def eventFilter(self, obj, event):
        if event.type() == QtCore.QEvent.KeyPress:
            k = event.key()
            if k != QtCore.Qt.Key_Down and k != QtCore.Qt.Key_Up and k != QtCore.Qt.Key_Enter \
                    and k != QtCore.Qt.Key_Return:
                QtWidgets.QApplication.sendEvent(self.target, event)
                return True
        return False
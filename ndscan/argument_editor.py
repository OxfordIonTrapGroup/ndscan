import logging

from PyQt5 import QtCore, QtGui, QtWidgets


logger = logging.getLogger(__name__)

class ArgumentEditor(QtWidgets.QTreeWidget):
	def __init__(self, manager, dock, expurl):
		super().__init__()

	def save_state(self):
		pass

	def restore_state(self, state):
		pass

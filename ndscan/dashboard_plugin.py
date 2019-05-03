"""ARTIQ dashboard plugin; loads an argument editor to  edit ndscan parameters.

Pass to ``artiq_dashboard`` using the ``--load-plugin/-p`` switch to load, e.g.::

    artiq_dashboard -s 10.255.6.191 -p ndscan.dashboard_plugin
"""

from artiq.dashboard.experiments import ExperimentManager
from .dashboard.argument_editor import ArgumentEditor

ExperimentManager.argument_ui_classes["ndscan"] = ArgumentEditor

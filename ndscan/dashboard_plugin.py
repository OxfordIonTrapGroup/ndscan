from artiq.dashboard.experiments import ExperimentManager
from .argument_editor import ArgumentEditor

ExperimentManager.argument_ui_classes["ndscan"] = ArgumentEditor

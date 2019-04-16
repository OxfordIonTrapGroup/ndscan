"""
Common fragments/… for unit tests.
"""

import numpy
from artiq.experiment import *
from ndscan.experiment import *


class AddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("value", FloatParam, "Value to return", 0.0)
        self.setattr_result("result", FloatChannel)

        self.num_host_setup_calls = 0
        self.num_device_setup_calls = 0

    def host_setup(self):
        self.num_host_setup_calls += 1

    def device_setup(self):
        self.num_device_setup_calls += 1

    def run_once(self):
        self.result.push(self.value.get() + 1)

    def get_default_analyses(self):
        # Nonsensical fit spec to exercise serialisation code.
        return [OnlineFit("lorentzian", {"x": self.value, "y": self.result})]


class ReboundAddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("add_one", AddOneFragment)
        self.setattr_param_rebind("value", self.add_one)

    def run_once(self):
        self.add_one.run_once()


class AddOneCustomAnalysisFragment(AddOneFragment):
    def get_default_analyses(self):
        return [CustomAnalysis({self.value}, self._analyze)]

    def _analyze(self, axis_values, result_values):
        return [
            Annotation("location", {self.value: numpy.mean(axis_values[self.value])}),
            Annotation("location",
                       {self.result: numpy.mean(result_values[self.result])})
        ]


class TrivialKernelFragment(ExpFragment):
    def build_fragment(self):
        pass

    @kernel
    def device_setup(self):
        pass

    @kernel
    def run_once(self):
        pass

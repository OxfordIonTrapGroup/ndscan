"""
Common fragments/… for unit tests.
"""

import numpy
from ndscan.experiment import *


class AddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("value", FloatParam, "Value to return", 0.0)
        self.setattr_result("result", FloatChannel)

        self.num_host_setup_calls = 0
        self.num_device_setup_calls = 0
        self.num_host_cleanup_calls = 0
        self.num_device_cleanup_calls = 0

    def host_setup(self):
        self.num_host_setup_calls += 1

    def device_setup(self):
        self.num_device_setup_calls += 1

    def host_cleanup(self):
        self.num_host_cleanup_calls += 1

    def device_cleanup(self):
        self.num_device_cleanup_calls += 1

    def run_once(self):
        self.result.push(self.value.get() + 1)

    def get_default_analyses(self):
        # Nonsensical fit spec to exercise serialisation code.
        return [
            OnlineFit(
                "lorentzian",
                {
                    "x": self.value,
                    "y": self.result
                },
                constants={"y0": 1.0},
                initial_values={"fwhm": 2.0},
            )
        ]


class ReboundAddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("add_one", AddOneFragment)
        self.setattr_param_rebind("value", self.add_one)

    def run_once(self):
        self.add_one.run_once()


class ReboundReboundAddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("rebound_add_one", ReboundAddOneFragment)
        self.setattr_param_rebind("value", self.rebound_add_one)

    def run_once(self):
        self.rebound_add_one.run_once()


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


class TransitoryErrorFragment(ExpFragment):
    def build_fragment(self):
        self.num_device_setup_to_fail = 0
        self.num_device_setup_to_restart_fail = 0
        self.num_run_once_to_fail = 0
        self.num_run_once_to_restart_fail = 0
        self.setattr_result("result", IntChannel)

    def device_setup(self):
        if self.num_device_setup_to_restart_fail > 0:
            self.num_device_setup_to_restart_fail -= 1
            raise RestartKernelTransitoryError
        if self.num_device_setup_to_fail > 0:
            self.num_device_setup_to_fail -= 1
            raise TransitoryError

    def run_once(self):
        if self.num_run_once_to_restart_fail > 0:
            self.num_run_once_to_restart_fail -= 1
            raise RestartKernelTransitoryError
        if self.num_run_once_to_fail > 0:
            self.num_run_once_to_fail -= 1
            raise TransitoryError
        self.result.push(42)


class RequestTerminationFragment(ExpFragment):
    """To test handling of TerminationRequested exceptions without having to implement
    actual termination requests in a mock scheduler, raises TerminationRequested as
    soon as it is run.
    """
    def build_fragment(self):
        pass

    def run_once(self):
        raise TerminationRequested


class TwoAnalysisFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("a", FloatParam, "a", 0.0)
        self.setattr_param("b", FloatParam, "b", 0.0)

    def get_default_analyses(self):
        return [
            CustomAnalysis([self.a], lambda *_: []),
            CustomAnalysis([self.b], lambda *_: [])
        ]

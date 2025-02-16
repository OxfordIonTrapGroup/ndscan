"""
Common fragments/… for unit tests.
"""

import numpy
from ndscan.experiment import *


class AddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("value", FloatParam, "Value to return", 0.0)
        self.setattr_result("result", FloatChannel)

        self.num_prepare_calls = 0
        self.num_host_setup_calls = 0
        self.num_device_setup_calls = 0
        self.num_host_cleanup_calls = 0
        self.num_device_cleanup_calls = 0

    def prepare(self):
        self.num_prepare_calls += 1

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
        self.setattr_param_rebind("value", self.add_one, unit="ms")

    def run_once(self):
        self.add_one.run_once()


class ReboundReboundAddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("rebound_add_one", ReboundAddOneFragment)
        self.setattr_param_rebind("value", self.rebound_add_one)

    def run_once(self):
        self.rebound_add_one.run_once()


class MultiReboundAddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("first", AddOneFragment)
        self.setattr_fragment("second", AddOneFragment)
        self.setattr_param_like("value", self.first)
        self.first.bind_param("value", self.value)
        self.second.bind_param("value", self.value)

    def run_once(self):
        self.first.run_once()
        self.second.run_once()


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
    """Fails device_setup() and run_once() a configurable number of times with a
    transitory error before succeeding.
    """
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


class MultiPointTransitoryErrorFragment(TransitoryErrorFragment):
    """TransitoryErrorFragment that resets counters after run_once() has completed
    successfully (for testing scan behaviour).
    """
    def build_fragment(self,
                       num_device_setup_to_fail=0,
                       num_device_setup_to_restart_fail=0,
                       num_run_once_to_fail=0,
                       num_run_once_to_restart_fail=0):
        super().build_fragment()
        self.orig_num_device_setup_to_fail = num_device_setup_to_fail
        self.orig_num_device_setup_to_restart_fail = num_device_setup_to_restart_fail
        self.orig_num_run_once_to_fail = num_run_once_to_fail
        self.orig_num_run_once_to_restart_fail = num_run_once_to_restart_fail
        self.reset_counters()

    def reset_counters(self):
        self.num_device_setup_to_fail = self.orig_num_device_setup_to_fail
        self.num_device_setup_to_restart_fail = \
            self.orig_num_device_setup_to_restart_fail
        self.num_run_once_to_fail = self.orig_num_run_once_to_fail
        self.num_run_once_to_restart_fail = self.orig_num_run_once_to_restart_fail

    def run_once(self):
        super().run_once()
        self.reset_counters()


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
        def make_analyse(channel_name):
            def analyse(coords, values, results):
                # Explicitly specify the channel name instead of just taking the only
                # value to make sure the names work as expected from the user side.
                results[channel_name].push(42.0)
                return []

            return analyse

        return [
            CustomAnalysis([self.a], make_analyse("result_a"),
                           [FloatChannel("result_a")]),
            CustomAnalysis([self.b], make_analyse("result_b"),
                           [FloatChannel("result_b")])
        ]


class AddOneAggregate(AggregateExpFragment):
    def build_fragment(self) -> None:
        self.setattr_fragment("a", AddOneFragment)
        self.setattr_fragment("b", AddOneFragment)

        self.setattr_result("sum", FloatChannel)

        def push_sum():
            self.sum.push(self.a.value.get() + 1 + self.b.value.get() + 1)

        return super().build_fragment([self.a, self.b, push_sum])


class TrivialKernelAggregate(AggregateExpFragment):
    def build_fragment(self) -> None:
        self.setattr_fragment("a", TrivialKernelFragment)
        self.setattr_fragment("b", TrivialKernelFragment)
        return super().build_fragment([self.a, self.b])


class TwoAnalysisAggregate(AggregateExpFragment):
    def build_fragment(self) -> None:
        self.setattr_fragment("first", TwoAnalysisFragment)
        self.setattr_fragment("second", TwoAnalysisFragment)
        self.setattr_param_rebind("a", self.first)
        self.second.bind_param("a", self.a)
        return super().build_fragment([self.first, self.second])


class ReadParamDefault(ExpFragment):
    def build_fragment(self):
        self.setattr_param("foo", IntParam, "Foo", default="dataset('foo', 0)")
        self.setattr_result("value", IntChannel)

    def run_once(self) -> None:
        self.value.push(self.foo.get())

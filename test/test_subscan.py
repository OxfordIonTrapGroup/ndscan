"""
Tests for subscan functionality.
"""

from ndscan.fragment import *
from ndscan.scan_generator import LinearGenerator
from ndscan.subscan import setattr_subscan

from mock_environment import ExpFragmentCase


class EchoFragment(ExpFragment):
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


class Scan1DFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("child", EchoFragment)
        setattr_subscan(self, "scan", self.child, [(self.child, "value")])

    def run_once(self):
        return self.scan.run([(self.child.value, LinearGenerator(0, 3, 4, False))])


class SubscanCase(ExpFragmentCase):
    def test_1d_subscan_return(self):
        parent = self.create(Scan1DFragment)
        coords, values = parent.run_once()

        expected_values = [float(n) for n in range(0, 4)]
        expected_results = [v + 1 for v in expected_values]
        self.assertEqual(coords, {parent.child.value: expected_values})
        self.assertEqual(values, {parent.child.result: expected_results})

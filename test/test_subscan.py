"""
Tests for subscan functionality.
"""

from ndscan.fragment import *
from ndscan.scan_generator import LinearGenerator
from ndscan.subscan import setattr_subscan

from fixtures import EchoFragment
from mock_environment import ExpFragmentCase


class Scan1DFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("child", EchoFragment)
        scan = setattr_subscan(self, "scan", self.child, [(self.child, "value")])
        assert self.scan == scan

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

import unittest
from ndscan.experiment.default_analysis import CustomAnalysis
from ndscan.experiment.scan_runner import ScanAxis, match_default_analysis
from ndscan.experiment import parameters


class CustomAnalysisTestCase(unittest.TestCase):
    def test_axis_matching(self):
        foo = parameters.FloatParamHandle(None, "foo")
        foo.set_store(parameters.FloatParamStore(("Fragment.foo", "*"), 0.0))
        bar = parameters.FloatParamHandle(None, "bar")
        bar.set_store(parameters.FloatParamStore(("Fragment.bar", "*"), 1.0))

        def make_axes(*axes):
            return [ScanAxis(None, None, ax._store) for ax in axes]

        a = CustomAnalysis([foo], lambda *a: [])
        self.assertTrue(match_default_analysis(a, make_axes(foo)))
        self.assertFalse(match_default_analysis(a, make_axes(bar)))
        self.assertFalse(match_default_analysis(a, make_axes(foo, bar)))

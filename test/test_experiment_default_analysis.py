import unittest
from ndscan.experiment.default_analysis import CustomAnalysis
from ndscan.experiment import parameters


class CustomAnalysisTestCase(unittest.TestCase):
    def test_has_data(self):
        foo = parameters.FloatParamHandle(None, "foo")
        foo.set_store(parameters.FloatParamStore(("Fragment.foo", "*"), 0.0))
        bar = parameters.FloatParamHandle(None, "bar")
        bar.set_store(parameters.FloatParamStore(("Fragment.bar", "*"), 1.0))

        a = CustomAnalysis([foo], lambda *a: [])
        self.assertTrue(a.has_data([foo._store.identity]))
        self.assertFalse(a.has_data([bar]))
        self.assertFalse(a.has_data([foo, bar]))

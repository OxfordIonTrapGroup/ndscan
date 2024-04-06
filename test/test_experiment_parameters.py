import unittest
from ndscan.experiment.parameters import FloatParam, IntParam, BoolParam


class FloatParamCase(unittest.TestCase):
    def test_describe(self):
        param = FloatParam("foo", "bar", 1.0, min=0.0, max=2.0, unit="baz", scale=1.0)
        self.assertEqual(
            param.describe(), {
                "fqn": "foo",
                "description": "bar",
                "default": "1.0",
                "spec": {
                    "min": 0.0,
                    "max": 2.0,
                    "unit": "baz",
                    "scale": 1.0,
                    "step": 0.1,
                    "is_scannable": True,
                },
                "type": "float"
            })

    def test_evaluate_default(self):
        def mock_get_dataset(key: str, default=None):
            return {"baz": 42.0}[key]

        param = FloatParam("foo", "bar", 0.0)
        self.assertEqual(param.eval_default(mock_get_dataset), 0.0)

        param = FloatParam("foo", "bar", "dataset('baz', 0.0)")
        self.assertEqual(param.eval_default(mock_get_dataset), 42.0)


class IntParamCase(unittest.TestCase):
    def test_describe(self):
        param = IntParam("foo", "bar", 0, min=-1, max=1, unit="baz", scale=1)
        self.assertEqual(
            param.describe(), {
                "fqn": "foo",
                "description": "bar",
                "default": "0",
                "spec": {
                    "min": -1,
                    "max": 1,
                    "unit": "baz",
                    "scale": 1,
                    "is_scannable": True,
                },
                "type": "int"
            })

    def test_evaluate_default(self):
        def mock_get_dataset(key: str, default=None):
            return {"baz": 42}[key]

        param = IntParam("foo", "bar", 0)
        self.assertEqual(param.eval_default(mock_get_dataset), 0)

        param = IntParam("foo", "bar", "dataset('baz', 0)")
        self.assertEqual(param.eval_default(mock_get_dataset), 42)


class BoolParamCase(unittest.TestCase):
    def test_describe(self):
        param = BoolParam("foo", "bar", True)
        self.assertEqual(
            param.describe(), {
                "fqn": "foo",
                "description": "bar",
                "type": "bool",
                "default": "True",
                "spec": {
                    "is_scannable": True
                }
            })

    def test_evaluate_default(self):
        def mock_get_dataset(key: str, default=None):
            return {"baz": True}[key]

        param = BoolParam("foo", "bar", True)
        self.assertEqual(param.eval_default(mock_get_dataset), True)

        param = BoolParam("foo", "bar", "dataset('baz', False)")
        self.assertEqual(param.eval_default(mock_get_dataset), True)

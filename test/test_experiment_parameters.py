import unittest
from ndscan.experiment.parameters import (FloatParam, IntParam, BoolParam,
                                          enum_param_factory)
from enum import Enum


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
            return {"baz": 42.}[key]

        param = FloatParam("foo", "bar", 0.)
        self.assertEqual(param.eval_default(mock_get_dataset), 0.)

        param = FloatParam("foo", "bar", "dataset('baz', 0.)")
        self.assertEqual(param.eval_default(mock_get_dataset), 42.)


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


class EnumParamCase(unittest.TestCase):
    def test_describe(self):
        class Options(Enum):
            first = "A"
            second = "B"
            third = "C"

        (EnumParam, _, _) = enum_param_factory(Options)

        param = EnumParam("foo", "bar", Options.second)
        self.assertEqual(
            param.describe(), {
                "fqn": "foo",
                "description": "bar",
                "type": f"enum_Options_{id(Options)}",
                "default": "'second'",
                "spec": {
                    "enum_display_map": {o.name: o.value
                                         for o in Options},
                    "is_scannable": True
                }
            })
        param = EnumParam("foo", "bar", "'second'")
        self.assertEqual(
            param.describe(), {
                "fqn": "foo",
                "description": "bar",
                "type": f"enum_Options_{id(Options)}",
                "default": "'second'",
                "spec": {
                    "enum_display_map": {o.name: o.value
                                         for o in Options},
                    "is_scannable": True
                }
            })

    def test_evaluate_default(self):
        class StrOptions(Enum):
            first = "A"
            second = "B"
            third = "C"

        class IntOptions(Enum):
            first = 1
            second = 2
            third = 3

        (StrEnumParam, _, _) = enum_param_factory(StrOptions)
        (IntEnumParam, _, _) = enum_param_factory(IntOptions)

        def mock_get_dataset(key: str, default=None):
            try:
                return {"baz": "third"}[key]
            except KeyError:
                return default

        str_param = StrEnumParam("foo", "bar", StrOptions.second)
        self.assertEqual(str_param.eval_default(mock_get_dataset), StrOptions.second)

        str_param = StrEnumParam("foo", "bar", "dataset('baz', 'first')")
        self.assertEqual(str_param.eval_default(mock_get_dataset), StrOptions.third)

        str_param = StrEnumParam("foo", "bar", "dataset('bam', 'first')")
        self.assertEqual(str_param.eval_default(mock_get_dataset), StrOptions.first)

        int_param = IntEnumParam("foo", "bar", IntOptions.second)
        self.assertEqual(int_param.eval_default(mock_get_dataset), IntOptions.second)

        int_param = IntEnumParam("foo", "bar", "dataset('baz', 'first')")
        self.assertEqual(int_param.eval_default(mock_get_dataset), IntOptions.third)

        int_param = IntEnumParam("foo", "bar", "dataset('bam', 'first')")
        self.assertEqual(int_param.eval_default(mock_get_dataset), IntOptions.first)

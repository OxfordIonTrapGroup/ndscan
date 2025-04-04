from mock_environment import HasEnvironmentCase
from enum import Enum, unique
import unittest
from ndscan.experiment.fragment import Fragment
from ndscan.experiment.parameters import (FloatParam, IntParam, StringParam, BoolParam,
                                          EnumParam)


def eval_if_str(x):
    if isinstance(x, str):
        return eval(x)
    return x


# "Container class" to hide base class from unittest auto-discovery.
class GenericBase:
    class Cases(HasEnvironmentCase):
        EXTRA_KWARGS = {}

        def to_dataset_value(self, x):
            """Hook for parameter types that require a dataset (get_dataset(), etc.)
            representation that is not just the value itself.
            """
            return x

        def to_dataset_fn_arg(self, x):
            """Hook for parameter types that require a default=dataset(key, <…>)
            representation that is not just the string-formatted value itself.
            """
            return x

        def test_describe(self):
            param = self.CLASS("foo", **(self.EXTRA_KWARGS | self.EXAMPLE_KWARGS))
            self.assertEqual(param.describe(),
                             self.EXPECTED_DESCRIPTION | {"fqn": "foo"})

        def test_evaluate_default(self):
            def mock_get_dataset(key: str, default=None):
                return {"baz": self.to_dataset_value(self.DEFAULT_1)}[key]

            param = self.CLASS("foo", "bar", self.DEFAULT_0, **self.EXTRA_KWARGS)
            self.assertEqual(param.eval_default(mock_get_dataset),
                             eval_if_str(self.DEFAULT_0))

            param = self.CLASS(
                "foo", "bar",
                f"dataset('baz', {self.to_dataset_fn_arg(self.DEFAULT_0)})",
                **self.EXTRA_KWARGS)
            self.assertEqual(param.eval_default(mock_get_dataset),
                             eval_if_str(self.DEFAULT_1))

        def test_rebind(self):
            class Foo(Fragment):
                def build_fragment(inner_self) -> None:
                    inner_self.setattr_param("bar", self.CLASS, **self.EXAMPLE_KWARGS,
                                             **self.EXTRA_KWARGS)
                    inner_self.setattr_param_rebind("baz", inner_self, "bar")

            foo = self.create(Foo, [])
            schemata = {}
            foo._collect_params({}, schemata, {})
            fqn = next(iter(schemata.keys()))
            self.assertTrue(fqn.endswith("Foo.baz"))
            self.assertEqual(schemata[fqn], self.EXPECTED_DESCRIPTION | {"fqn": fqn})
            self.assertEqual(foo.bar.parameter, foo.baz.parameter)


class FloatParamCase(GenericBase.Cases):
    CLASS = FloatParam
    DEFAULT_0 = 1.0
    DEFAULT_1 = 42.0
    EXAMPLE_KWARGS = {
        "description": "bar",
        "default": 1.0,
        "min": 0.0,
        "max": 2.0,
        "unit": "baz",
        "scale": 1.0
    }
    EXPECTED_DESCRIPTION = {
        "description": "bar",
        "default": "1.0",
        "type": "float",
        "spec": {
            "min": 0.0,
            "max": 2.0,
            "unit": "baz",
            "scale": 1.0,
            "step": 0.1,
            "is_scannable": True
        }
    }


class IntParamCase(GenericBase.Cases):
    CLASS = IntParam
    DEFAULT_0 = 1
    DEFAULT_1 = 42
    EXAMPLE_KWARGS = {
        "description": "bar",
        "default": 0,
        "min": -1,
        "max": 1,
        "unit": "baz",
        "scale": 1
    }
    EXPECTED_DESCRIPTION = {
        "description": "bar",
        "default": "0",
        "type": "int",
        "spec": {
            "min": -1,
            "max": 1,
            "unit": "baz",
            "scale": 1,
            "is_scannable": True,
        },
    }


class StringParamCase(GenericBase.Cases):
    CLASS = StringParam
    DEFAULT_0 = "'foo'"
    DEFAULT_1 = "'bar'"
    EXAMPLE_KWARGS = {
        "description": "baz",
        "default": "'foo'",
    }
    EXPECTED_DESCRIPTION = {
        "description": "baz",
        "default": "'foo'",
        "type": "string",
        "spec": {
            "is_scannable": True,
        },
    }

    def to_dataset_value(self, x):
        # The whole "eval strings as code" business only applies to the experiment-side
        # APIs and schema descriptions. Once dealing with datasets, though, StringParams
        # are just strings without an extra Python code evaluation layer. Thus, need to
        # eval the above strings, which have extra quotes so they can be put into
        # constructor arguments and dataset(key, <…>) calls, back to plain strings.
        return eval(x)


class BoolParamCase(GenericBase.Cases):
    CLASS = BoolParam
    DEFAULT_0 = False
    DEFAULT_1 = True
    EXAMPLE_KWARGS = {
        "description": "bar",
        "default": True,
    }
    EXPECTED_DESCRIPTION = {
        "description": "bar",
        "default": "True",
        "type": "bool",
        "spec": {
            "is_scannable": True,
        },
    }


@unique
class Options(Enum):
    first = "A"
    second = "B"
    third = "C"


class EnumParamElemCase(GenericBase.Cases):
    CLASS = EnumParam
    EXTRA_KWARGS = {"enum_class": Options}
    DEFAULT_0 = Options.second
    DEFAULT_1 = Options.third
    EXAMPLE_KWARGS = {
        "description": "bar",
        "default": Options.first,
    }
    EXPECTED_DESCRIPTION = {
        "description": "bar",
        "type": "enum",
        "default": "'first'",
        "spec": {
            "members": {o.name: o.value
                        for o in Options},
            "is_scannable": True
        }
    }

    def to_dataset_value(self, x):
        return x.name

    def to_dataset_fn_arg(self, x):
        return f"'{x.name}'"


class EnumParamStringCase(GenericBase.Cases):
    CLASS = EnumParam
    EXTRA_KWARGS = {"enum_class": Options}
    DEFAULT_0 = Options.second
    DEFAULT_1 = Options.third
    EXAMPLE_KWARGS = {
        "description": "bar",
        "default": "'first'",
    }
    EXPECTED_DESCRIPTION = {
        "fqn": "foo",
        "description": "bar",
        "type": "enum",
        "default": "'first'",
        "spec": {
            "members": {o.name: o.value
                        for o in Options},
            "is_scannable": True
        }
    }

    def to_dataset_value(self, x):
        return x.name

    def to_dataset_fn_arg(self, x):
        return f"'{x.name}'"


class EnumParamCase(unittest.TestCase):
    def test_evaluate_default(self):
        class StrOptions(Enum):
            first = "A"
            second = "B"
            third = "C"

        class IntOptions(Enum):
            first = 1
            second = 2
            third = 3

        def mock_get_dataset(key: str, default=None):
            try:
                return {"baz": "third"}[key]
            except KeyError:
                return default

        str_param = EnumParam("foo", "bar", StrOptions.second)
        self.assertEqual(str_param.eval_default(mock_get_dataset), StrOptions.second)

        str_param = EnumParam("foo", "bar", "dataset('baz', 'first')", StrOptions)
        self.assertEqual(str_param.eval_default(mock_get_dataset), StrOptions.third)

        str_param = EnumParam("foo", "bar", "dataset('bam', 'first')", StrOptions)
        self.assertEqual(str_param.eval_default(mock_get_dataset), StrOptions.first)

        int_param = EnumParam("foo", "bar", IntOptions.second)
        self.assertEqual(int_param.eval_default(mock_get_dataset), IntOptions.second)

        int_param = EnumParam("foo", "bar", "dataset('baz', 'first')", IntOptions)
        self.assertEqual(int_param.eval_default(mock_get_dataset), IntOptions.third)

        int_param = EnumParam("foo", "bar", "dataset('bam', 'first')", IntOptions)
        self.assertEqual(int_param.eval_default(mock_get_dataset), IntOptions.first)

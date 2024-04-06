from mock_environment import HasEnvironmentCase
from ndscan.experiment.fragment import Fragment
from ndscan.experiment.parameters import FloatParam, IntParam, BoolParam


# "Container class" to hide base class from unittest auto-discovery.
class GenericBase:
    class Cases(HasEnvironmentCase):
        def test_describe(self):
            param = self.CLASS("foo", **self.EXAMPLE_KWARGS)
            self.assertEqual(param.describe(),
                             self.EXPECTED_DESCRIPTION | {"fqn": "foo"})

        def test_evaluate_default(self):
            def mock_get_dataset(key: str, default=None):
                return {"baz": self.DEFAULT_1}[key]

            param = self.CLASS("foo", "bar", self.DEFAULT_0)
            self.assertEqual(param.eval_default(mock_get_dataset), self.DEFAULT_0)

            param = self.CLASS("foo", "bar", f"dataset('baz', {self.DEFAULT_0})")
            self.assertEqual(param.eval_default(mock_get_dataset), self.DEFAULT_1)

        def test_rebind(self):
            class Foo(Fragment):
                def build_fragment(inner_self) -> None:
                    inner_self.setattr_param("bar", self.CLASS, **self.EXAMPLE_KWARGS)
                    inner_self.setattr_param_rebind("baz", inner_self, "bar")

            foo = self.create(Foo, [])
            schemata = {}
            foo._collect_params({}, schemata)
            fqn = next(iter(schemata.keys()))
            self.assertTrue(fqn.endswith("Foo.baz"))
            self.assertEqual(schemata[fqn], self.EXPECTED_DESCRIPTION | {"fqn": fqn})


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

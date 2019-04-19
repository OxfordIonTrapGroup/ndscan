"""
Tests for general fragment tree behaviour.
"""

from ndscan.experiment import *
from ndscan.experiment.parameters import IntParamStore
from mock_environment import HasEnvironmentCase


class DatasetDefaultFragment(Fragment):
    def build_fragment(self):
        self.setattr_param("foo", IntParam, "Foo", default="dataset('foo', 1)")
        self.setattr_param("bar", IntParam, "Bar", default="dataset('bar', 2)")


class TestParamDefaults(HasEnvironmentCase):
    def test_nonexistent_datasets(self):
        ddf = self.create(DatasetDefaultFragment, [])
        ddf.init_params()
        self.assertEqual(ddf.foo.get(), 1)
        self.assertEqual(ddf.bar.get(), 2)

    def test_datasets(self):
        ddf = self.create(DatasetDefaultFragment, [])
        self.dataset_db.data["foo"] = (False, 3)
        self.dataset_db.data["bar"] = (False, 4)

        store = IntParamStore("...", 5)
        ddf.init_params({ddf.fqn + ".bar": [("*", store)]})
        self.assertEqual(ddf.foo.get(), 3)
        self.assertEqual(ddf.bar.get(), 5)

        # Make sure recompute_param_defaults() updates params set to their default
        # value, but only those.
        self.dataset_db.data["foo"] = (False, 6)
        ddf.recompute_param_defaults()
        self.assertEqual(ddf.foo.get(), 6)
        self.assertEqual(ddf.bar.get(), 5)

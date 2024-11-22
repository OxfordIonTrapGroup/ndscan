"""
Tests for general fragment tree behaviour.
"""

from ndscan.experiment import *
from ndscan.experiment.parameters import IntParamStore
from fixtures import (AddOneFragment, MultiReboundAddOneFragment,
                      ReboundReboundAddOneFragment)
from mock_environment import HasEnvironmentCase


class DatasetDefaultFragment(Fragment):
    def build_fragment(self):
        self.setattr_param("foo", IntParam, "Foo", default="dataset('foo', 1)")
        self.setattr_param("bar", IntParam, "Bar", default="dataset('bar', 2)")


class DatasetNoFallbackDefaultFragment(Fragment):
    def build_fragment(self):
        self.setattr_param("baz", IntParam, "Baz", default="dataset('baz')")


class TestParamDefaults(HasEnvironmentCase):
    def test_nonexistent_datasets(self):
        ddf = self.create(DatasetDefaultFragment, [])
        ddf.init_params()
        self.assertEqual(ddf.foo.get(), 1)
        self.assertEqual(ddf.bar.get(), 2)

    def test_nonexistent_datasets_in_examine(self):
        # Should not fail due to not being able to set the default.
        ddf = self.create(DatasetDefaultFragment, [], like_examine=True)
        ddf.init_params()
        self.assertEqual(ddf.foo.get(), 1)
        self.assertEqual(ddf.bar.get(), 2)

    def test_nonexistent_datasets_no_default(self):
        dnfdf = self.create(DatasetNoFallbackDefaultFragment, [])
        with self.assertRaises(ValueError):
            dnfdf.init_params()

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


class TransitiveReboundAddOneFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("first", AddOneFragment)
        self.setattr_fragment("second", AddOneFragment)
        self.setattr_fragment("third", AddOneFragment)

        self.setattr_param_like("value", self.first, default=2)

        self.first.bind_param("value", self.value)
        self.second.bind_param("value", self.first.value)
        self.third.bind_param("value", self.second.value)

    def run_once(self):
        self.first.run_once()
        self.second.run_once()
        self.third.run_once()


class TestRebinding(HasEnvironmentCase):
    def test_recursive_rebind_default(self):
        rrf = self.create(ReboundReboundAddOneFragment, [])
        result = run_fragment_once(rrf)[rrf.rebound_add_one.add_one.result]
        self.assertEqual(result, 1)

    def test_recursive_rebind_override(self):
        rrf = self.create(ReboundReboundAddOneFragment, [])
        rrf.override_param("value", 2)
        result = run_fragment_once(rrf)[rrf.rebound_add_one.add_one.result]
        self.assertEqual(result, 3)

    def test_multi_rebind(self):
        mrf = self.create(MultiReboundAddOneFragment, [])
        mrf.override_param("value", 2)
        result = run_fragment_once(mrf)
        self.assertEqual(result[mrf.first.result], 3)
        self.assertEqual(result[mrf.second.result], 3)

    def test_transitive_rebind(self):
        trf = self.create(TransitiveReboundAddOneFragment, [])

        result = run_fragment_once(trf)
        self.assertEqual(result[trf.first.result], 3)
        self.assertEqual(result[trf.second.result], 3)
        self.assertEqual(result[trf.second.result], 3)

    def test_transitive_rebind_with_final_override(self):
        trf = self.create(TransitiveReboundAddOneFragment, [])
        trf.override_param("value", 3)
        result = run_fragment_once(trf)
        self.assertEqual(result[trf.first.result], 4)
        self.assertEqual(result[trf.second.result], 4)
        self.assertEqual(result[trf.second.result], 4)

    def test_transitive_rebind_with_initial_override_fails(self):
        class OverriddenTransitiveReboundAddOneFragment(ExpFragment):
            def build_fragment(self):
                self.setattr_fragment("first", AddOneFragment)
                self.setattr_fragment("second", AddOneFragment)

                self.first.override_param("value", 2)
                self.second.bind_param("value", self.first.value)

            def run_once(self):
                self.first.run_once()
                self.second.run_once()

        with self.assertRaises(AssertionError):
            self.create(OverriddenTransitiveReboundAddOneFragment, [])

    def test_invalid_bind(self):
        class InvalidBindFragment(ExpFragment):
            def build_fragment(self):
                self.setattr_fragment("add_one", AddOneFragment)
                self.setattr_param("value_int", IntParam, "Integer", default=0)
                self.add_one.bind_param("value", self.value_int)

        with self.assertRaises(AssertionError):
            self.create(InvalidBindFragment, [])


class TestMisc(HasEnvironmentCase):
    def test_namespacing(self):
        a = self.create(AddOneFragment, ["a"])
        self.assertEqual(a.make_namespaced_identifier("foo"), "a/foo")
        self.assertEqual(a.make_namespaced_identifier("foo"), "a/foo")
        b = self.create(AddOneFragment, ["b", "c", "d"])
        self.assertEqual(b.make_namespaced_identifier("foo"), "b/c/d/foo")

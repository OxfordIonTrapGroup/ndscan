import unittest
from itertools import permutations

from ndscan.utils import shorten_to_unambiguous_suffixes, strip_prefix, strip_suffix


class StripTest(unittest.TestCase):
    def test_strip_prefix(self):
        self.assertEqual(strip_prefix("foo_bar", "foo_"), "bar")
        self.assertEqual(strip_prefix("foo_bar", "_bar"), "foo_bar")

    def test_strip_suffix(self):
        self.assertEqual(strip_suffix("foo_bar", "foo_"), "foo_bar")
        self.assertEqual(strip_suffix("foo_bar", "_bar"), "foo")


class ShortenTest(unittest.TestCase):
    def test_shorten(self):
        def shorten_at_slash(fqns):
            return shorten_to_unambiguous_suffixes(
                fqns, lambda fqn, n: "/".join(fqn.split("/")[-n:])
            )

        def test(expected):
            # Test all orderings.
            for keys in permutations(expected.keys()):
                self.assertEqual(shorten_at_slash(keys), expected)

        test({})

        test({"foo": "foo"})
        test({"": "", "foo/bar": "foo/bar", "foo/baz": "baz", "baz/bar": "baz/bar"})

        test({"a1/b": "a1/b", "a2/b": "a2/b"})
        test({"a1/b/c": "a1/b/c", "a2/b/c": "a2/b/c"})
        test({"a1/b/c/d": "a1/b/c/d", "a2/b/c/d": "a2/b/c/d"})
        test({"a1/b/c/d/e": "a1/b/c/d/e", "a2/b/c/d/e": "a2/b/c/d/e"})

        test({"bar": "bar", "foo/bar": "foo/bar"})

        # Test repeated fqns.
        with self.assertRaises(ValueError):
            shorten_at_slash(["foo/bar", "foo/bar"])

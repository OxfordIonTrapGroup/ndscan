import unittest
from ndscan.utils import strip_prefix, strip_suffix, shorten_to_unambiguous_suffixes


class StripTest(unittest.TestCase):
    def test_strip_prefix(self):
        self.assertEqual(strip_prefix("foo_bar", "foo_"), "bar")
        self.assertEqual(strip_prefix("foo_bar", "_bar"), "foo_bar")

    def test_strip_suffix(self):
        self.assertEqual(strip_suffix("foo_bar", "foo_"), "foo_bar")
        self.assertEqual(strip_suffix("foo_bar", "_bar"), "foo")


class ShortenTest(unittest.TestCase):
    def test_shorten(self):
        def test(expected):
            self.assertEqual(
                shorten_to_unambiguous_suffixes(
                    expected.keys(), lambda fqn, n: "/".join(fqn.split("/")[-n:])),
                expected)

        test({"foo": "foo"})
        test({"": "", "foo/bar": "foo/bar", "foo/baz": "baz", "baz/bar": "baz/bar"})

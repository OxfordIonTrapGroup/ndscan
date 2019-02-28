import unittest
from artiq.language import kernel
from ndscan.utils import *


class PathMatchingTest(unittest.TestCase):
    PATHS = [a.split("/") for a in ["foo", "a/b", "a/b/c"]]

    def test_simple_match(self):
        for p in self.PATHS:
            for q in self.PATHS:
                self.assertEqual(path_matches_spec(p, "/".join(q)), p == q)

    def test_wildcard(self):
        for p in self.PATHS:
            self.assertTrue(path_matches_spec(p, "*"))


class StripTest(unittest.TestCase):
    def test_strip_prefix(self):
        self.assertEqual(strip_prefix("foo_bar", "foo_"), "bar")
        self.assertEqual(strip_prefix("foo_bar", "_bar"), "foo_bar")

    def test_strip_suffix(self):
        self.assertEqual(strip_suffix("foo_bar", "foo_"), "foo_bar")
        self.assertEqual(strip_suffix("foo_bar", "_bar"), "foo")


def _regular_free_function():
    pass


@kernel
def _kernel_free_function():
    pass


class IsKernelTest(unittest.TestCase):
    def _regular_method(self):
        pass

    @kernel
    def _kernel_method(self):
        pass

    def test_free_function(self):
        self.assertFalse(is_kernel(_regular_free_function))
        self.assertTrue(is_kernel(_kernel_free_function))

    def test_method(self):
        self.assertFalse(is_kernel(self._regular_method))
        self.assertTrue(is_kernel(self._kernel_method))

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


class StripPrefixTest(unittest.TestCase):
    def test(self):
        self.assertEqual(strip_prefix("foobar", "foo"), "bar")
        self.assertEqual(strip_prefix("foobar", "bar"), "foobar")


def _regular_free_function():
    pass


@kernel
def _kernel_free_function():
    pass


class WillSpawnKernelTest(unittest.TestCase):
    def _regular_method(self):
        pass

    @kernel
    def _kernel_method(self):
        pass

    def test_free_function(self):
        self.assertFalse(will_spawn_kernel(_regular_free_function))
        self.assertTrue(will_spawn_kernel(_kernel_free_function))

    def test_method(self):
        self.assertFalse(will_spawn_kernel(self._regular_method))
        self.assertTrue(will_spawn_kernel(self._kernel_method))

import json
import unittest

from sipyco.sync_struct import Notifier

from ndscan.plots.model import Context
from ndscan.plots.model.subscriber import SubscriberRoot


class SinglePointTest(unittest.TestCase):
    def setUp(self):
        self.context = Context()
        self.root = SubscriberRoot("ndscan.", self.context)
        self.datasets = Notifier({
            "ndscan.axes": (False, "[]"),
            "ndscan.channels": (False,
                                json.dumps({
                                    "foo": {
                                        "description": "Foo",
                                        "path": "foo",
                                        "type": "int",
                                        "unit": ""
                                    },
                                    "bar": {
                                        "description": "Bar",
                                        "path": "foo",
                                        "type": "int",
                                        "unit": ""
                                    }
                                }))
        })
        self.pending_mods = []
        self.datasets.publish = lambda a: self.pending_mods.append(a)

    def init(self):
        self.pending_mods = [{
            "action": "init",
            "struct": self.datasets.raw_view.copy()
        }]
        self.sync()

    def sync(self):
        self.root.data_changed(self.datasets.raw_view, self.pending_mods)
        self.pending_mods.clear()

    def test_new_point(self):
        self.init()
        self.datasets["ndscan.point.foo"] = (False, 42)
        self.datasets["ndscan.point.bar"] = (False, 23)
        self.datasets["ndscan.point_phase"] = (False, True)
        self.sync()
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})

    def test_halfway(self):
        self.datasets["ndscan.point.foo"] = (False, 42)
        self.init()

        with self.assertRaises(ValueError):
            # No complete point yet.
            self.root.get_model().get_point()

        self.datasets["ndscan.point.bar"] = (False, 23)
        self.datasets["ndscan.point_phase"] = (False, True)
        self.sync()
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})

    def test_one_and_a_half(self):
        self.datasets["ndscan.point.foo"] = (False, 42)
        self.init()

        with self.assertRaises(ValueError):
            # No complete point yet.
            self.root.get_model().get_point()

        self.datasets["ndscan.point.bar"] = (False, 23)
        self.datasets["ndscan.point_phase"] = (False, True)

        # Already write foo value of next point.
        self.datasets["ndscan.point.foo"] = (False, 0)
        self.sync()

        # Foo should still be the old value.
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})

    def test_preexisting(self):
        self.datasets["ndscan.point.foo"] = (False, 42)
        self.datasets["ndscan.point.bar"] = (False, 42)
        self.datasets["ndscan.point_phase"] = (False, True)
        self.datasets["ndscan.point.foo"] = (False, 0)
        self.init()

        with self.assertRaises(ValueError):
            # Can't know whether point is complete (it indeed isn't).
            self.root.get_model().get_point()

        self.datasets["ndscan.point.bar"] = (False, 1)
        self.datasets["ndscan.point_phase"] = (False, False)
        self.sync()

        self.assertEqual(self.root.get_model().get_point(), {"foo": 0, "bar": 1})

    def test_already_completed(self):
        self.datasets["ndscan.point.foo"] = (False, 42)
        self.datasets["ndscan.point.bar"] = (False, 23)
        self.datasets["ndscan.point_phase"] = (False, True)
        self.datasets["ndscan.completed"] = (False, True)
        self.init()
        self.assertEqual(self.root.get_model().get_point(), {"foo": 42, "bar": 23})

"""
Tests for extracting subscan models from single points (ndscan.plots.model.subscan),
run against actual experiment output to cover the channel naming contract between the
experiment and applet sides.
"""

import json
from typing import Any

from mock_environment import HasEnvironmentCase

from ndscan.experiment import *
from ndscan.plots.model import Context, SinglePointModel
from ndscan.plots.model.subscan import create_subscan_roots
from ndscan.utils import SCHEMA_REVISION_KEY

#
# Set up two fragments with disjoint result channel names, and subscans scanning them.
#


class LeafAFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("value", FloatParam, "Value", default=0.0)
        self.setattr_result("result", FloatChannel)

    def run_once(self):
        self.result.push(self.value.get() + 1)


class LeafBFragment(ExpFragment):
    def build_fragment(self):
        self.setattr_param("offset", FloatParam, "Offset", default=0.0)
        self.setattr_result("total", FloatChannel)

    def run_once(self):
        self.total.push(self.offset.get() + 10)


class SubscanA(SubscanExpFragment):
    def build_fragment(self):
        self.setattr_fragment("frag", LeafAFragment)
        super().build_fragment(self, self.frag, [(self.frag, "value")])
        self.configure([(self.frag.value, LinearGenerator(0.0, 2.0, 3, False))])


class SubscanB(SubscanExpFragment):
    def build_fragment(self):
        self.setattr_fragment("frag", LeafBFragment)
        super().build_fragment(self, self.frag, [(self.frag, "offset")])
        self.configure([(self.frag.offset, LinearGenerator(0.0, 4.0, 3, False))])


# # #


class TwoSubscansFragment(ExpFragment):
    """Combines two subscans with disjoint result channel names (but same scan `spec`
    channel name).
    """

    def build_fragment(self):
        self.setattr_fragment("scan_a", SubscanA)
        self.setattr_fragment("scan_b", SubscanB)

    def run_once(self):
        self.scan_a.run_once()
        self.scan_b.run_once()


TwoSubscansExp = make_fragment_scan_exp(TwoSubscansFragment)


class LegacySubscanFragment(ExpFragment):
    """Straightforward, single legacy subscan as a baseline.

    This also differs from the SubscanExpFragments in having a non-empty channel name
    prefix (`scan_spec` rather than `_spec`, etc.).
    """

    def build_fragment(self):
        self.setattr_fragment("child", LeafAFragment)
        setattr_subscan(self, "scan", self.child, [(self.child, "value")])

    def run_once(self):
        self.scan.run([(self.child.value, LinearGenerator(0.0, 2.0, 3, False))])


LegacySubscanExp = make_fragment_scan_exp(LegacySubscanFragment)


class DatasetSinglePointModel(SinglePointModel):
    """Exposes the datasets produced by a no-axes scan in the same way as the `hdf5` or
    `subscriber` single-point models do, but without jumping through the hoops of
    setting up that machinery.
    """

    def __init__(self, dataset_db, prefix: str, context: Context):
        super().__init__(dataset_db.get(prefix + SCHEMA_REVISION_KEY), context)
        self._channel_schemata = json.loads(dataset_db.get(prefix + "channels"))
        self._point = {
            name: dataset_db.get(prefix + "point." + name)
            for name in self._channel_schemata.keys()
        }

    def get_channel_schemata(self) -> dict[str, Any]:
        return self._channel_schemata

    def get_point(self) -> dict[str, Any] | None:
        return self._point


class SubscanModelCase(HasEnvironmentCase):
    def _run_and_get_model(self, klass):
        exp = self.create(klass)
        exp.prepare()
        exp.run()
        return DatasetSinglePointModel(self.dataset_db, "ndscan.rid_0.", Context())

    def test_resolve_inconsistently_shortened_channel_names(self):
        model = self._run_and_get_model(TwoSubscansExp)

        # Make sure this exercises the interesting case, where globally unique leaf
        # names cause the channels holding the subscan data to be shortened such that
        # they do not retain the name of their spec channel as a prefix.
        schemata = model.get_channel_schemata()
        self.assertIn("scan_a__spec", schemata)
        self.assertIn("_channel_result", schemata)
        self.assertIn("_channel_total", schemata)

        roots = create_subscan_roots(model)
        self.assertEqual(sorted(roots.keys()), ["scan_a_", "scan_b_"])

        data_a = roots["scan_a_"].get_model().get_point_data()
        self.assertEqual(list(data_a["axis_0"]), [0.0, 1.0, 2.0])
        self.assertEqual(list(data_a["channel_result"]), [1.0, 2.0, 3.0])

        data_b = roots["scan_b_"].get_model().get_point_data()
        self.assertEqual(list(data_b["axis_0"]), [0.0, 2.0, 4.0])
        self.assertEqual(list(data_b["channel_total"]), [10.0, 12.0, 14.0])

    def test_resolve_legacy_subscan_channel_names(self):
        model = self._run_and_get_model(LegacySubscanExp)

        roots = create_subscan_roots(model)
        self.assertEqual(list(roots.keys()), ["scan"])

        data = roots["scan"].get_model().get_point_data()
        self.assertEqual(list(data["axis_0"]), [0.0, 1.0, 2.0])
        self.assertEqual(list(data["channel_result"]), [1.0, 2.0, 3.0])

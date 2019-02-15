"""
Tests for ndscan.experiment top-level runners.
"""

import json
from ndscan.experiment import make_fragment_scan_exp
from fixtures import EchoFragment
from mock_environment import HasEnvironmentCase

ScanEchoExp = make_fragment_scan_exp(EchoFragment)


class FragmentScanExpCase(HasEnvironmentCase):
    def test_run_trivial_scan(self):
        exp = self.create(ScanEchoExp)
        exp._params["scan"]["continuous_without_axes"] = False
        exp.prepare()
        exp.run()

        def d(key):
            return self.dataset_db.get("ndscan." + key)

        self.assertEqual(json.loads(d("axes")), [])
        self.assertEqual(d("fragment_fqn"), "fixtures.EchoFragment")
        self.assertEqual(d("rid"), 0)

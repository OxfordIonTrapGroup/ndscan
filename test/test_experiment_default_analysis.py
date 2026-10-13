import logging
import sys
import unittest
from typing import Any

import numpy as np
from mock_environment import HasEnvironmentCase

from ndscan.experiment.default_analysis import (
    CustomAnalysis,
    OnlineFit,
    clean_channel_name,
)
from ndscan.experiment.entry_point import run_fragment_once
from ndscan.experiment.fragment import ExpFragment
from ndscan.experiment.parameters import (
    FloatParam,
    FloatParamHandle,
    FloatParamStore,
)
from ndscan.experiment.result_channels import (
    FloatChannel,
    ResultChannel,
)
from ndscan.experiment.scan_generator import LinearGenerator
from ndscan.experiment.scan_runner import ScanAxis, match_default_analysis
from ndscan.experiment.subscan import SubscanExpFragment
from ndscan.utils import FIT_OBJECTS

TOLERATED_FIT_ERROR = 3e-2
TOLERATED_REDUCED_CHI_SQUARED = 0.2

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)


class CustomAnalysisTestCase(unittest.TestCase):
    def test_axis_matching(self):
        param = FloatParam("", "", 0.0)
        foo = FloatParamHandle(None, "foo", param)
        foo.set_store(FloatParamStore(("Fragment.foo", "*"), 0.0))
        bar = FloatParamHandle(None, "bar", param)
        bar.set_store(FloatParamStore(("Fragment.bar", "*"), 1.0))

        def make_axes(*axes):
            return [ScanAxis(None, None, ax._store) for ax in axes]

        a = CustomAnalysis([foo], lambda *a: [])
        self.assertTrue(match_default_analysis(a, make_axes(foo)))
        self.assertFalse(match_default_analysis(a, make_axes(bar)))
        self.assertFalse(match_default_analysis(a, make_axes(foo, bar)))


class OnlineFitChannelNameTestCase(unittest.TestCase):
    def test_online_fit_sinusoid_channel_names(self):
        """Test channel names of sinusoid fit. Verify that the ...t_pi/2...
        parameter names have been corrected to ...t_pi_2..."""
        o = OnlineFit("sinusoid", data=None, save_fit_results=True)
        self.assertIn("sinusoid_fit_t_pi_2", o._result_channels.keys())
        self.assertNotIn("sinusoid_fit_t_pi/2", o._result_channels.keys())
        self.assertIn("sinusoid_fit_t_pi_2_err", o._result_channels.keys())
        self.assertNotIn("sinusoid_fit_t_pi/2_err", o._result_channels.keys())
        self.assertEqual(
            o._result_channels["sinusoid_fit_t_pi_2"].path, "sinusoid_t_pi_2"
        )
        self.assertEqual(
            o._result_channels["sinusoid_fit_t_pi_2_err"].path, "sinusoid_t_pi_2_err"
        )

    def test_all_online_fit_is_identifier(self):
        """Test that for every fit object, the resulting channel names are valid
        python identifiers, and that there are no duplicate channel names."""
        for fit_type in FIT_OBJECTS:
            o = OnlineFit(fit_type, data=None, save_fit_results=True)
            for k, v in o._result_channels.items():
                self.assertTrue(
                    k.isidentifier(),
                    f"{k} from {fit_type} fit is not a valid python identifier",
                )
                self.assertTrue(
                    v.path.isidentifier(),
                    f"{v.path} from {fit_type} fit is not a valid python identifier",
                )


class OnlineFitDataSource(ExpFragment):
    def build_fragment(
        self,
        fit_name: str,
        parameters: dict[str, float],
        constants: dict[str, Any] | None = None,
        initial_values: dict[str, Any] | None = None,
        gaussian_noise: float = 0.0,
        random_seed: int = 0,
        analysis_identifier: str | None = None,
    ):
        self.fit_name = fit_name

        self.parameters = parameters
        self.constants = {} if constants is None else constants
        self.initial_values = {} if initial_values is None else initial_values
        self.gaussian_noise = gaussian_noise
        self.analysis_identifier = analysis_identifier
        self.setattr_param("x", FloatParam, "x axis (function input)", default=0.0)
        self.setattr_result("y", FloatChannel)
        if self.gaussian_noise > 0.0:
            self.setattr_result("y_err", FloatChannel)
            self.rng = np.random.default_rng(seed=random_seed)

    def run_once(self) -> None:
        model = FIT_OBJECTS[self.fit_name].fitting_function(
            self.x.get(), self.parameters | self.constants | self.initial_values
        )
        if self.gaussian_noise > 0:
            model += self.rng.standard_normal() * self.gaussian_noise
            self.y_err.push(self.gaussian_noise)
        self.y.push(model)

    def get_default_analyses(self) -> list[OnlineFit]:
        data = {"x": self.x, "y": self.y}
        if self.gaussian_noise > 0.0:
            data["y_err"] = self.y_err
        return [
            OnlineFit(
                self.fit_name,
                data=data,
                constants=self.constants,
                initial_values=self.initial_values,
                analysis_identifier=self.analysis_identifier,
            )
        ]


class OnlineFitDataSourceSubscan(SubscanExpFragment):
    def build_fragment(self, *args, **kwargs) -> None:
        self.setattr_fragment("ofds", OnlineFitDataSource, *args, **kwargs)
        super().build_fragment(self, "ofds", [(self.ofds, "x")])

    def host_setup(self):
        self.configure(
            [(self.ofds.x, LinearGenerator(-10.0, 10.0, 100, randomise_order=True))]
        )
        super().host_setup()


class OnlineFitAnalysisTestCase(HasEnvironmentCase):
    CASES = {  # noqa: RUF012
        "gaussian": {
            "x0": 2.0,
            "y0": 1.0,
            "a": 3.0,
            "sigma": 1.5,
        },
        "lorentzian": {
            "x0": -1.0,
            "y0": 0.5,
            "a": 2.0,
            "fwhm": 1.0,
        },
        "exponential_decay": {
            "x0": -2.0,
            "y0": 10.0,
            "y_inf": 5.0,
            "tau": 3.0,
        },
    }

    def _test_execute(
        self,
        fit_name,
        true_params,
        constants,
        initial_values,
        gaussian_noise=0.0,
        analysis_identifier=None,
    ):
        self.exp = self.create(
            OnlineFitDataSourceSubscan,
            [],
            fit_name=fit_name,
            parameters=true_params,
            constants=constants,
            initial_values=initial_values,
            gaussian_noise=gaussian_noise,
            analysis_identifier=analysis_identifier,
        )

        def fit_channel(key) -> ResultChannel:
            full_attr = (
                f"_{fit_name}_fit_{key}"
                if (analysis_identifier is None)
                else f"_{analysis_identifier}_{key}"
            )
            return getattr(self.exp, full_attr)

        results = run_fragment_once(self.exp)
        for param_name, true_value in true_params.items():
            fit_value = results[fit_channel(param_name)]
            fit_err = results[fit_channel(param_name + "_err")]
            self.assertAlmostEqual(fit_value, true_value, delta=5 * fit_err)

        reduced_chi_squared = results[fit_channel("reduced_chi_squared")]
        if gaussian_noise > 0.0:
            # Vague check that chi-squared is calculated correctly (since we exactly add
            # i.i.d. Gaussian nodes of the known error, this should be 1.0 up to
            # sampling noise).
            self.assertGreaterEqual(reduced_chi_squared, 0.5)
            self.assertLessEqual(reduced_chi_squared, 1.5)
        else:
            # Residuals should be zero for perfect match, since we don't add noise.
            self.assertLess(np.abs(reduced_chi_squared), 1e-5)
        return results

    def test_exact(self):
        for fit_name, true_params in self.CASES.items():
            self._test_execute(
                fit_name,
                true_params,
                constants={},
                initial_values={},
                gaussian_noise=0.0,
            )

    def test_sinusoid_exact(self):
        # We need to override the initial values for sinusoid, or else
        # the auto-initialized values are outside of the param bounds.
        sinusoid_true_params = {
            "omega": 10.0,
            "t_dead": 0.01,
            "a": 0.8,
            "c": 0.05,
            "phi": -0.05,
        }
        init_values = {
            "omega": 1e-3,
            "t_dead": 1e-3,
            "a": 1e-3,
            "c": 1e-3,
            "phi": 1e-3,
        }
        results = self._test_execute(
            "sinusoid",
            sinusoid_true_params,
            constants={},
            initial_values=init_values,
            gaussian_noise=0.0,
        )

        self.assertIn(self.exp._sinusoid_fit_t_pi_2, results.keys())
        self.assertIn(self.exp._sinusoid_fit_t_pi_2_err, results.keys())

        self.assertIsNotNone(results[self.exp._sinusoid_fit_t_pi_2])
        self.assertIsNotNone(results[self.exp._sinusoid_fit_t_pi_2_err])

    def test_analysis_identifier_channel_prefix(self):
        """
        Test to ensure that the analysis_identifier value precedes the channel name
        as expected for an online fit.
        """
        sinusoid_true_params = {
            "omega": 10.0,
            "t_dead": 0.01,
            "a": 0.8,
            "c": 0.05,
            "phi": -0.05,
        }
        init_values = {
            "omega": 1e-3,
            "t_dead": 1e-3,
            "a": 1e-3,
            "c": 1e-3,
            "phi": 1e-3,
        }
        self.assertRaises(
            AssertionError,
            lambda: self._test_execute(
                "sinusoid",
                sinusoid_true_params,
                constants={},
                initial_values=init_values,
                gaussian_noise=0.0,
                analysis_identifier="z/",
            ),
        )

        custom_identifier = "CUSTOM_ANALYSIS_IDENTIFIER"

        results = self._test_execute(
            "sinusoid",
            sinusoid_true_params,
            constants={},
            initial_values=init_values,
            gaussian_noise=0.0,
            analysis_identifier=custom_identifier,
        )

        original_fit_param_names = [
            clean_channel_name(p_name)
            for p_name in FIT_OBJECTS["sinusoid"].all_parameter_names
        ]
        for p in original_fit_param_names:
            attr = getattr(self.exp, f"_{custom_identifier}_{p}")
            self.assertIn(attr, results.keys())
            self.assertIsNotNone(results[attr])

    def test_noisy(self):
        for fit_name, true_params in self.CASES.items():
            self._test_execute(
                fit_name,
                true_params,
                constants={},
                initial_values={},
                gaussian_noise=0.1,
            )

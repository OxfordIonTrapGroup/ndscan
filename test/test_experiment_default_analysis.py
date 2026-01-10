import logging
import sys
import unittest
from typing import Any

import numpy as np
from mock_environment import HasEnvironmentCase

from ndscan.experiment.default_analysis import CustomAnalysis, OnlineFit
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


class OnlineFitDataSource(ExpFragment):
    def build_fragment(
        self,
        fit_name: str,
        parameters: dict[str, float],
        constants: dict[str, Any] | None = None,
        initial_values: dict[str, Any] | None = None,
        gaussian_noise: float = 0.0,
    ):
        self.fit_name = fit_name

        self.parameters = parameters
        self.constants = {} if constants is None else constants
        self.initial_values = {} if initial_values is None else initial_values
        self.gaussian_noise = gaussian_noise
        self.setattr_param("x", FloatParam, "x axis (function input)", default=0.0)
        self.setattr_result("y", FloatChannel)
        if self.gaussian_noise > 0.0:
            self.setattr_result("y_err", FloatChannel)

    def run_once(self) -> None:
        model = FIT_OBJECTS[self.fit_name].fitting_function(
            self.x.get(), self.parameters | self.constants | self.initial_values
        )
        if self.gaussian_noise > 0:
            model += np.random.randn() * self.gaussian_noise
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
    CASES = {
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
        self, fit_name, true_params, constants, initial_values, gaussian_noise=0.0
    ):
        self.exp = self.create(
            OnlineFitDataSourceSubscan,
            [],
            fit_name=fit_name,
            parameters=true_params,
            constants=constants,
            initial_values=initial_values,
            gaussian_noise=gaussian_noise,
        )

        def fit_channel(key) -> ResultChannel:
            return getattr(self.exp, f"_{fit_name}_fit_{key}")

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

    def test_exact(self):
        for fit_name, true_params in self.CASES.items():
            self._test_execute(
                fit_name,
                true_params,
                constants={},
                initial_values={},
                gaussian_noise=0.0,
            )

    def test_noisy(self):
        for fit_name, true_params in self.CASES.items():
            self._test_execute(
                fit_name,
                true_params,
                constants={},
                initial_values={},
                gaussian_noise=0.5,
            )

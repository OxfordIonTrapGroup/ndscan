import logging
import sys
import unittest
from typing import Type

import numpy as np
from mock_environment import HasEnvironmentCase
from oitg.fitting.FitBase import FitBase

from ndscan.experiment.annotations import AnnotationContext
from ndscan.experiment.default_analysis import CustomAnalysis, OnlineFit
from ndscan.experiment.parameters import (
    FloatParam,
    FloatParamHandle,
    FloatParamStore,
    IntParam,
    ParamBase,
    ParamHandle,
)
from ndscan.experiment.result_channels import (
    ArraySink,
    FloatChannel,
    IntChannel,
    OpaqueChannel,
    ResultChannel,
    ScalarDatasetSink,
)
from ndscan.experiment.scan_runner import ScanAxis, match_default_analysis
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


class OnlineFitAnalysisTestCase(HasEnvironmentCase):
    def _test_get_analysis_results(self):
        fit_types = FIT_OBJECTS.keys()
        for fit in fit_types:
            for save_fit_results in [True, False]:
                for coordinates_dict in self._create_coordinates_dicts():
                    online_fit = self._create_online_fit(
                        fit,
                        coordinates_dict,
                        save_fit_results,
                    )
                    self._check_analysis_results(fit, online_fit, save_fit_results)

    def test_execute(self):
        fit_types = ["gaussian", "lorentzian", "exponential_decay"]
        test_id = 0
        for fit in fit_types:
            # for random_y in [True, False]:
            for i, coordinates_dict in enumerate(self._create_coordinates_dicts()):
                random_y = False
                self._check_execute_instance(
                    fit,
                    coordinates_dict,
                    True,
                    random_y,
                    test_id,
                )
                test_id += 1

    def _check_execute_instance(
        self,
        fit_type: str,
        coordinates_dict: dict[str, ParamHandle | ResultChannel],
        save_fit_results: bool,
        random_y=False,
        uid=None,
    ):
        include_y_err = "y_err" in coordinates_dict
        xy_data, actual_params = self._generate_data(
            fit_type,
            include_y_err,
            random_y,
        )
        actual_params = None if random_y else actual_params

        online_fit = self._create_online_fit(fit_type, coordinates_dict, True, uid)

        def key_to_data(key):
            if key == "x":
                return xy_data[0]
            elif key == "y":
                return xy_data[1]
            elif key == "y_err":
                return xy_data[2]

        axis_data = {
            data_obj._store.identity: key_to_data(key)
            for key, data_obj in coordinates_dict.items()
            if isinstance(data_obj, ParamHandle)
        }
        result_data = {
            data_obj: key_to_data(key)
            for key, data_obj in coordinates_dict.items()
            if isinstance(data_obj, ResultChannel)
        }

        context = AnnotationContext(lambda _: 0, lambda _: "", lambda _: True)

        online_fit.execute(axis_data, result_data, context)
        results = online_fit.get_analysis_results()

        def relative_error(expected, actual):
            return abs(expected - actual) / abs(expected)

        # If actual_params is None, we expect the fit to have failed

        for i, (param_name, result) in enumerate(results.items()):
            value = result.sink.get_last()
            if actual_params is None:
                # reduced_chi_squared_key = f"{online_fit._channel_prefix}reduced_chi_squared"
                # value = results[reduced_chi_squared_key].sink.get_last()
                # logger.error(f"Reduced chi squared value: {value}")
                self.assertTrue(value is None, f"{result.sink.get_last()}")
                # self.assertFalse(
                #     0.8 <= value <= 1.2,
                #     f"chi squared {value} indicates a successful fit. ",
                # )
                continue

            logger.warning(
                f"param_name={param_name}, value={value}, actual_params={actual_params[param_name]}"
            )
            self.assertTrue(
                relative_error(actual_params[param_name], value) <= TOLERATED_FIT_ERROR,
                f"Parameter '{param_name}' fit value {value} deviates from actual "
                f"value {actual_params[param_name]} beyond tolerated error. {result.sink.get_last()}",
            )

    def _check_analysis_results(
        self, fit_type, online_fit: OnlineFit, save_fit_results: bool
    ):
        results = online_fit.get_analysis_results()

        if not save_fit_results:
            self.assertEqual(len(results.keys()), 0)
            return

        try:
            params = FIT_OBJECTS[fit_type].all_parameter_names
        except AttributeError:
            # dev environment has an older version of oitg.fitting
            params = FIT_OBJECTS[fit_type].parameter_names

        for param in params:
            self.assertIn(param, results)
            self.assertIn(f"{param}_err", results)
            self.assertEqual(results[param].path, f"fit_{param}")
        self.assertIn("reduced_chi_squared", results)

    def _generate_data(self, fit_type, include_y_err=True, random_y=False):
        fit_obj: FitBase = FIT_OBJECTS[fit_type]
        fit_fn = fit_obj.fitting_function
        fit_params = fit_obj.parameter_names

        x = np.linspace(-10, 10, 100)
        rng = np.random.default_rng(12345)

        if random_y:
            y = rng.uniform(0, 100, size=x.shape)
            if include_y_err:
                y_err = rng.normal(0.1, 0.5, size=x.shape)
                return (x, y, y_err), None
            return (x, y), None

        if fit_type == "gaussian":
            params = {
                "x0": 2.0,
                "y0": 1.0,
                "a": 3.0,
                "sigma": 1.5,
            }
        elif fit_type == "lorentzian":
            params = {
                "x0": -1.0,
                "y0": 0.5,
                "a": 2.0,
                "fwhm": 1.0,
            }
        elif fit_type == "exponential_decay":
            params = {
                "x0": -2.0,
                "y0": 10.0,
                "y_inf": 5.0,
                "tau": 3.0,
            }
        else:
            raise NotImplementedError

        y_clean = fit_fn(x, params)
        y_err = 0.03 * np.abs(y_clean) if include_y_err else None
        noise = rng.normal(0, 0.1 * np.abs(y_clean), size=y_clean.shape)
        y = y_clean + noise

        p, p_err, residuals = fit_obj.fit(
            x, y_clean + noise, y_err=y_err, calculate_residuals=True
        )

        weights = 1 / (y_err**2) if include_y_err else np.ones_like(y_clean)
        reduced_chi_squared = np.sum((residuals**2 * weights) ** 2) / (
            len(x) - len(fit_params)
        )

        prefix = f"{fit_type}_fit_"

        results_dict = {f"{prefix}{param}": value for param, value in p.items()}
        results_dict.update(
            {f"{prefix}{param}_err": value for param, value in p_err.items()},
        )
        results_dict[f"{prefix}reduced_chi_squared"] = reduced_chi_squared

        if include_y_err:
            return (x, y, y_err), results_dict
        else:
            return (x, y), results_dict

    def _create_online_fit(
        self, fit_type, coordinates_dict, save_fit_results, uid=None
    ):
        # parent = self.create(AddOneFragment)
        sink = self.create(ScalarDatasetSink, f"{fit_type}_result_{uid}")
        try:
            online_fit = OnlineFit(
                data=coordinates_dict,
                fit_type=fit_type,
                save_fit_results=save_fit_results,
            )
        except Exception as e:
            self.fail(f"Failed to create OnlineFit: {e}")
            return

        results = online_fit.get_analysis_results()
        for result in results.values():
            result.set_sink(sink)

        return online_fit

    def _create_coordinates_dicts(self):
        def create_param_handle(name, param_type: Type[ParamBase]):
            param = param_type(name, name, 0.0)
            handle = param.HandleType(None, "param", param)
            handle.set_store(param.StoreType(("Fragment.param", "*"), 0.0))
            return handle

        def create_results_channel(
            name,
            result_type: Type[ResultChannel],
        ):
            channel = result_type(name, name)
            channel.set_sink(ArraySink())
            return channel

        return [
            {
                "x": create_param_handle("x_axis", FloatParam),
                "y": create_results_channel("y_axis", OpaqueChannel),
            },
            {
                "x": create_param_handle("x_axis", IntParam),
                "y": create_param_handle("y_axis", FloatParam),
            },
            {
                "x": create_results_channel("x_axis", FloatChannel),
                "y": create_results_channel("y_axis", IntChannel),
                "y_err": create_results_channel("y_err", OpaqueChannel),
            },
            {
                "x": create_param_handle("x_axis", IntParam),
                "y": create_results_channel("y_axis", FloatChannel),
                "y_err": create_param_handle("y_err", FloatParam),
            },
        ]

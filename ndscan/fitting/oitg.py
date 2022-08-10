import numpy as np
from typing import Dict, List, Tuple
from .fitting import FitBase
import oitg.fitting


class OITGFit(FitBase):
    _oitg_obj: oitg.fitting.FitBase
    """ Compatibility shim for the OITG fitting functions """
    def fit(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        """
        Fit the dataset and return the fitted parameter values and uncertainties.
        """
        p, p_err = self._oitg_obj.fit(x=self._x,
                                      y=self._y,
                                      y_err=self._y_err,
                                      constants=self._fixed_params,
                                      initialise=self._initial_values,
                                      calculate_residuals=False,
                                      evaluate_function=False,
                                      evaluate_x_limit=[None, None],
                                      evaluate_n=1000)
        self._p = p
        self._p_err = p_err
        return p, p_err

    @classmethod
    def func(cls, x, params):
        """The fit function."""
        return cls._oitg_obj.fitting_function(x, params)

    @classmethod
    def get_params(cls) -> List[str]:
        """Returns list of fit params"""
        return cls._oitg_obj.parameter_names

    @classmethod
    def get_derived_params(cls) -> List[str]:
        """Returns list of derived parameters"""
        # 🙈🙈🙈🙈🙈🙈 a "not for poets" (TM) implementation
        # the oitg code doesn't provide a list of derived parameter values so we feed it
        # a series of meaningless parameter values chosen to avoid divide by zero errors
        # and then see what it gives us back.
        if cls._oitg_obj.derived_parameter_function is None:
            return []

        params = cls.get_params()
        numbers = np.arange(1, len(params))
        p_dict = {key: number for key, number in zip(params, numbers)}
        p_error_dict = dict(p_dict)
        cls._oitg_obj.derived_parameter_function(p_dict, p_error_dict)
        return list(set(p_dict.keys()) - set(params))

    @classmethod
    def get_default_bounds(cls) -> Dict[str, Tuple[float, float]]:
        """
        Returns a dictionary mapping parameter names to a tuple of (upper, lower) bounds
        """
        bounds = cls._oitg_obj.parameter_bounds
        return {
            param: bounds.get(param, (-np.inf, np.inf))
            for param in cls.get_params()
        }

    def _calculate_derived_params(self):
        """
        Updates fit results with values and uncertainties for derived parameters.
        """
        if self._oitg_obj.derived_parameter_function is None:
            return

        self._oitg_obj.derived_parameter_function(self._p, self._p_err)

    def _estimate_parameters(self) -> Dict[str, float]:
        """
        Returns a dictionary of estimates for the parameter values for the current
        dataset.
        """
        initial_values = {}
        self._oitg_obj.parameter_initialiser(self._x, self._y, initial_values)
        return initial_values


class sinusoid(OITGFit):
    _oitg_obj = oitg.fitting.sinusoid

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"pi_time": {"x": "t_pi"}}


class cos(OITGFit):
    _oitg_obj = oitg.fitting.cos


class decaying_sinusoid(OITGFit):
    _oitg_obj = oitg.fitting.decaying_sinusoid

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"pi_time": {"x": "t_max_transfer"}}


class detuned_square_pulse(OITGFit):
    _oitg_obj = oitg.fitting.detuned_square_pulse

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"centre": {"x": "offset"}}


class exponential_decay(OITGFit):
    _oitg_obj = oitg.fitting.exponential_decay

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"t_1_e": {"x": "t_1_e"}}


class gaussian(OITGFit):
    _oitg_obj = oitg.fitting.gaussian

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"centre": {"x": "x0"}}


class line(OITGFit):
    _oitg_obj = oitg.fitting.line


class lorentzian(OITGFit):
    _oitg_obj = oitg.fitting.lorentzian

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"extremum": {"x": "x0"}}


class rabi_flop(OITGFit):
    _oitg_obj = oitg.fitting.rabi_flop

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"pi_time": {"x": "t_pi"}}


class v_function(OITGFit):
    _oitg_obj = oitg.fitting.v_function

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"centre": {"x": "x0"}}


class parabola(OITGFit):
    _oitg_obj = oitg.fitting.shifted_parabola

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {"extremum": {"x": "position"}}

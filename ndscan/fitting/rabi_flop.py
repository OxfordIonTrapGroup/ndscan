import numpy as np
from typing import Dict, List, Tuple
from . import FitBase, Sinusoid


class RabiFlop(FitBase):
    """ Time-/frequency-domain exponentially damped Rabi flop fit according to:
        y = P0 * y0 + P1 * y1
        y1 = Gamma * (contrast * (omega * t / 2 * sinc(W*t/2))^2 + P_lower - c) + c
        Gamma = exp(-t/tau)
        w = sqrt(omega^2 + delta^2)
        contrast = P_upper - P_lower
        c = 0.5 * (P_upper + P_lower)
        P0 = 1 - P1
        y0 = 1 - y1

    For frequency scans we use: t = t_pulse - t_dead, delta = x - detuning_offset
    For time scans we use: {t = x - t_dead for x > t_dead, x = 0 for t <= t_dead},
                           delta = detuning_offset

    This class is not intended to be used directly. See RabiFlopTime and
    RabiFlopFrequency.

    Fit parameters (all floated by default unless stated otherwise):
        - P1: initial upper-state population (fixed to 1 by default)
        - P_upper: upper readout level (fixed to 1 by default)
        - P_lower: lower readout level (fixed to 0 by default)
        - omega: Rabi frequency
        - t_pulse: pulse duration (frequency scans only)
        - t_dead: dead_time (fixed to 0 by default)
        - detuning_offset: detuning offset
        - tau: decay time constant (fixed to np.inf by default)

    Derived parameters:
        - t_pi: pi-time including dead-time (so t_2pi != 2*t_pi), is not the time for
          maximum population transfer for finite tau (TODO: add that as a derived
          parameter!)
        - t_pi_2: pi/2-time including dead-time (so t_pi != 2*t_pi_2)
        - TODO: do we want pulse area error, etc?

    All phases are in radians, detunings are in angular units.
    """
    @staticmethod
    def func(x, params):
        raise NotImplementedError

    @staticmethod
    def rabi_func(delta, t, params):
        P1 = params["P1"]
        P_upper = params["P_upper"]
        P_lower = params["P_lower"]
        omega = params["omega"]
        tau = params["tau"]

        P0 = 1 - P1
        Gamma = np.exp(-t / tau)
        W = np.sqrt(np.pow(omega, 2), np.pow(delta, 2))
        contrast = P_upper - P_lower
        c = 0.5 * (P_upper + P_lower)

        # NB np.sinc(x) = sin(pi*x)/(pi*x)
        y1 = Gamma * (contrast * np.pow(omega * t / 2 * np.sinc(W * t /
                                                                (2 * np.pi)), 2) +
                      P_lower - c) + c
        y1[t < 0] = P1 * P_upper
        y0 = 1 - y1

        return P0 * y0 + P1 * y1

    @staticmethod
    def get_params() -> List[str]:
        """Returns list of fit params"""
        return ["P1", "P_upper", "P_lower", "omega", "t_dead", "tau", "detuning_offset"]

    @classmethod
    def get_default_bounds(cls) -> Dict[str, Tuple[float, float]]:
        """
        Returns a dictionary mapping parameter names to a tuple of (upper, lower) bounds
        """
        bounds = {
            "P1": (0, 1),
            "P_upper": (0, 1),
            "P_lower": (0, 1),
            "omega": (0, np.inf),
            "t_pulse": (0, np.inf),
            "t_dead": (0, np.inf),
            "detuning_offset": (-np.inf, np.inf),
            "tau": (0, np.inf)
        }
        return {param: bounds[param] for param in cls.get_params()}

    @classmethod
    def get_default_fixed_params(cls) -> Dict[str, float]:
        """Returns a dictionary mapping names of parameters which are not floated by
        default to their values.
        """
        fixed = {
            "P1": 0,
            "P_upper": 1,
            "P_lower": 0,
            "t_dead": 0,
            "tau": np.inf,
        }
        return {param: fixed[param] for param in cls.get_params()}

    def get_default_scale_factors(self) -> Dict[str, float]:
        """ Returns a dictionary of default parameter scale factors. """
        scales = {
            "P1": 1,
            "P_upper": 1,
            "P_lower": 1,
            "omega": 1 / self._x_scale,
            "t_pulse": self._x_scale,
            "t_dead": self._x_scale,
            "detuning_offset": 1 / self._x_scale,
            "tau": self._x_scale
        }
        return {param: scales[param] for param in self.get_params()}

    @staticmethod
    def get_derived_params() -> List[str]:
        """Returns a list of derived parameters.

        NB we define t_pi (t_pi_2) as the time taken for a pi-pulse (pi/2-pulse)
        including deadtime. As a result, the 2*pi time is not 2*t_pi.
        """
        return ["t_pi", "t_pi_2"]

    def _estimate_parameters(self):
        param_guesses = self._sinusoid_estimator(
            x=self._x,
            y=self._y,
            y_err=self._y_err,
            fixed_params=Sinusoid.get_default_fixed_params(),
            param_bounds=Sinusoid.get_default_bounds(),
        )

        param_guesses['P1'] = 1  # TODO: estimate from sine phase?
        param_guesses['P_upper'] = param_guesses['offset'] + np.abs(param_guesses['a'])
        param_guesses['P_lower'] = param_guesses['offset']
        return param_guesses


class RabiFlopTime(RabiFlop):
    """ Time-domain Rabi flops. See RabiFlop for details. """
    @staticmethod
    def func(x, params):
        t = x - params["t_dead"]
        delta = params["detuning_offset"]
        return RabiFlop.rabi_func(delta=delta, t=t, params=params)

    def _estimate_parameters(self):
        param_guesses = super()._estimate_parameters()
        param_guesses['detuning_offset'] = 0
        return {param: param_guesses[param] for param in self.get_params()}


class RabiFlopFrequency(RabiFlop):
    """ Frequency-domain Rabi flops. See RabiFlop for details. """
    @staticmethod
    def func(x, params):
        t = params["t_pulse"] - params["t_dead"]
        delta = x - params["detuning_offset"]
        return RabiFlop.rabi_func(delta=delta, t=t, params=params)

    @staticmethod
    def get_params() -> List[str]:
        """Returns list of fit params"""
        return super().get_params() + ["t_pulse"]

    def _estimate_parameters(self):
        param_guesses = super()._estimate_parameters()

        # sqrt(3) factor derived from assuming pi pulse
        param_guesses['t_pulse'] = param_guesses['omega'] / np.sqrt(3)
        param_guesses['omega'] = np.pi / param_guesses["t_pulse"]
        param_guesses['detuning_offset'] = param_guesses['x_peak']

        return {param: param_guesses[param] for param in self.get_params()}

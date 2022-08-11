import numpy as np
from scipy import signal, optimize
from typing import Dict, List, Tuple
from ndscan import fitting

__all__ = ['Sinusoid']


class Sinusoid(fitting.FitBase):
    """Generalised sinusoid fit according to:
    x > x_dead: y = Gamma*a*sin(omega*(x - x0 - x_dead) + phi0) + offset
    x <= x_dead: y = a*sin(-x0*omega + phi0) + offset
    where Gamma = exp(-1/tau * (x - x_dead))

    Fit parameters (all floated by default unless stated otherwise):
      - a: initial (x = x_dead) amplitude of the decaying sinusoid
      - omega: angular frequency
      - phi0: phase offset
      - offset: y-axis offset
      - x_dead: "dead time" (fixed to 0 by default)
      - x0: x-axis time offset (fixed to 0 by default)
      - tau: decay time constant (fixed to np.inf by default)

    Derived parameters:
      - f: frequency
      - phi_cosine: cosine phase (phi0 + pi/2)
      - min/max: min / max values of the undamped sinusoid (including the offset and
          decay).
      - period: period of oscillation

    All phases are in radians, frequencies are in angular units.

    x0 and phi0 are equivalent parametrisations for the phase offset, but in some cases
    it works out convenient to have access to both (e.g. one as a fixed offset, the
    other floated). At most one of them may be floated at once. By default, x0 is fixed
    at 0 and phi0 is floated.

    TODO: calculate peak values of the damped sinusoid as well as time that the peak
    value occurs at.
    """
    @staticmethod
    def func(x, params):
        a = params["a"]
        w = params["omega"]
        phi0 = params["phi0"]
        offset = params["offset"]
        x_dead = params["x_dead"]
        x0 = params["x0"]
        tau = params["tau"]

        Gamma = np.exp(-1 / tau * (x - x_dead))
        y = Gamma * a * np.sin(w * (x - x0 - x_dead) + phi0) + offset
        y0 = a * np.sin(-x0 * w + phi0) + offset
        y[x <= x_dead] = y0
        return y

    @staticmethod
    def get_params() -> List[str]:
        """Returns list of fit params"""
        return ["a", "omega", "phi0", "offset", "x_dead", "x0", "tau"]

    @staticmethod
    def get_default_bounds() -> Dict[str, Tuple[float, float]]:
        """
        Returns a dictionary mapping parameter names to a tuple of (upper, lower) bounds
        """
        return {
            "a": (0, 1),
            "omega": (0, np.inf),
            "phi0": (0, 2 * np.pi),
            "offset": (0, 1),
            "x_dead": (0, np.inf),
            "x0": (-np.inf, np.inf),
            "tau": (0, np.inf)
        }

    @staticmethod
    def get_default_fixed_params() -> Dict[str, float]:
        """Returns a dictionary mapping names of parameters which are not floated by
        default to their values.
        """
        return {'x_dead': 0, 'x0': 0, 'tau': np.inf}

    def get_default_scale_factors(self) -> Dict[str, float]:
        """ Returns a dictionary of default parameter scale factors. """
        scales = {}
        scales['a'] = self._y_scale
        scales['omega'] = 1 / self._x_scale
        scales['phi0'] = 1
        scales['offset'] = self._y_scale
        scales['x_dead'] = self._x_scale
        scales['x0'] = self._x_scale
        scales['tau'] = self._x_scale
        return scales

    @staticmethod
    def get_derived_params() -> List[str]:
        """Returns a list of derived parameters. """
        return ["f", "phi_cosine", "min", "max", "period"]

    def _estimate_parameters(self):
        param_guesses = {}

        sorted_inds = np.argsort(self._x)
        x = self._x[sorted_inds]
        y = self._y[sorted_inds]
        y_err = None if self._y_err is None else self._y_err[sorted_inds]

        param_guesses["offset"] = np.mean(y)
        param_guesses["x_dead"] = 0
        param_guesses["x0"] = 0
        param_guesses["tau"] = max(x)  # TODO: more clever estimate here

        # Step 1: use a Lomb–Scargle Periodogram to estimate the frequency and amplitude
        # of the signal
        min_step = np.diff(x).min()
        length = x.ptp()

        # Nyquist limit does not apply to irregularly spaced data
        # We'll use it as a starting point anyway...
        f_max = 0.5 / min_step
        # relaxed Fourier limit
        f_min = 0.25 / length

        omega_list = 2 * np.pi * np.linspace(f_min, f_max, int(f_max / f_min))
        pgram = signal.lombscargle(x, y, omega_list, precenter=True)
        peak = np.argmax(np.abs(pgram))

        param_guesses["a"] = np.sqrt(pgram[peak] * 4 / len(y))
        param_guesses["omega"] = omega_list[peak]

        # Step 2: crude initial guess of the phase
        def fit_fun(x, phi0):
            params = dict(param_guesses)
            params["phi0"] = phi0
            return self.func(x, params)

        def cost_fun(x, phi0):
            return np.sum(np.power(y - fit_fun(x, phi0), 2))

        phis = np.linspace(0, 2 * np.pi, 16)
        costs = np.zeros_like(phis)
        for idx, phi0 in np.ndenumerate(phis):
            costs[idx] = cost_fun(x, phi0)
        param_guesses["phi0"] = phis[np.argmin(costs)]

        # Step 3: single-parameter fit to find the phase (more robust than floating
        # multiple parameters at once while the phase is off)
        p, _ = optimize.curve_fit(f=fit_fun,
                                  xdata=x,
                                  ydata=y,
                                  p0=param_guesses["phi0"],
                                  sigma=y_err,
                                  absolute_sigma=True,
                                  bounds=(0, 2 * np.pi))
        param_guesses["phi0"] = float(p)

        # x0 and phi0 are equivalent parameters, but in some cases it's useful to have
        # access to both. If exactly one of them is floated (the main anticipated
        # use-case) we can easily pick a sensible initial value for the other.
        # If both are floated there is no well-defined solution so we bug out
        period = 2 * np.pi / param_guesses["omega"]
        w = param_guesses["omega"]
        fixed = self._fixed_params

        if "x0" not in fixed and "phi0" in fixed:
            d_phi = param_guesses['phi0'] - self._fixed_params['phi0']
            param_guesses["x0"] = -d_phi / w

        elif "phi0" not in fixed and "x0" in fixed:
            d_x0 = -self._fixed_params['x0']
            param_guesses["phi0"] = -d_x0 * w

        elif "phi0" not in fixed and "x0" not in fixed:
            raise ValueError('At most one of "phi0" and "x0" can be floated at a time!')

        # handle phase wrapping
        if "phi0" not in fixed:
            phi_bounds = self._param_bounds["phi0"]
            if param_guesses["phi0"] < phi_bounds[0]:
                diff = phi_bounds[0] - param_guesses["phi0"]
                param_guesses["phi0"] += ((diff // (2 * np.pi)) + 1) * 2 * np.pi
            if param_guesses["phi0"] > phi_bounds[1]:
                diff = param_guesses["phi0"] - phi_bounds[1]
                param_guesses["phi0"] -= ((diff // (2 * np.pi)) + 1) * 2 * np.pi
        else:
            param_guesses['phi0'] = self._fixed_params['phi0']

        if "x0" not in fixed:
            x0_bounds = self._param_bounds["x0"]
            if param_guesses["x0"] < x0_bounds[0]:
                diff = x0_bounds[0] - param_guesses["x0"]
                param_guesses["x0"] += ((diff // period) + 1) * period
            if param_guesses["x0"] > x0_bounds[1]:
                diff = param_guesses["x0"] - x0_bounds[1]
                param_guesses["x0"] -= ((diff // period) + 1) * period
        else:
            param_guesses['x0'] = self._fixed_params['x0']

        return param_guesses

    def _calculate_derived_params(self):
        self._p["f"] = self._p["omega"] / (2 * np.pi)
        self._p["phi_cosine"] = self._p["phi0"] + np.pi / 2
        self._p["min"] = self._p["offset"] - np.abs(self._p["a"])
        self._p["max"] = self._p["offset"] + np.abs(self._p["a"])
        self._p["period"] = 2 * np.pi / self._p["omega"]

        # TODO: consider covariances
        self._p_err["f"] = self._p_err["omega"] / (2 * np.pi)

        self._p_err["phi_cosine"] = self._p_err["phi0"]
        self._p_err["min"] = np.sqrt(self._p_err["offset"]**2 + self._p_err["a"]**2)
        self._p_err["max"] = np.sqrt(self._p_err["offset"]**2 + self._p_err["a"]**2)

        self._p_err["period"] = \
            2 * np.pi / self._p["omega"] * (self._p_err["omega"] / self._p["omega"])


# TODO: move this into a unit test!
if __name__ == "__main__":
    from matplotlib import pyplot as plt

    x = np.linspace(0, 5, 501)
    params = {
        "a": np.random.uniform(low=0.25, high=1, size=1),
        "f": np.random.uniform(low=0.5, high=3, size=1),
        "phi0": np.random.uniform(low=0, high=2 * np.pi, size=1),
        "offset": np.random.uniform(low=0.25, high=0.75, size=1),
    }
    y = Sinusoid.func(x, params) + np.random.normal(0, 0.05, x.shape)
    fit = Sinusoid(x, y)

    if not np.isclose(fit.a[0], params["a"], rtol=5e-2):
        print(f"Amplitude error is {100*(1-fit.a[0]/params['a'])}%")
    if not np.isclose(fit.f[0], params["f"], rtol=5e-2):
        print(f"Frequency error is {100*(1-fit.f[0]/params['f'])}%")
    if not np.isclose(fit.phi0[0], params["phi0"], rtol=5e-2):
        print(f"Phase error is {100*(1-fit.phi0[0]/params['phi0'])}%")
    if not np.isclose(fit.offset[0], params["offset"], rtol=5e-2):
        print(f"Offset error is {100*(1-fit.offset[0]/params['offset'])}%")

    plt.plot(x, y, label="data")
    x_fit, y_fit = fit.evaluate()
    plt.plot(x_fit, y_fit, label="fit")
    plt.grid()
    plt.show()

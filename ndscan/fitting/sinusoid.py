import numpy as np
from scipy import signal, optimize
from typing import Dict, List, Tuple
from ndscan import fitting

__all__ = ['Sinusoid']


class Sinusoid(fitting.FitBase):
    """Sinusoid fit according to:
    y = a*sin(2*np.pi*f*(x-x0) + phi) + offset

    By default, x0 is fixed at 0 and phi is floated. If phi is fixed and x0 floated, we
    pick a default initial value for x0 that puts it in the first oscillation of the
    sinusoid (equivalent to putting phi in the range [0, 2*pi]).

    TODO: t_dead, exp decay (but default to fixed at 0)
    """
    @staticmethod
    def func(x, params):
        w = 2 * np.pi * params["f"]
        a = params["a"]
        phi = params["phi"]
        x0 = params["x0"]
        offset = params["offset"]
        return a * np.sin(w * (x - x0) + phi) + offset

    @staticmethod
    def get_params() -> List[str]:
        """Returns list of fit params"""
        return ["a", "f", "phi", "offset", "t_dead", "x0"]

    @staticmethod
    def get_default_bounds() -> Dict[str, Tuple[float, float]]:
        """
        Returns a dictionary mapping parameter names to a tuple of (upper, lower) bounds
        """
        return {
            "a": (0, 1),
            "f": (0, np.inf),
            "phi": (0, 2 * np.pi),
            "offset": (0, 1),
            "t_dead": (0, np.inf),
            "x0": (-np.inf, np.inf)
        }

    @staticmethod
    def get_default_fixed_params() -> Dict[str, float]:
        """Returns a dictionary mapping names of parameters which are not floated by
        default to their values.
        """
        return {'t_dead': 0, 'x0': 0}

    def get_default_scale_factors(self) -> Dict[str, float]:
        """ Returns a dictionary of default parameter scale factors. """
        scales = {}
        scales['a'] = self._y_scale
        scales['f'] = 1 / self._x_scale
        scales['phi'] = 1
        scales['offset'] = self._y_scale
        scales['t_dead'] = self._x_scale
        scales['x0'] = self._x_scale
        return scales

    @staticmethod
    def get_derived_params() -> List[str]:
        """Returns a list of derived parameters.

        NB we define t_pi (t_pi_2) as the time taken for a pi-pulse (pi/2-pulse)
        including deadtime. As a result, the 2*pi time is not 2*t_pi.
        """
        return ["omega", 't_pi', 't_pi_2', 'phi_cosine', "min", "max", "period"]

    def _estimate_parameters(self):
        param_guesses = {}

        sorted_inds = np.argsort(self._x)
        x = self._x[sorted_inds]
        y = self._y[sorted_inds]
        y_err = None if self._y_err is None else self._y_err[sorted_inds]

        param_guesses["offset"] = np.mean(y)
        param_guesses["t_dead"] = 0
        param_guesses["x0"] = 0

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
        param_guesses["f"] = omega_list[peak] / (2 * np.pi)

        # Step 2: crude initial guess of the phase
        def fit_fun(x, phi):
            params = dict(param_guesses)
            params["phi"] = phi
            return self.func(x, params)

        def cost_fun(x, phi):
            return np.sum(np.power(y - fit_fun(x, phi), 2))

        phis = np.arange(1, 8) / (2 * np.pi)
        costs = np.zeros_like(phis)
        for idx, phi in np.ndenumerate(phis):
            costs[idx] = cost_fun(x, phi)
        param_guesses["phi"] = phis[np.argmin(costs)]

        # Step 3: single-parameter fit to find the phase (more robust than floating
        # multiple parameters at once while the phase is off)
        p, _ = optimize.curve_fit(f=fit_fun,
                                  xdata=x,
                                  ydata=y,
                                  p0=param_guesses["phi"],
                                  sigma=y_err,
                                  absolute_sigma=True,
                                  bounds=self.get_default_bounds()["phi"])
        param_guesses["phi"] = float(p)

        # allow the user to float x0 rather than phi
        fixed = self._fixed_params
        if "x0" not in fixed and "phi" in fixed:
            w = 2 * np.pi * param_guesses["f"]
            param_guesses["x0"] = param_guesses["phi"] / w
            param_guesses["phi"]

        return param_guesses

    def _calculate_derived_params(self):
        self._p["omega"] = 2 * np.pi * self._p["f"]
        self._p['t_pi'] = self._p["t_dead"] + np.pi / self._p["omega"]
        self._p["t_pi_2"] = self._p["t_dead"] + np.pi / 2 / self._p["omega"]
        self._p["phi_cosine"] = self._p["phi"] + np.pi / 2
        self._p["min"] = self._p["offset"] - np.abs(self._p["a"])
        self._p["max"] = self._p["offset"] + np.abs(self._p["a"])
        self._p["period"] = 2 * np.pi / self._p["omega"]

        # TODO: consider covariances
        self._p_err["omega"] = self._p_err["f"] * 2 * np.pi
        self._p_err["t_pi"] = np.sqrt(self._p_err["t_dead"]**2 +
                                      (np.pi / self._p["omega"] *
                                       (self._p_err["omega"] / self._p["omega"]))**2)

        self._p_err["t_pi/2"] = np.sqrt(self._p_err["t_dead"]**2 +
                                        (np.pi / 2 / self._p["omega"] *
                                         (self._p_err["omega"] / self._p["omega"]))**2)

        self._p_err["phi_cosine"] = self._p_err["phi"]
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
        "phi": np.random.uniform(low=0, high=2 * np.pi, size=1),
        "offset": np.random.uniform(low=0.25, high=0.75, size=1),
    }
    y = Sinusoid.func(x, params) + np.random.normal(0, 0.05, x.shape)
    fit = Sinusoid(x, y)

    if not np.isclose(fit.a[0], params["a"], rtol=5e-2):
        print(f"Amplitude error is {100*(1-fit.a[0]/params['a'])}%")
    if not np.isclose(fit.f[0], params["f"], rtol=5e-2):
        print(f"Frequency error is {100*(1-fit.f[0]/params['f'])}%")
    if not np.isclose(fit.phi[0], params["phi"], rtol=5e-2):
        print(f"Phase error is {100*(1-fit.phi[0]/params['phi'])}%")
    if not np.isclose(fit.offset[0], params["offset"], rtol=5e-2):
        print(f"Offset error is {100*(1-fit.offset[0]/params['offset'])}%")

    plt.plot(x, y, label="data")
    x_fit, y_fit = fit.evaluate()
    plt.plot(x_fit, y_fit, label="fit")
    plt.grid()
    plt.show()

import numpy as np
import functools
import collections
from typing import Dict, List, Optional, Tuple, Union
from scipy import optimize

__all__ = ['FitBase']


# TODO: type annotations! (What's the best way of annotating np arrays?)
# TODO: docstring formatting for sphinx
class FitBase:
    def __init__(
        self,
        x=None,
        y=None,
        y_err=None,
        param_bounds: Optional[Dict[str, Tuple[float, float]]] = None,
        fixed_params: Optional[Dict[str, float]] = None,
        initial_values: Optional[Dict[str, float]] = None,
        x_scale: float = 1,
        y_scale: float = 1,
    ):
        """
        If `x` and `y` are provided we fit the provided dataset. `x`, `y` and `y_err`
        may be updated post construction to fit other datasets (c.f. :meth fit:).

        Fit results are exposed as class properties. For example, a fit parameter
        'foo' is accessed via 'fit.foo', which returns a tuple with the fitted value
        of 'foo' and its estimated uncertainty.

        :param x: array of x-axis values.
        :param y: array of y-axis values.
        :param y_err: array of y-axis uncertainties.
        :param param_bounds: dictionary of tuples containing the lower and upper bounds
            for each parameter. If not specified, the defaults from
            :meth get_default_bounds: are used.
        :param fixed_params: dictionary specifying constant values for any non-floated
            parameters. If not specified, the defaults from
            :meth get_default_fixed_params: are used.
        :param initial_values: dictionary specifying initial parameter values to use in
            the fit. These override the values found by :meth estimate_parameters:
        :param x_scale: x-axis scale factor used to normalise parameter values
            during fitting to improve accuracy. See :meth get_default_scale_factors:).
        :param y_scale: y-axis scale factor used to normalise parameter values
            during fitting to improve accuracy. See :meth get_default_scale_factors:).
        """
        self._x = x
        self._y = y
        self._y_err = y_err

        self._validate_param_names(fixed_params)
        if fixed_params is not None:
            self._fixed_params = fixed_params
        else:
            self._fixed_params = self.get_default_fixed_params()
        self._free_params = [
            param for param in self.get_params()
            if param not in self._fixed_params.keys()
        ]

        self._validate_param_names(param_bounds)
        self._param_bounds = self.get_default_bounds()
        self._param_bounds.update(param_bounds or {})
        self._param_bounds = {
            param: bound
            for param, bound in self._param_bounds.items() if param in self._free_params
        }

        self._validate_param_names(initial_values, self._free_params)
        self._initial_values = initial_values or {}

        self._x_scale = x_scale
        self._y_scale = y_scale
        self._scale_factors = self.get_default_scale_factors()
        assert set(self._scale_factors) == set(self.get_params())

        self._p = {}
        self._perr = {}

        def getter(name, instance):
            return instance._p[name], instance._p_err[name]

        cls = type(self)
        for param in self.get_params() + self.get_derived_params():
            setattr(cls, param, property(functools.partial(getter, param)))

        if self._x is not None and self._y is not None:
            self.fit()

    def fit(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        """
        Fit the dataset and return the fitted parameter values and uncertainties.
        """
        valid_pts = np.logical_and(np.isfinite(self._x), np.isfinite(self._y))
        x = self._x[valid_pts]
        y = self._y[valid_pts]
        y_err = None if self._y_err is None else self._y_err[valid_pts]

        initial_values = self._estimate_parameters()
        initial_values.update(self._initial_values)

        lower_bounds = [self._param_bounds[param][0] for param in self._free_params]
        upper_bounds = [self._param_bounds[param][1] for param in self._free_params]

        p0 = [initial_values[param] for param in self._free_params]
        p0 = [max(estimate, lower) for estimate, lower in zip(p0, lower_bounds)]
        p0 = [min(estimate, upper) for estimate, upper in zip(p0, upper_bounds)]

        scale_factors = [
            self._scale_factors.get(param, 1) for param in self._free_params
        ]
        scale_factors = np.asarray(scale_factors)

        p0 = np.asarray(p0) / scale_factors
        lower_bounds = np.asarray(lower_bounds) / scale_factors
        upper_bounds = np.asarray(upper_bounds) / scale_factors

        p, p_cov = optimize.curve_fit(
            f=functools.partial(self._func_free, scale_factors),
            xdata=x,
            ydata=y,
            p0=p0,
            sigma=y_err,
            absolute_sigma=True,
            bounds=(lower_bounds, upper_bounds),
        )

        p *= scale_factors
        p_cov *= scale_factors**2

        self._p_cov = p_cov
        p_err = np.sqrt(np.diag(p_cov))

        self._p = {param: value for param, value in zip(self._free_params, p)}
        self._p_err = {param: value for param, value in zip(self._free_params, p_err)}
        self._p.update(self._fixed_params)
        self._p_err.update({param: 0 for param in self._fixed_params.keys()})

        self._calculate_derived_params()
        assert set(self.get_params() + self.get_derived_params()) == set(self._p.keys())
        return self._p, self._p_err

    def _func_free(self, scale_factors, x, *args):
        """Evaluate the fit function with given values for the free parameters."""
        params = dict(self._fixed_params)

        p = np.asarray(args)
        p *= scale_factors
        params.update({k: v for k, v in zip(self._free_params, p)})

        return self.func(x, params)

    def evaluate(self, x_fit: Union[np.array, int] = 100):
        """Evaluates the function along x-fit and returns the tuple (x_fit, y_fit)
        with the results.

        `x_fit` may either be a scalar or an array. If it is a scalar it gives the
        number of equally spaced points between `min(self.x)` `max(self.x)`. Otherwise
        it gives the x-axis to use.
        """
        if np.isscalar(x_fit):
            x_fit = np.linspace(np.min(self._x), np.max(self._x), x_fit)
        y_fit = self.func(x_fit, self._p)
        return x_fit, y_fit

    @property
    def residuals(self):
        """Returns the fit residuals."""
        return self._y - self._func_free(self._x, self._p)

    def get_default_scale_factors(self) -> Dict[str, float]:
        """ Returns a dictionary of default parameter scale factors. """
        return {param: 1 for param in self.get_params()}

    def _calculate_derived_params(self):
        """
        Updates fit results with values and uncertainties for derived parameters.
        """
        pass

    @staticmethod
    def func(x, params):
        """The fit function."""
        raise NotImplementedError

    @staticmethod
    def get_params() -> List[str]:
        """Returns list of fit params"""
        raise NotImplementedError

    def get_free_params(self) -> List[str]:
        """ Returns a list of the free (not held constant) fit parameters. """
        return self._free_params

    @staticmethod
    def get_derived_params() -> List[str]:
        """Returns list of derived parameters"""
        return []

    @staticmethod
    def get_default_fixed_params() -> Dict[str, float]:
        """Returns a dictionary mapping names of parameters which are not floated by
        default to their values.
        """
        return {}

    @staticmethod
    def get_default_bounds() -> Dict[str, Tuple[float, float]]:
        """
        Returns a dictionary mapping parameter names to a tuple of (upper, lower) bounds
        """
        raise NotImplementedError

    @staticmethod
    def get_default_annotations():
        """ Returns a dictionary of default annotations for the fit. """
        return {}

    def _estimate_parameters(self) -> Dict[str, float]:
        """
        Returns a dictionary of estimates for the parameter values for the current
        dataset.
        """
        raise NotImplementedError

    def _validate_param_names(self, param_names, valid_params=None):
        if param_names is None:
            return

        valid_params = set(
            valid_params if valid_params is not None else self.get_params())
        params = set(param_names)

        duplicates = [
            param for param, count in collections.Counter(params).items() if count > 1
        ]
        if duplicates:
            raise ValueError(f"Duplicate parameters: {','.join(duplicates)}")

        invalid = params - params.intersection(valid_params)
        if invalid:
            raise ValueError(f"Invalid parameters: {','.join(invalid)}")

    @property
    def x(self):
        """ Dataset x axis """
        return self._x

    @x.setter
    def x(self, x):
        self._x = np.asarray(x)
        self._p = {}
        self._perr = {}

    @property
    def y(self):
        """ Dataset y axis """
        return self._y

    @y.setter
    def y(self, y):
        self._y = np.asarray(y)
        self._p = {}
        self._perr = {}

    @property
    def y_err(self):
        """ Dataset y-axis uncertainty """
        return self._y_err

    @y_err.setter
    def y_err(self, y_err=None):
        self._y_err = np.asarray(y_err) if y_err is not None else None
        self._p = {}
        self._perr = {}

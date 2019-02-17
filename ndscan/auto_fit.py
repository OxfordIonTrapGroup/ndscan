"""
Declarative fits, to be excecuted locally by the user interface displaying the data as
it comes in.
"""
import logging
import oitg.fitting
from typing import Any, Callable, Dict, List, Tuple, Union

from .parameters import ParamHandle
from .result_channels import ResultChannel

logger = logging.getLogger(__name__)

#: Registry of well-known fit procecure names.
FIT_OBJECTS = {
    n: getattr(oitg.fitting, n)
    for n in ["exponential_decay", "lorentzian", "rabi_flop"]
}
FIT_OBJECTS["parabola"] = oitg.fitting.shifted_parabola

#: Default points of interest for various fit types (e.g. highlighting the π time for a
#: Rabi flop fit, or the extremum of a parabola.
DEFAULT_POIS = {
    "exponential_decay": {
        "t_1_e": {
            "x": "t_1_e"
        }
    },
    "parabola": {
        "extremum": {
            "x": "position"
        }
    },
    "rabi_flop": {
        "pi_time": {
            "x": "t_pi"
        }
    },
    "lorentzian": {
        "extremum": {
            "x": "x0"
        }
    }
}


class AutoFitSpec:
    """Describes an automatically executed fit for a given combination of scan axes
    and result channels.

    :param fit_type: Fitting procedure name, per :data:`FIT_OBJECTS`.
    :param data: Maps fit data axis names (``"x"``, ``"y"``) to parameter handles or
        result channels that supply the respective data.
    :param points_of_interest: Any points of interest to highlight in the fit results,
        given in the form of a dictionary mapping (arbitrary) identifiers to
        dictionaries mapping coordinate names to fit result names. If ``None``,
        :data:`DEFAULT_POIS` will be queried.
    """

    def __init__(self,
                 fit_type: str,
                 data: Dict[str, Union[ParamHandle, ResultChannel]],
                 points_of_interest: Union[None, Dict[str, Dict[str, Any]]] = None):
        self.fit_type = fit_type
        if fit_type not in FIT_OBJECTS:
            logger.warning("Unknown fit type: '%s'", fit_type, exc_info=True)
        self.data = data
        if points_of_interest is None:
            points_of_interest = DEFAULT_POIS.get(fit_type, {})
        self.points_of_interest = points_of_interest

    def has_data(self, scanned_axes: List[Tuple[str, str]]):
        """Return whether the scanned axes contain the data necessary for this fit to be
        applicable.

        :param scanned_axes: A list of axis identities, i.e. ``(fqn, path_spec)``
            tuples, being scanned over.

        :return: Whether this fit applies or not.
        """
        for arg in self.data.values():
            if isinstance(arg, ParamHandle):
                if not arg._store:
                    return False
                if arg._store.identity not in scanned_axes:
                    return False
        return True

    def describe(self, get_axis_name: Callable[[Tuple[str, str]], str],
                 get_channel_name: Callable[[str], str]) -> Dict[str, Any]:
        """Serialise information about this fit to stringly typed metadata.

        :param get_axis_name: Callable to resolve axis identity to the string to use
            to describe them.
        :param get_channel_name: Callable to resolve result channel path to the string
            to use to describe them.

        :return: A string dictionary representation of this fit specification.
        """

        def describe_argument(obj):
            if isinstance(obj, ParamHandle):
                return get_axis_name(obj._store.identity)
            elif isinstance(obj, ResultChannel):
                return get_channel_name(obj.path)
            else:
                raise ValueError("Invalid fit argument source: {}".format(obj))

        return {
            "fit_type": self.fit_type,
            "data": {name: describe_argument(obj)
                     for name, obj in self.data.items()},
            "pois": list(self.points_of_interest.values())
        }

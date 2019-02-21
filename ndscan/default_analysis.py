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
    for n in ["cos", "exponential_decay", "lorentzian", "rabi_flop"]
}
FIT_OBJECTS["parabola"] = oitg.fitting.shifted_parabola


class DefaultAnalysis:
    def has_data(self, scanned_axes: List[Tuple[str, str]]) -> bool:
        """Return whether the scanned axes contain the data necessary for this analysis
        to be applicable.

        :param scanned_axes: A list of axis identities, i.e. ``(fqn, path_spec)``
            tuples, being scanned over.

        :return: Whether this analysis applies or not.
        """
        raise NotImplementedError

    def describe_online_analyses(
            self, get_axis_name: Callable[[Tuple[str, str]], str],
            get_channel_name: Callable[[ResultChannel], str]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Exceute analysis and serialise information about resulting annotations and
        online analyses to stringly typed metadata.

        :param get_axis_name: Callable to resolve axis identity to the string to use
            to describe them.
        :param get_channel_name: Callable to resolve result channel path to the string
            to use to describe them.

        :return: A tuple of string dictionary representations for annotations and
            online analyses.
        """
        raise NotImplementedError


#: Default points of interest for various fit types (e.g. highlighting the π time for a
#: Rabi flop fit, or the extremum of a parabola.
DEFAULT_FIT_ANNOTATIONS = {
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


class OnlineFit(DefaultAnalysis):
    """Describes an automatically executed fit for a given combination of scan axes
    and result channels.

    :param fit_type: Fitting procedure name, per :data:`FIT_OBJECTS`.
    :param data: Maps fit data axis names (``"x"``, ``"y"``) to parameter handles or
        result channels that supply the respective data.
    :param annotations: Any points of interest to highlight in the fit results,
        given in the form of a dictionary mapping (arbitrary) identifiers to
        dictionaries mapping coordinate names to fit result names. If ``None``,
        :data:`DEFAULT_FIT_ANNOTATIONS` will be queried.
    """

    def __init__(self,
                 fit_type: str,
                 data: Dict[str, Union[ParamHandle, ResultChannel]],
                 annotations: Union[None, Dict[str, Dict[str, Any]]] = None):
        self.fit_type = fit_type
        if fit_type not in FIT_OBJECTS:
            logger.warning("Unknown fit type: '%s'", fit_type, exc_info=True)
        self.data = data
        if annotations is None:
            annotations = DEFAULT_FIT_ANNOTATIONS.get(fit_type, {})
        self.annotations = annotations

    def has_data(self, scanned_axes: List[Tuple[str, str]]):
        for arg in self.data.values():
            if isinstance(arg, ParamHandle):
                if not arg._store:
                    return False
                if arg._store.identity not in scanned_axes:
                    return False
        return True

    def describe_online_analyses(
            self, get_axis_name: Callable[[Tuple[str, str]], str],
            get_channel_name: Callable[[ResultChannel], str]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        def argument_name(obj):
            if isinstance(obj, ParamHandle):
                return get_axis_name(obj._store.identity)
            elif isinstance(obj, ResultChannel):
                return get_channel_name(obj)
            else:
                raise ValueError("Invalid fit argument source: {}".format(obj))

        # FIXME: Allow more than one fit per type.
        fit_analysis_name = "fit_" + self.fit_type

        def analysis_result_desc(key):
            return {
                "kind": "analysis_result",
                "analysis_name": fit_analysis_name,
                "result_key": key
            }

        # TODO: Generalise to higher-dimensional fits.
        annotation_descs = [{
            "kind": "computed_curve",
            "parameters": {
                "function_name":
                self.fit_type,
                "associated_channels": [
                    get_channel_name(v) for v in self.data.values()
                    if isinstance(v, ResultChannel)
                ]
            },
            "data": {
                k: analysis_result_desc(k)
                for k in FIT_OBJECTS[self.fit_type].parameter_names
            }
        }]

        for a in self.annotations.values():
            # TODO: Change API to allow more general annotations.
            if set(a.keys()) == set("x"):
                annotation_descs.append({
                    "kind": "location",
                    "coordinates": {
                        argument_name(self.data["x"]): analysis_result_desc(a["x"])
                    },
                    "data": {
                        argument_name(self.data["x"]) + "_error":
                        analysis_result_desc(a["x"] + "_error")
                    }
                })

        return annotation_descs, {
            fit_analysis_name: {
                "kind": "named_fit",
                "fit_type": self.fit_type,
                "data": {name: argument_name(obj)
                         for name, obj in self.data.items()}
            }
        }

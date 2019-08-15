"""
Declarative fits, to be excecuted locally by the user interface displaying the data as
it comes in.
"""
import logging
from typing import Any, Callable, Dict, List, Iterable, Tuple, Union

from ..utils import FIT_OBJECTS
from .parameters import ParamHandle
from .result_channels import ResultChannel

__all__ = ["Annotation", "DefaultAnalysis", "CustomAnalysis", "OnlineFit"]

logger = logging.getLogger(__name__)


class AnnotationValueRef:
    def __init__(self, kind: str, **kwargs):
        self.spec = {"kind": kind, **kwargs}


class AnnotationContext:
    def __init__(self, get_axis_index: Callable[[ParamHandle], int],
                 name_channel: Callable[[ResultChannel], str]):
        self._get_axis_index = get_axis_index
        self._name_channel = name_channel

    def describe_coordinate(self, obj) -> str:
        if isinstance(obj, ParamHandle):
            return "axis_{}".format(self._get_axis_index(obj))
        if isinstance(obj, ResultChannel):
            return "channel_" + self._name_channel(obj)
        return obj

    def describe_value(self, obj) -> AnnotationValueRef:
        if isinstance(obj, AnnotationValueRef):
            return obj
        if isinstance(obj, ResultChannel):
            return AnnotationValueRef("result_channel", name=self._name_channel(obj))
        # TODO: We would really like to avoid serialising large fit data into the JSON
        # schemata and push it to an appropriately-typed store instead. However, we
        # cannot do this for subscans, as we would need to set up appropriate result
        # channels beforehand. We could (and probably should) set up a different `kind`
        # (e.g. "annotation_data") for top-level scans though that loads from a dataset
        # subtree (e.g. "ndscan.annotation_data.<annotation_name>_<value_key>").
        return AnnotationValueRef("fixed", value=obj)


class Annotation:
    """Annotation to be displayed alongside scan result data, recording derived
    quantities (e.g. a fit minimizer).
    """
    def __init__(self,
                 kind: str,
                 coordinates: dict = {},
                 parameters: dict = {},
                 data: dict = {}):
        self.kind = kind
        self.coordinates = coordinates
        self.parameters = parameters
        self.data = data

    def describe(self, context: AnnotationContext) -> Dict[str, Any]:
        def to_spec_map(dictionary):
            result = {}
            for key, value in dictionary.items():
                keyspec = context.describe_coordinate(key)
                valuespec = context.describe_value(value).spec
                result[keyspec] = valuespec
            return result

        spec = {"kind": self.kind}
        spec["coordinates"] = to_spec_map(self.coordinates)
        spec["parameters"] = self.parameters
        spec["data"] = to_spec_map(self.data)
        return spec


class DefaultAnalysis:
    """Analysis functionality associated with an `ExpFragment` to be executed when that
    fragment is scanned in a particular way.
    """
    def has_data(self, scanned_axes: List[Tuple[str, str]]) -> bool:
        """Return whether the scanned axes contain the data necessary for this analysis
        to be applicable.

        :param scanned_axes: A list of axis identities, i.e. ``(fqn, path_spec)``
            tuples, being scanned over. This is the correct concept of identity to use
            (rather than e.g. directly parameter handles), as an analysis typically
            doesn't care whether a parameter was for instance scanned via the path of
            the particular handle given or a wildcard path spec.

        :return: Whether this analysis applies or not.
        """
        raise NotImplementedError

    def describe_online_analyses(
            self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Exceute analysis and serialise information about resulting annotations and
        online analyses to stringly typed metadata.

        :param context: The AnnotationContext to use to describe the coordinate axes/
            result channels in the resulting metadata.

        :return: A tuple of string dictionary representations for annotations and
            online analyses.
        """
        raise NotImplementedError

    def execute(self, axis_data: Dict[Tuple[str, str], list],
                result_data: Dict[ResultChannel, list],
                context: AnnotationContext) -> List[Dict[str, Any]]:
        """Exceute analysis and serialise information about resulting annotations to
        stringly typed metadata.

        :param context: The AnnotationContext to use to describe the coordinate axes/
            result channels in the resulting metadata.

        :return: A list of string dictionary representations for the resulting
            annotations, if any.
        """
        raise NotImplementedError


class CustomAnalysis(DefaultAnalysis):
    r""":class:`DefaultAnalysis` that executes a user-defined analysis function in the
    `execute()` step.

    No analysis is run online.

    :param required_axes: List of parameters (given by their :class:`.ParamHandle`\ s)
        required to be scanend for the analysis to be applicable.
    :param analyze_fn: The function to invoke in the analysis step. It is passed two
        dictionaries giving list of axis/result channel values for each point of the
        scan to analyse. The function can return a list of :class:`Annotation`\ s to be
        broadcast.
    """
    def __init__(
            self, required_axes: Iterable[ParamHandle],
            analyze_fn: Callable[[Dict[ParamHandle, list], Dict[ResultChannel, list]],
                                 List[Annotation]]):
        self._required_axis_handles = set(required_axes)
        self._analyze_fn = analyze_fn

    def has_data(self, scanned_axes: List[Tuple[str, str]]) -> bool:
        ""
        return all(h._store.identity in scanned_axes
                   for h in self._required_axis_handles)

    def describe_online_analyses(
            self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        ""
        return [], {}

    def execute(self, axis_data: Dict[Tuple[str, str], list],
                result_data: Dict[ResultChannel, list],
                context: AnnotationContext) -> List[Dict[str, Any]]:
        ""
        user_axis_data = {}
        for handle in self._required_axis_handles:
            user_axis_data[handle] = axis_data[handle._store.identity]
        annotations = self._analyze_fn(user_axis_data, result_data)
        if annotations is None:
            # Tolerate the user forgetting the return statement.
            annotations = []
        return [a.describe(context) for a in annotations]


#: Default points of interest for various fit types (e.g. highlighting the π time for a
#: Rabi flop fit, or the extremum of a parabola.
DEFAULT_FIT_ANNOTATIONS = {
    "decaying_sinusoid": {
        "pi_time": {
            "x": "t_max_transfer"
        }
    },
    "exponential_decay": {
        "t_1_e": {
            "x": "t_1_e"
        }
    },
    "lorentzian": {
        "extremum": {
            "x": "x0"
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
    "sinusoid": {
        "pi_time": {
            "x": "t_pi"
        }
    },
}


class OnlineFit(DefaultAnalysis):
    """Describes an automatically executed fit for a given combination of scan axes
    and result channels.

    :param fit_type: Fitting procedure name, per :data:`.FIT_OBJECTS`.
    :param data: Maps fit data axis names (``"x"``, ``"y"``) to parameter handles or
        result channels that supply the respective data.
    :param annotations: Any points of interest to highlight in the fit results,
        given in the form of a dictionary mapping (arbitrary) identifiers to
        dictionaries mapping coordinate names to fit result names. If ``None``,
        :data:`DEFAULT_FIT_ANNOTATIONS` will be queried.
    :param analysis_identifier: Optional explicit name to use for online analysis.
        Defaults to ``fit_<fit_type>``, but can be set explicitly to allow more than one
        fit of a given type at a time.
    """
    def __init__(self,
                 fit_type: str,
                 data: Dict[str, Union[ParamHandle, ResultChannel]],
                 annotations: Union[None, Dict[str, Dict[str, Any]]] = None,
                 analysis_identifier: str = None):
        self.fit_type = fit_type
        if fit_type not in FIT_OBJECTS:
            logger.warning("Unknown fit type: '%s'", fit_type, exc_info=True)
        self.data = data
        if annotations is None:
            annotations = DEFAULT_FIT_ANNOTATIONS.get(fit_type, {})
        self.annotations = annotations
        self.analysis_identifier = analysis_identifier

    def has_data(self, scanned_axes: List[Tuple[str, str]]):
        ""
        num_axes = 0
        for arg in self.data.values():
            if isinstance(arg, ParamHandle):
                num_axes += 1
                if not arg._store:
                    return False
                if arg._store.identity not in scanned_axes:
                    return False
        return len(scanned_axes) == num_axes

    def describe_online_analyses(
            self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        ""
        # TODO: Generalise to higher-dimensional fits.
        channels = [
            context.describe_coordinate(v) for v in self.data.values()
            if isinstance(v, ResultChannel)
        ]

        analysis_identifier = self.analysis_identifier
        if analysis_identifier is None:
            # By default, mangle fit type and channels into a pseudo-unique identifier,
            # which should work for the vast majority of cases (i.e. unless the user
            # creates needlessly duplicate analyses).
            analysis_identifier = "fit_" + self.fit_type + "_" + "_".join(channels)

        def analysis_ref(key):
            return AnnotationValueRef("analysis_result",
                                      analysis_name=analysis_identifier,
                                      result_key=key)

        annotations = [
            Annotation("computed_curve",
                       parameters={
                           "function_name": self.fit_type,
                           "associated_channels": channels
                       },
                       data={
                           k: analysis_ref(k)
                           for k in FIT_OBJECTS[self.fit_type].parameter_names
                       })
        ]
        for a in self.annotations.values():
            # TODO: Change API to allow more general annotations.
            if set(a.keys()) == set("x"):
                annotations.append(
                    Annotation(
                        "location",
                        coordinates={self.data["x"]: analysis_ref(a["x"])},
                        data={
                            context.describe_coordinate(self.data["x"]) + "_error":
                            analysis_ref(a["x"] + "_error")
                        },
                        parameters={"associated_channels": channels}))

        return [a.describe(context) for a in annotations], {
            analysis_identifier: {
                "kind": "named_fit",
                "fit_type": self.fit_type,
                "data": {
                    name: context.describe_coordinate(obj)
                    for name, obj in self.data.items()
                }
            }
        }

    def execute(self, axis_data: Dict[Tuple[str, str], list],
                result_data: Dict[ResultChannel, list],
                context: AnnotationContext) -> List[Dict[str, Any]]:
        ""
        # Nothing to do off-line for online fits.
        return []

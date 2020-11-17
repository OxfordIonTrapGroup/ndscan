"""
Interfaces and declarations for analyses.

Conceptually, analyses are attached to a fragment, and produce results "the next level
up" – that is, they condense all the points from a scan over a particular choice of
parameters into a few parameters.

Two modalities are supported:
 - Declarative fits of a number of pre-defined functions, to be excecuted locally by the
   user interface displaying the result data, and updated as data continues to
   accumulate ("online analysis").
 - A separate analysis step executed at the end, after a scan has been completed. This
   is the equivalent of ARTIQ's ``EnvExperiment.analyze()``, and is executed within the
   master worker process ("execute an analysis", "analysis results").

Both can produce annotations; particular values or plot locations highlighted in the
user interface.
"""
import logging
from typing import Any, Callable, Dict, List, Iterable, Optional, Tuple, Union

from ..utils import FIT_OBJECTS
from .parameters import ParamHandle
from .result_channels import ResultChannel

__all__ = ["Annotation", "DefaultAnalysis", "CustomAnalysis", "OnlineFit"]

logger = logging.getLogger(__name__)


class AnnotationValueRef:
    """Marker type to distinguish an already-serialised annotation value source
    specification from an user-supplied value of dictionary type.
    """
    def __init__(self, kind: str, **kwargs):
        self.spec = {"kind": kind, **kwargs}


class AnnotationContext:
    """Resolves entities in user-specified annotation schemata to stringly-typed
    dictionary form.

    The user-facing interface to annotations allows references to parameters, result
    channels, etc. to be given as their representation in the fragment tree. Thus, to
    write annotations to scan metadata, it is necessary to resolve these to a
    JSON-compatible form to funnel them to the applet (or any number of other dataset
    consumers).

    This class encapsulates the knowledge of the order of scan axes, shortened names of
    result channels, etc. – that is, the global state – necessary to produce these
    schema descriptions.
    """
    def __init__(self, get_axis_index: Callable[[ParamHandle], int],
                 name_channel: Callable[[ResultChannel], str],
                 analysis_result_is_exported: Callable[[ResultChannel], bool]):
        self._get_axis_index = get_axis_index
        self._name_channel = name_channel
        self._analysis_result_is_exported = analysis_result_is_exported

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
            # Only emit analysis result reference if it is actually exported (might not
            # be for a subscan) – emit direct value reference otherwise.
            if self._analysis_result_is_exported(obj):
                return AnnotationValueRef("analysis_result", name=obj.path)
            obj = obj.sink.get_last()
        return AnnotationValueRef("fixed", value=obj)


class Annotation:
    """Annotation to be displayed alongside scan result data, recording derived
    quantities (e.g. a fit minimizer).
    """
    def __init__(self,
                 kind: str,
                 coordinates: Optional[dict] = None,
                 parameters: Optional[dict] = None,
                 data: Optional[dict] = None):
        self.kind = kind
        self.coordinates = {} if coordinates is None else coordinates
        self.parameters = {} if parameters is None else parameters
        self.data = {} if data is None else data

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


#: A tuple ``(fqn, path_spec)`` describing an axis being scanned over. This is the
#: correct concept of identity to use (rather than e.g. directly parameter handles), as
#: an analysis typically doesn't care whether a parameter was for instance scanned via
#: the path of the particular handle given or a wildcard path spec.
AxisIdentity = Tuple[str, str]


class DefaultAnalysis:
    """Analysis functionality associated with an `ExpFragment` to be executed when that
    fragment is scanned in a particular way.
    """
    def has_data(self, scanned_axes: List[AxisIdentity], allow_axes_subset: bool = False) -> bool:
        """Return whether the scanned axes contain the data necessary for this analysis
        to be applicable.

        :param scanned_axes: A list of axis identities being scanned over.
        :return: Whether this analysis applies or not.
        """
        raise NotImplementedError

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Exceute analysis and serialise information about resulting annotations and
        online analyses to stringly typed metadata.

        :param context: The :class:`.AnnotationContext` to use to resolve references to
            fragment tree objects in user-specified data to m.

        :return: A tuple of string dictionary representations for annotations and
            online analyses (with all the fragment tree references resolved).
        """
        raise NotImplementedError

    def get_analysis_results(self) -> Dict[str, ResultChannel]:
        raise NotImplementedError

    def execute(
        self,
        axis_data: Dict[AxisIdentity, list],
        result_data: Dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> List[Dict[str, Any]]:
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
        required to be scanend for the analysis to be applicable. (The order is not
        relevant.)
    :param analyze_fn: The function to invoke in the analysis step. It is passed two
        dictionaries giving list of axis/result channel values for each point of the
        scan to analyse. The function can return a list of :class:`Annotation`\ s to be
        broadcast.
    """
    def __init__(
            self,
            required_axes: Iterable[ParamHandle],
            analyze_fn: Callable[[Dict[ParamHandle, list], Dict[ResultChannel, list]],
                                 Tuple[Dict[str, Any], List[Annotation]]],
            analysis_results: Iterable[ResultChannel] = []):
        self._required_axis_handles = set(required_axes)
        self._analyze_fn = analyze_fn

        self._result_channels = {}
        for channel in analysis_results:
            name = channel.path
            if name in self._result_channels:
                axes = ", ".join(h._store.identity for h in self._required_axis_handles)
                raise ValueError("Duplicate analysis result channel name '" + name +
                                 "' in analysis for axes '" + axes + "'")
            self._result_channels[name] = channel

    def has_data(self, scanned_axes: List[AxisIdentity], allow_axes_subset: bool = False) -> bool:
        ""
        identities = set()
        for h in self._required_axis_handles:
            # If any of the stores has not been created yet, analysis not
            # applicable at this point
            if h._store is None:
                return False
            identities.add(h._store.identity)
        if allow_axes_subset:
            return identities.issubset(set(scanned_axes))
        return identities == set(scanned_axes)

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        ""
        return [], {}

    def get_analysis_results(self) -> Dict[str, ResultChannel]:
        ""
        return self._result_channels

    def execute(
        self,
        axis_data: Dict[AxisIdentity, list],
        result_data: Dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> List[Dict[str, Any]]:
        ""
        user_axis_data = {}
        for handle in self._required_axis_handles:
            user_axis_data[handle] = axis_data[handle._store.identity]

        try:
            annotations = self._analyze_fn(user_axis_data, result_data,
                                           self._result_channels)
        except TypeError as orignal_exception:
            # Tolerate old analysis functions that do not take analysis result channels.
            try:
                annotations = self._analyze_fn(user_axis_data, result_data)
            except TypeError:
                # KLUDGE: If that also fails (e.g. there is a TypeError in the actual
                # implementation), let the original exception bubble up.
                raise orignal_exception

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
    "detuned_square_pulse": {
        "centre": {
            "x": "offset"
        }
    },
    "exponential_decay": {
        "t_1_e": {
            "x": "t_1_e"
        }
    },
    "gaussian": {
        "centre": {
            "x": "x0"
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
    "v_function": {
        "centre": {
            "x": "x0"
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
    :param constants: Specifies parameters to be held constant during the fit. This is
        a dictionary mapping fit parameter names to the respective constant values,
        forwarded to :meth:`oitg.fitting.FitBase.FitBase.fit`.
    :param initial_values: Specifies initial values for the fit parameters. This is
        a dictionary mapping fit parameter names to the respective values, forwarded to
        :meth:`oitg.fitting.FitBase.FitBase.fit`.
    """
    def __init__(self,
                 fit_type: str,
                 data: Dict[str, Union[ParamHandle, ResultChannel]],
                 annotations: Optional[Dict[str, Dict[str, Any]]] = None,
                 analysis_identifier: str = None,
                 constants: Optional[Dict[str, Any]] = None,
                 initial_values: Optional[Dict[str, Any]] = None):
        self.fit_type = fit_type
        if fit_type not in FIT_OBJECTS:
            logger.warning("Unknown fit type: '%s'", fit_type, exc_info=True)
        self.data = data
        if annotations is None:
            annotations = DEFAULT_FIT_ANNOTATIONS.get(fit_type, {})
        self.annotations = annotations
        self.analysis_identifier = analysis_identifier
        self.constants = {} if constants is None else constants
        self.initial_values = {} if initial_values is None else initial_values

    def has_data(self, scanned_axes: List[AxisIdentity], allow_axes_subset: bool = False) -> bool:
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
            return AnnotationValueRef("online_result",
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
                },
                "constants": self.constants,
                "initial_values": self.initial_values
            }
        }

    def get_analysis_results(self) -> Dict[str, ResultChannel]:
        ""
        # Could return annotation locations in the future.
        return {}

    def execute(
        self,
        axis_data: Dict[AxisIdentity, list],
        result_data: Dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> List[Dict[str, Any]]:
        ""
        # Nothing to do off-line for online fits.
        return []

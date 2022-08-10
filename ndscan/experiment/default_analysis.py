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
import numpy as np
import logging
from typing import Any, Callable, Dict, List, Iterable, Optional, Set, Tuple, Union
import collections
import dataclasses

from .parameters import ParamHandle
from .result_channels import ResultChannel, FloatChannel
from .. import utils

__all__ = [
    "Annotation", "DefaultAnalysis", "CustomAnalysis", "OnlineFit",
    "ResultPrefixAnalysisWrapper", "ExportedResult", "FitDescription"
]

logger = logging.getLogger(__name__)


class AnnotationValueRef:
    """Marker type to distinguish an already-serialised annotation value source
    specification from an user-supplied value of dictionary type.
    """
    def __init__(self, kind: str, **kwargs):
        self.spec = {"kind": kind, **kwargs}


@dataclasses.dataclass
class FitDescription():
    """ Description of an online analysis fit.

    Attributes:
        fit_class_name: name of the fit class to use
        fit_module: module that fit_class resides in (must be in the current python
            path)
        data: maps fit data axis names (``"x"``, ``"y"``) to parameter handles or result
            channels that supply the respective data.
        param_bounds: dictionary of tuples containing the lower and upper bounds for
            each parameter. If not specified, the defaults from the fit class are used.
        fixed_params: dictionary specifying constant values for any non-floated
            parameters. If not specified, the defaults from the fit class are used.
        initial_values: dictionary specifying initial parameter values to use in
            the fit. These override the values found by the fit's parameter estimator.
        x_scale: x-axis scale factor used to normalise parameter values during fitting
            to improve accuracy.
    """

    fit_class_name: str
    fit_module: str
    data: dict
    param_bounds: Dict[str, Tuple[float, float]]
    fixed_params: Dict[str, float]
    initial_values: Dict[str, float]
    x_scale: float
    kind: str = dataclasses.field(init=False, default="fit_description")

    @classmethod
    def from_dict(cls, data: dict):
        kind = data.get('kind')
        if kind != cls.kind:
            raise ValueError("Attempt to construct FitDescription from dictionary with"
                             f"incorrect 'kind': {kind}")
        data = dict(data)
        del data['kind']
        return cls(**data)


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
    def required_axes(self) -> Set[ParamHandle]:
        """Return the scan axes necessary for the analysis to apply, in form of the
        parameter handles."""
        raise NotImplementedError

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, FitDescription]]:
        """Exceute analysis and serialise information about resulting annotations and
        online analyses to stringly typed metadata.

        :param context: The :class:`.AnnotationContext` to use to resolve references to
            fragment tree objects in user-specified data.

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
    :meth:`execute` step.

    No analysis is run online.

    :param required_axes: List/set/… of parameters that are required as inputs for the
        analysis to run (given by their :class:`.ParamHandle`\ s). The order of elements
        is inconsequential.
    :param analyze_fn: The function to invoke in the analysis step. It is passed three
        dictionaries:

            1. a map from parameter handles to lists of the respective values for each\
               scan point,

            2. a map from result channels to lists of results for each scan point,

            3. channels for each of the optional analysis results specified in\
               ``analysis_results``, given as a dictionary indexed by channel name.

        For backwards-compatibility, the third parameter can be omitted. Optionally, a
        list of annotations to broadcast can be returned.
    :param analysis_results: Optionally, a number of result channels for analysis
        results. They are later passed to ``analyze_fn``.
    """
    def __init__(
        self,
        required_axes: Iterable[ParamHandle],
        analyze_fn: Callable[[
            Dict[ParamHandle, list], Dict[ResultChannel, list], Dict[str, ResultChannel]
        ], Optional[List[Annotation]]],
        analysis_results: Iterable[ResultChannel] = [],
    ):
        self._required_axis_handles = set(required_axes)
        self._analyze_fn = analyze_fn

        self._result_channels = {}
        for channel in analysis_results:
            name = channel.path
            if name in self._result_channels:
                axes = ", ".join(h.name + "@" + h.owner._stringize_path()
                                 for h in self._required_axis_handles)
                raise ValueError(f"Duplicate analysis result channel name '{name}' " +
                                 f"in analysis for axes [{axes}]")
            self._result_channels[name] = channel

    def required_axes(self) -> Set[ParamHandle]:
        ""
        return self._required_axis_handles

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, FitDescription]]:
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


@dataclasses.dataclass
class ExportedResult:
    """ Analysis result exported from an online fit.

    Attributes:
        fit_parameter: fit parameter to export
        result_name: name of the new result channel to create. Defaults to
            :param fit_parameter: if not specified.
        export_err: if True, we export the uncertainty in the fitted parameter value
            as an additional result with the name (:param result_name: + `_err`)
        result_type: the result channel class for the new result
        config: dictionary of kwargs to pass into the constructor of the result
            channel
    """
    fit_parameter: str
    result_name: Optional[str] = None
    export_err: bool = True
    result_type: ResultChannel = FloatChannel
    config: Optional[Dict] = None

    def __post_init__(self):
        self.result_name = self.result_name or self.fit_parameter
        self.config = self.config or {}


class OnlineFit(DefaultAnalysis):
    """Describes an automatically executed fit for a given combination of scan axes
    and result channels.

    :param fit_class: name of the python class within :param fit_module: to use for the
        fit.
    :param data: Maps fit data axis names (``"x"``, ``"y"``) to parameter handles or
        result channels that supply the respective data.
    :param annotations: Any points of interest to highlight in the fit results,
        given in the form of a dictionary mapping (arbitrary) identifiers to
        dictionaries mapping coordinate names to fit result names. If ``None``,
        the defaults provided by the fit function will be used.
    :param analysis_identifier: Optional explicit name to use for online analysis.
        Defaults to ``fit_<fit_type>``, but can be set explicitly to allow more than one
        fit of a given type at a time.
    :param constants: dictionary specifying constant values for any non-floated
        parameters. If not specified, the defaults from the fit class are used.
    :param initial_values: dictionary specifying initial parameter values to use in
        the fit. These override the values found by the fit's parameter estimator.
    :param bounds: dictionary of tuples containing the lower and upper bounds for
            each parameter. If not specified, the defaults from the fit class are used.
    :param exported_results: Specifies fitted parameter values to export as analysis
        results.
    :param fit_module: python module containing the fit class. Will default to
        `ndscan.fitting` in the future. To use the oitg fitting functions, `fit_module`
        should be set to the default `ndscan.fitting.oitg`.
    """
    def __init__(self,
                 fit_class: str,
                 data: Dict[str, Union[ParamHandle, ResultChannel]],
                 annotations: Optional[Dict[str, Dict[str, Any]]] = None,
                 analysis_identifier: str = None,
                 constants: Optional[Dict[str, Any]] = None,
                 initial_values: Optional[Dict[str, Any]] = None,
                 bounds: Optional[Dict[str, Tuple[float, float]]] = None,
                 exported_results: Optional[List[ExportedResult]] = None,
                 fit_module: str = 'ndscan.fitting.oitg'):

        self.fit_class_name = fit_class
        self.fit_module = fit_module
        self.data = data
        self.annotations = annotations
        self.analysis_identifier = analysis_identifier
        self.initial_values = initial_values or {}
        self.exported_results = exported_results or []

        x_param_handle = self.data['x']
        self.x_scale = x_param_handle.param.scale

        self._result_channels = []
        for result in self.exported_results:
            channel = result.result_type(path=result.result_name, **result.config)
            self._result_channels.append(channel)

            if result.export_err:
                err_channel = FloatChannel(
                    path=result.result_name + '_err',
                    min=0.0,
                    display_hints={"error_bar_for": channel.path})
                self._result_channels.append(err_channel)

        duplicate_channels = [
            channel.path
            for channel, count in collections.Counter(self._result_channels).items()
            if count > 1
        ]
        if duplicate_channels:
            raise ValueError(
                f"Duplicate result channels: {','.join(duplicate_channels)}")
        self._result_channels = {
            channel.path: channel
            for channel in self._result_channels
        }

        klass = utils.import_class(self.fit_module, self.fit_class_name)
        self.fit_klass = klass
        self.bounds = bounds if bounds is not None else klass.get_default_bounds()

        if annotations is not None:
            self.annotations = annotations
        else:
            self.annotations = self.fit_klass.get_default_annotations()

        if constants is not None:
            self.constants = constants
        else:
            self.constants = self.fit_klass.get_default_fixed_params()

    def required_axes(self) -> Set[ParamHandle]:
        ""
        return set(a for a in self.data.values() if isinstance(a, ParamHandle))

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, FitDescription]]:
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
            analysis_identifier = ("fit_" + f"{self.fit_module}.{self.fit_class_name}" +
                                   "_" + "_".join(channels))

        def analysis_ref(key):
            return AnnotationValueRef("online_result",
                                      analysis_name=analysis_identifier,
                                      result_key=key)

        fit_params = self.fit_klass.get_params()

        annotations = [
            Annotation("computed_curve",
                       parameters={
                           "fit_class_name": self.fit_class_name,
                           "fit_module": self.fit_module,
                           "associated_channels": channels,
                       },
                       data={param: analysis_ref(param)
                             for param in fit_params})
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

        analysis = {
            analysis_identifier:
            FitDescription(fit_class_name=self.fit_class_name,
                           fit_module=self.fit_module,
                           data={
                               name: context.describe_coordinate(obj)
                               for name, obj in self.data.items()
                           },
                           param_bounds=self.bounds,
                           fixed_params=self.constants,
                           initial_values=self.initial_values,
                           x_scale=self.x_scale)
        }
        return [a.describe(context) for a in annotations], analysis

    def get_analysis_results(self) -> Dict[str, ResultChannel]:
        ""
        # Could return annotation locations in the future.
        return self._result_channels

    def execute(
        self,
        axis_data: Dict[AxisIdentity, list],
        result_data: Dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> List[Dict[str, Any]]:
        ""
        user_axis_data = {}
        for handle in self.required_axes():
            user_axis_data[handle] = axis_data[handle._store.identity]
        # TODO: Generalise to higher-dimensional fits.
        x = axis_data[self.data['x']._store.identity]
        y = result_data[self.data['y']]

        fit = self.fit_klass(x=np.asarray(x),
                             y=np.asarray(y),
                             y_err=None,
                             param_bounds=self.bounds,
                             fixed_params=self.constants,
                             initial_values=self.initial_values,
                             x_scale=self.x_scale)
        for result in self.exported_results:
            p, p_err = getattr(fit, result.fit_parameter)
            channel = self._result_channels[result.result_name]
            channel.push(p)
            if result.export_err:
                err_channel = self._result_channels[result.result_name + '_err']
                err_channel.push(p_err)
        return []


class ResultPrefixAnalysisWrapper(DefaultAnalysis):
    """Wraps another default analysis, prepending the given string to the name of each
    analysis result.

    This can be used to disambiguate potential conflicts between result names when
    programmatically collecting analyses from multiple sources.
    """
    def __init__(self, wrapped: DefaultAnalysis, prefix: str):
        """
        :param wrapped: The :class:`.DefaultAnalysis` instance to forward to.
        :param prefix: The string to prepend to the name of each analysis result.
        """
        self._wrapped = wrapped
        self._prefix = prefix

    def required_axes(self) -> Set[ParamHandle]:
        return self._wrapped.required_axes()

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> Tuple[List[Dict[str, Any]], Dict[str, FitDescription]]:
        return self._wrapped.describe_online_analyses(context)

    def get_analysis_results(self) -> Dict[str, ResultChannel]:
        # TODO: Prepend to ResultChannel.path as well? For now, nothing relies on the
        # path schema entry for analysis results, so it's a wash.
        return {
            self._prefix + k: v
            for k, v in self._wrapped.get_analysis_results().items()
        }

    def execute(
        self,
        axis_data: Dict[AxisIdentity, list],
        result_data: Dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> List[Dict[str, Any]]:
        return self._wrapped.execute(axis_data, result_data, context)

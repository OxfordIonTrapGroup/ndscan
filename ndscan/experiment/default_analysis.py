r"""
Interfaces and declarations for analyses.

Conceptually, analyses are attached to a fragment, and produce results "the next level
up" – that is, they condense all the points from a scan over a particular choice of
parameters into a few derived results.

Two modalities are supported:
 - Declarative fits of a number of pre-defined functions, to be executed locally by the
   user interface displaying the result data, and updated as data continues to
   accumulate ("online analysis").
 - A separate analysis step executed at the end, after a scan has been completed. This
   is the equivalent of ARTIQ's ``EnvExperiment.analyze()``, and is executed within the
   master worker process ("execute an analysis", "analysis results").

Both can produce :class:`Annotation`\ s; particular values or plot locations highlighted
in the user interface.
"""

import logging
from collections.abc import Callable, Iterable
from typing import Any

from ..utils import FIT_OBJECTS
from .annotations import (
    Annotation,
    AnnotationContext,
    AnnotationValueRef,
    axis_location,
    computed_curve,
)
from .parameters import ParamHandle
from .result_channels import ResultChannel

__all__ = [
    "Annotation",
    "DefaultAnalysis",
    "CustomAnalysis",
    "OnlineFit",
    "ResultPrefixAnalysisWrapper",
]

logger = logging.getLogger(__name__)

#: A tuple ``(fqn, path_spec)`` describing an axis being scanned over. This is the
#: correct concept of identity to use (rather than e.g. directly parameter handles), as
#: an analysis typically doesn't care whether a parameter was for instance scanned via
#: the path of the particular handle given or a wildcard path spec.
AxisIdentity = tuple[str, str]


class DefaultAnalysis:
    """Analysis functionality associated with an `ExpFragment` to be executed when that
    fragment is scanned in a particular way.
    """

    def required_axes(self) -> set[ParamHandle]:
        """Return the scan axes necessary for the analysis to apply, in form of the
        parameter handles."""
        raise NotImplementedError

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        """Exceute analysis and serialise information about resulting annotations and
        online analyses to stringly typed metadata.

        :param context: The :class:`.AnnotationContext` to use to resolve references to
            fragment tree objects in user-specified data.

        :return: A tuple of string dictionary representations for annotations and
            online analyses (with all the fragment tree references resolved).
        """
        raise NotImplementedError

    def get_analysis_results(self) -> dict[str, ResultChannel]:
        r"""Return :class:`ResultChannel`\ s for the results produced by the analysis,
        as a dictionary indexed by name.
        """
        raise NotImplementedError

    def execute(
        self,
        axis_data: dict[AxisIdentity, list],
        result_data: dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> list[dict[str, Any]]:
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
        analyze_fn: Callable[
            [
                dict[ParamHandle, list],
                dict[ResultChannel, list],
                dict[str, ResultChannel],
            ],
            list[Annotation] | None,
        ],
        analysis_results: Iterable[ResultChannel] = [],
    ):
        self._required_axis_handles = set(required_axes)
        self._analyze_fn = analyze_fn

        self._result_channels = {}
        for channel in analysis_results:
            name = channel.path
            if name in self._result_channels:
                axes = ", ".join(
                    h.name + "@" + h.owner._stringize_path()
                    for h in self._required_axis_handles
                )
                raise ValueError(
                    f"Duplicate analysis result channel name '{name}' "
                    + f"in analysis for axes [{axes}]"
                )
            self._result_channels[name] = channel

    def required_axes(self) -> set[ParamHandle]:
        ""
        return self._required_axis_handles

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        ""
        return [], {}

    def get_analysis_results(self) -> dict[str, ResultChannel]:
        ""
        return self._result_channels

    def execute(
        self,
        axis_data: dict[AxisIdentity, list],
        result_data: dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> list[dict[str, Any]]:
        ""
        user_axis_data = {}
        for handle in self._required_axis_handles:
            user_axis_data[handle] = axis_data[handle._store.identity]

        try:
            annotations = self._analyze_fn(
                user_axis_data, result_data, self._result_channels
            )
        except TypeError as orignal_exception:
            # Tolerate old analysis functions that do not take analysis result channels.
            try:
                annotations = self._analyze_fn(user_axis_data, result_data)
            except TypeError:
                # KLUDGE: If that also fails (e.g. there is a TypeError in the actual
                # implementation), let the original exception bubble up.
                raise orignal_exception from None

        if annotations is None:
            # Tolerate the user forgetting the return statement.
            annotations = []
        return [a.describe(context) for a in annotations]


#: Default points of interest for various fit types (e.g. highlighting the π time for a
#: Rabi flop fit, or the extremum of a parabola.
DEFAULT_FIT_ANNOTATIONS = {
    "decaying_sinusoid": {"pi_time": {"x": "t_max_transfer"}},
    "detuned_square_pulse": {"centre": {"x": "offset"}},
    "exponential_decay": {"t_1_e": {"x": "t_1_e"}},
    "gaussian": {"centre": {"x": "x0"}},
    "lorentzian": {"extremum": {"x": "x0"}},
    "parabola": {"extremum": {"x": "position"}},
    "rabi_flop": {"pi_time": {"x": "t_pi"}},
    "sinusoid": {"pi_time": {"x": "t_pi"}},
    "v_function": {"centre": {"x": "x0"}},
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

    def __init__(
        self,
        fit_type: str,
        data: dict[str, ParamHandle | ResultChannel],
        annotations: dict[str, dict[str, Any]] | None = None,
        analysis_identifier: str = None,
        constants: dict[str, Any] | None = None,
        initial_values: dict[str, Any] | None = None,
    ):
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

    def required_axes(self) -> set[ParamHandle]:
        ""
        return {a for a in self.data.values() if isinstance(a, ParamHandle)}

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        ""
        # TODO: Generalise to higher-dimensional fits.
        channels = [
            context.describe_coordinate(v)
            for v in self.data.values()
            if isinstance(v, ResultChannel)
        ]

        analysis_identifier = self.analysis_identifier
        if analysis_identifier is None:
            # By default, mangle fit type and channels into a pseudo-unique identifier,
            # which should work for the vast majority of cases (i.e. unless the user
            # creates needlessly duplicate analyses).
            analysis_identifier = "fit_" + self.fit_type + "_" + "_".join(channels)

        def analysis_ref(key):
            return AnnotationValueRef(
                "online_result", analysis_name=analysis_identifier, result_key=key
            )

        annotations = [
            computed_curve(
                function_name=self.fit_type,
                parameters={
                    k: analysis_ref(k)
                    for k in FIT_OBJECTS[self.fit_type].parameter_names
                },
                associated_channels=channels,
            )
        ]
        for a in self.annotations.values():
            # TODO: Change API to allow more general annotations.
            if set(a.keys()) == set("x"):
                annotations.append(
                    axis_location(
                        axis=self.data["x"],
                        position=analysis_ref(a["x"]),
                        position_error=analysis_ref(a["x"] + "_error"),
                        associated_channels=channels,
                    )
                )

        return [a.describe(context) for a in annotations], {
            analysis_identifier: {
                "kind": "named_fit",
                "fit_type": self.fit_type,
                "data": {
                    name: context.describe_coordinate(obj)
                    for name, obj in self.data.items()
                },
                "constants": self.constants,
                "initial_values": self.initial_values,
            }
        }

    def get_analysis_results(self) -> dict[str, ResultChannel]:
        ""
        # Could return annotation locations in the future.
        return {}

    def execute(
        self,
        axis_data: dict[AxisIdentity, list],
        result_data: dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> list[dict[str, Any]]:
        ""
        # Nothing to do off-line for online fits.
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

    def required_axes(self) -> set[ParamHandle]:
        return self._wrapped.required_axes()

    def describe_online_analyses(
        self, context: AnnotationContext
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        return self._wrapped.describe_online_analyses(context)

    def get_analysis_results(self) -> dict[str, ResultChannel]:
        # TODO: Prepend to ResultChannel.path as well? For now, nothing relies on the
        # path schema entry for analysis results, so it's a wash.
        return {
            self._prefix + k: v for k, v in self._wrapped.get_analysis_results().items()
        }

    def execute(
        self,
        axis_data: dict[AxisIdentity, list],
        result_data: dict[ResultChannel, list],
        context: AnnotationContext,
    ) -> list[dict[str, Any]]:
        return self._wrapped.execute(axis_data, result_data, context)

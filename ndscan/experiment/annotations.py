"""
Annotations are a way for analyses (:mod:`ndscan.experiment.default_analysis`) to
specify additional information to be displayed to the user along with the scan data, for
instance a fit curve or a line indicating a centre frequency or oscillation period
derived from the analysis.

Conceptually, annotations express hints about a suitable user interface for exploration
of experimental data, rather than a mechanism for data storage or exchange. To make
analysis results programmatically accessible to other parts fo a complex experiment,
analysis result channels (see
:meth:`.DefaultAnalysis.get_analysis_results`) should be used instead.
"""
from dataclasses import dataclass
import numpy as np
from typing import Any, Callable
from .parameters import ParamHandle
from .result_channels import ResultChannel
from ..utils import FIT_OBJECTS

__all__ = ["Annotation", "curve_1d", "curve", "computed_curve", "axis_location"]


class AnnotationValueRef:
    """Marker type to distinguish an already-serialised annotation value source
    specification from an user-supplied value of dictionary type.
    """
    def __init__(self, kind: str, **kwargs):
        self.spec = {"kind": kind, **kwargs}


@dataclass(frozen=True)
class AxisAssociatedKeyRef:
    """Kludge to allow referring to an axis in annotation data before the actual index
    is known (from the AnnotationContext when describe()ing the annotation).
    """
    axis: ParamHandle
    key: str


class AnnotationContext:
    """Resolves entities in user-specified annotation schemata to stringly-typed
    dictionary form.

    The user-facing interface to annotations allows references to parameters, result
    channels, etc. to be given as their representation in the fragment tree. Thus, to
    write annotations to scan metadata, it is necessary to resolve these to a
    JSON-compatible form to funnel them to the applet (or any number of other dataset
    consumers).

    This is not quite an abstract interface, in that the class already encapsulates the
    information on how to refer to the different object types in the scan schema. What
    is, however, still missing is the knowledge of the order of scan axes, the shortened
    names of result channels, etc. – that is, the scan-global state – necessary to
    actually derive the final stringly-typed descriptions, which are supplied via
    callbacks.

    :param get_axis_index: Given a :class:`ParamHandle` that is being scanned over,
        return its index among the scan axes.
    :param name_channel: Given a :class:`ResultChannel`, return the flattened name
        in the context of the scan (as used e.g. for the ``points.channel_…`` dataset
        key).
    :param analysis_result_is_exported: Given an analysis result :class:`ResultChannel`,
        return whether it is actually accessible in the datasets (e.g. subscan result
        channels might not be exposed).
    """
    def __init__(self, get_axis_index: Callable[[ParamHandle], int],
                 name_channel: Callable[[ResultChannel], str],
                 analysis_result_is_exported: Callable[[ResultChannel], bool]):
        self._get_axis_index = get_axis_index
        self._name_channel = name_channel
        self._analysis_result_is_exported = analysis_result_is_exported

    def describe_coordinate(self, obj) -> str:
        if isinstance(obj, ParamHandle):
            return f"axis_{self._get_axis_index(obj)}"
        if isinstance(obj, ResultChannel):
            return "channel_" + self._name_channel(obj)
        if isinstance(obj, AxisAssociatedKeyRef):
            return self.describe_coordinate(obj.axis) + "_" + obj.key
        return obj

    def describe_value(self, obj) -> AnnotationValueRef:
        if isinstance(obj, AnnotationValueRef):
            return obj
        if isinstance(obj, ResultChannel):
            # Only emit analysis result reference if it is actually exported; write out
            # the value inline otherwise.
            if self._analysis_result_is_exported(obj):
                return AnnotationValueRef("analysis_result", name=obj.path)
            obj = obj.sink.get_last()
        return AnnotationValueRef("fixed", value=obj)


class Annotation:
    """An annotation to be displayed alongside scan result data, recording derived
    quantities (e.g. a fit minimizer).

    See :func:`curve`, :func:`curve_1d`, :func:`computed_curve`, :func:`axis_location`.
    """
    def __init__(self,
                 kind: str,
                 coordinates: dict | None = None,
                 parameters: dict | None = None,
                 data: dict | None = None):
        self.kind = kind
        self.coordinates = {} if coordinates is None else coordinates
        self.parameters = {} if parameters is None else parameters
        self.data = {} if data is None else data

    def describe(self, context: AnnotationContext) -> dict[str, Any]:
        def to_spec_map(dictionary):
            result = {}
            for key, value in dictionary.items():
                keyspec = context.describe_coordinate(key)
                valuespec = context.describe_value(value).spec
                result[keyspec] = valuespec
            return result

        spec = {"kind": self.kind}
        spec["coordinates"] = to_spec_map(self.coordinates)
        # FIXME: to_spec_map() on parameters as well for associated_channels?
        spec["parameters"] = self.parameters
        spec["data"] = to_spec_map(self.data)
        return spec


def curve(
    coordinates: dict[ParamHandle | ResultChannel,
                      list[float] | np.ndarray]) -> Annotation:
    """Create a curve annotation from a dictionary of coordinate lists.

    This will typically be shown as a connected line in the plot applet. See
    :func:`curve_1d` for an alternative signature that is slightly more
    explicit/self-documenting for one-dimensional scans (the only type of curve
    supported in the plot applet at this point).

    If the curve data comes from a functional relationship matching one of the
    predefined fit types (:data:`ndscan.util.FIT_OBJECTS`), prefer
    :func:`computed_curve`, as this allows for unlimited resolution (also if the user
    looks at a range outside that corresponding to the source scan) and is more
    efficient to store.

    :param coordinates: A dictionary mapping, for each dimension, the axis in question
        (:class:`.ParamHandle`/:class:`.ResultChannel`) to a list of coordinates for
        each curve point. Each list must have the same length.

    :return: The :class:`Annotation` object describing the curve.
    """
    num_points = None

    def normalise(key, values):
        if isinstance(values, np.ndarray):
            values = values.tolist()
        nonlocal num_points
        if num_points is None:
            num_points = len(values)
        elif len(values) != num_points:
            raise ValueError(
                f"Got {len(values)} values for '{key}', previously had {num_points}")
        return values

    return Annotation("curve",
                      coordinates={k: normalise(k, v)
                                   for k, v in coordinates.items()})


def curve_1d(x_axis: ParamHandle,
             x_values: list[float] | np.ndarray | AnnotationValueRef,
             y_axis: ResultChannel,
             y_values: list[float] | np.ndarray | AnnotationValueRef) -> Annotation:
    """Create a curve annotation from explicit lists of x and y coordinates.

    This will typically be shown as a connected line in the plot applet. See
    :func:`curve` for a generic variant covering multiple dimensions (though curve
    annotations are currently only displayed for one-dimensional scans in the applet).

    If the curve data comes from a functional relationship matching one of the
    predefined fit types (:data:`ndscan.util.FIT_OBJECTS`), prefer
    :func:`computed_curve_1d` as this allows for unlimited resolution (also if the user
    looks at a range outside that corresponding to the source scan) and is more
    efficient to store.

    :param x_axis: The parameter corresponding to the x axis of the curve.
    :param x_values: A list of x coordinates for the curve points.
    :param y_axis: The result channel corresponding to the y axis of the curve.
    :param y_values: A list of y coordinates for the curve points.

    :return: The :class:`Annotation` object describing the curve.
    """
    return curve({x_axis: x_values, y_axis: y_values})


def computed_curve(function_name: str,
                   parameters: dict[str, Any | AnnotationValueRef],
                   associated_channels: list | None = None) -> Annotation:
    """Create a curve annotation that is computed from a well-known fit function
    (:data:`ndscan.util.FIT_OBJECTS`).

    This will typically be shown as a connected line in the plot applet. See
    :func:`curve`/:func:`curve_1d` for a variant defined by a discrete list of points
    instead of the evaluation of a function.

    :param function_name: The name of the function to use, matching the keys in
        :data:`ndscan.util.FIT_OBJECTS`.
    :param parameters: The fixed parameters to evaluate the function with at each point,
        given as a dictionary (see :attr:`oitg.fitting.FitBase.parameter_names` for the
        expected keys).
    :param associated_channels: If given, the curve will be shown on the same
        axis/plot/etc. as the given result channels (possibly more than once). Prefer
        explicitly specifying this to avoid unexpected behaviour if e.g. additional
        result channels with different logical meanings (units, etc.) are added to the
        experiment later.

    :return: The :class:`Annotation` object describing the curve.
    """
    if function_name not in FIT_OBJECTS.keys():
        known_types = ", ".join(FIT_OBJECTS.keys())
        raise ValueError(f"Computed curve type '{function_name}' is not among the " +
                         f"known FIT_OBJECTS ({known_types})")

    given_params = set(parameters.keys())
    expected_params = set(FIT_OBJECTS[function_name].parameter_names)
    if given_params != expected_params:
        raise ValueError(f"Unexpected parameters for curve type '{function_name}' " +
                         f"(expected {expected_params}, got {given_params})")

    params = {"function_name": function_name}
    if associated_channels:
        params["associated_channels"] = associated_channels
    return Annotation("computed_curve", parameters=params, data=parameters)


def axis_location(axis: ParamHandle | ResultChannel,
                  position: Any | AnnotationValueRef,
                  position_error: float | AnnotationValueRef | None = None,
                  associated_channels: list | None = None) -> Annotation:
    """Create an annotation marking a specific location on the given axis.

    This will typically be shown as a vertical/horizontal line in the plot applet
    (though currently only vertical lines for x axis positions are implemented on the
    applet side).

    :param axis: The parameter or result channel the location corresponds to.
    :param position: The location to mark, given in the same units as the axis or
        parameter value. Typically a numerical value, though once scans over
        non-numerical axes are implemented, this could also be one instance of a
        categorical value.
    :param position_error: Optionally, the uncertainty ("error bar") associated with the
        given position.
    :param associated_channels: If given, the curve will be shown on the same
        axis/plot/etc. as the given result channels (possibly more than once). Prefer
        explicitly specifying this to avoid unexpected behaviour if e.g. additional
        result channels with different logical meanings (units, etc.) are added to the
        experiment later.

    :return: The :class:`Annotation` object describing the curve.
    """
    parameters = {}
    if associated_channels:
        parameters["associated_channels"] = associated_channels
    data = {}
    if position_error is not None:
        data[AxisAssociatedKeyRef(axis, "error")] = position_error
    return Annotation("location",
                      coordinates={axis: position},
                      parameters=parameters,
                      data=data)

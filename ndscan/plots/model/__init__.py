r"""This package contains abstractions for the data source of a plot, and several
implementations thereof.

This is a fairly thin layer above the results (meta)data generated by experiments,
addressing two main points:

 * We want to use the same type of plot (1D/2D/…) in several different contexts – not
   only as the top-level scan while an experiment is running, but also to display
   subscans, or for displaying ARTIQ HDF5 files after the fact.
 * The data we want to show is dynamic: new data points come in over time as an
   experiment is running, fits are recomputed, the user might select a different
   point to display a subscan for, and so on. We want to react to those changes, yet
   avoid re-creating the entire hierarchy of plot objects on every update to keep the
   user interface responsive.

To achieve this, the data for a given plot is represented as a tree of ``QObject``\ s
that emit signals when their direct children changed in a specific way (e.g.
``channel_schemata_changed``, ``points_appended``, …).

(This is somewhat of an ad-hoc solution which works well since the described data model
is fairly small in complexity; a more principled approach for creating models with
different granularities of change tracking might be more appropriate for more complex
situations.)
"""

import logging
from quamash import QtCore
from typing import Any, Callable, Dict, List, Union
from .online_analysis import OnlineNamedFitAnalysis

logger = logging.getLogger(__name__)


class Context(QtCore.QObject):
    """Describes the environment in which a certain plot is displayed.

    This is the moral equivalent of a container for global variables and should be used
    only sparsely (i.e. for actual properties of the environment).
    """

    title_changed = QtCore.pyqtSignal(str)

    def __init__(self, set_dataset: Callable[[str, Any], None] = None):
        super().__init__()
        self._set_dataset = set_dataset
        self.title = ""

    def set_title(self, title: str) -> None:
        if title != self.title:
            self.title = title
            self.title_changed.emit(title)

    def is_online_master(self) -> bool:
        """Return whether the plot is run in an environment where there is a connection
        to an ARTIQ master (as opposed to e.g. displaying an offline results file).
        """
        return self._set_dataset is not None

    def set_dataset(self, key: str, value: Any) -> None:
        """Sets dataset ``key`` to ``value`` on the connected master, if any.

        See: :meth:`is_online_master`.
        """
        self._set_dataset(key, value)


class AnnotationDataSource(QtCore.QObject):
    changed = QtCore.pyqtSignal()

    def get(self) -> Any:
        raise NotImplementedError


class FixedDataSource(AnnotationDataSource):
    def __init__(self, value):
        super().__init__()
        self._value = value

    def get(self) -> Any:
        return self._value


class OnlineAnalysisDataSource(AnnotationDataSource):
    def __init__(self, analysis, key):
        super().__init__()
        self._analysis = analysis
        self._analysis.updated.connect(self.changed)
        self._key = key

    def get(self) -> Any:
        return self._analysis.get_data().get(self._key, None)


class Annotation:
    def __init__(self, kind: str, parameters: Dict[str, Any],
                 coordinates: Dict[str, AnnotationDataSource],
                 data: Dict[str, AnnotationDataSource]):
        self.kind = kind
        self.parameters = parameters
        self.coordinates = coordinates
        self.data = data


class Root(QtCore.QObject):
    """The root of a plot data tree, i.e. all the data making up a plot displayed in a
    given window.

    This is a reference to (at most) one :class:`Model`, but makes it possible to
    represent situations where the model is not yet known (e.g. because we are still
    waiting for the experiment to set the top-level metadata datasets), or might change
    (because we are showing a subscan for a user-selected point).
    """

    model_changed = QtCore.pyqtSignal(object)

    def get_model(self) -> Union["Model", None]:
        raise NotImplementedError


class Model(QtCore.QObject):
    channel_schemata_changed = QtCore.pyqtSignal(dict)

    def __init__(self, context: Context):
        super().__init__()
        self.context = context

    def get_channel_schemata(self) -> Dict[str, Any]:
        raise NotImplementedError


class SinglePointModel(Model):
    point_changed = QtCore.pyqtSignal(object)

    def get_point(self) -> Union[None, Dict[str, Any]]:
        raise NotImplementedError


class ScanModel(Model):
    points_rewritten = QtCore.pyqtSignal(dict)
    points_appended = QtCore.pyqtSignal(dict)
    annotations_changed = QtCore.pyqtSignal(list)

    def __init__(self, axes: List[Dict[str, Any]], context: Context):
        super().__init__(context)
        self.axes = axes
        self._annotations = []
        self._annotation_schemata = []
        self._online_analyses = {}

    def get_point_data(self) -> Dict[str, Any]:
        raise NotImplementedError

    def get_annotations(self) -> List[Annotation]:
        return self._annotations

    def _set_annotation_schemata(self, schemata: List[Dict[str, Any]]):
        self._annotation_schemata = schemata
        self._annotations = []

        def data_source(spec):
            kind = spec["kind"]
            if kind == "fixed":
                return FixedDataSource(spec["value"])
            if kind == "analysis_result":
                analysis = self._online_analyses.get(spec["analysis_name"], None)
                if analysis is None:
                    return None
                return OnlineAnalysisDataSource(analysis, spec["result_key"])
            logger.info("Ignoring unsupported annotation data source type: '%s'", kind)
            return None

        def to_data_sources(specs):
            return {k: data_source(v) for k, v in specs.items()}

        for schema in schemata:
            sources = [to_data_sources(schema.get(n)) for n in ("coordinates", "data")]
            self._annotations.append(
                Annotation(schema["kind"], schema.get("parameters", {}), *sources))
        self.annotations_changed.emit(self._annotations)

    def _set_online_analyses(self,
                             analysis_schemata: Dict[str, Dict[str, Any]]) -> None:
        for a in self._online_analyses.values():
            a.stop()
        self._online_analyses = {}

        for name, schema in analysis_schemata.items():
            kind = schema["kind"]
            if kind == "named_fit":
                self._online_analyses[name] = OnlineNamedFitAnalysis(schema, self)
            else:
                logger.warning("Ignoring unsupported online analysis type: '%s'", kind)

        # Rebind annotation schemata to new analysis data sources.
        self._set_annotation_schemata(self._annotation_schemata)

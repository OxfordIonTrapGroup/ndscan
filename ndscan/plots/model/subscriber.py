import json
from collections.abc import Iterable
from typing import Any

from sipyco.sync_struct import ModAction

from ...utils import SCHEMA_REVISION_KEY, strip_prefix
from . import (
    Annotation,
    Context,
    FixedDataSource,
    Model,
    Root,
    ScanModel,
    SinglePointModel,
)


class SubscriberRoot(Root):
    """Scan root fed from artiq.applets.simple data_changed callbacks, listening to the
    top-level ndscan dataset.

    :param prefix: Prefix of the ndscan dataset tree to represent, e.g.
        ``"ndscan."`` for the default location.
    """

    def __init__(self, prefix: str, context: Context):
        super().__init__()

        self._prefix = prefix
        self._context = context
        self._model = None

        # For root dataset sources, scan metadata doesn't change once it's been set.
        self._schema_revision = None
        self._title_set = False
        self._source_id_set = False
        self._axes_initialised = False

    def data_changed(
        self, values: dict[str, Any], mods: Iterable[dict[str, Any]]
    ) -> None:
        def d(name):
            return values.get(self._prefix + name)

        # Wait until schema revision is set before proceeding.
        schema_revision = d(SCHEMA_REVISION_KEY)
        if schema_revision is None:
            return

        fqn = d("fragment_fqn")
        if not self._title_set or self._context.get_title() != fqn:
            if fqn:
                self._context.set_title(fqn)
                self._title_set = True

        source_id = d("source_id")
        if not self._source_id_set or self._context.get_source_id() != source_id:
            if source_id:
                self._context.set_source_id(source_id)
                self._source_id_set = True

        if not self._axes_initialised:
            axes_json = d("axes")
            if not axes_json:
                return
            axes = json.loads(axes_json)

            dim = len(axes)
            if dim == 0:
                self._model = SubscriberSinglePointModel(
                    self._prefix, schema_revision, self._context
                )
            else:
                self._model = SubscriberScanModel(
                    axes, self._prefix, schema_revision, self._context
                )

            self._axes_initialised = True
            self.model_changed.emit(self._model)

        self._model.data_changed(values, mods)

    def get_model(self) -> Model | None:
        return self._model


class SubscriberSinglePointModel(SinglePointModel):
    def __init__(self, prefix: str, schema_revision: int, context: Context):
        super().__init__(schema_revision, context)
        self._prefix = prefix
        self._series_initialised = False
        self._channel_schemata = None
        self._current_point = None
        self._next_point = {}

    def get_channel_schemata(self) -> dict[str, Any] | None:
        return self._channel_schemata

    def get_point(self) -> dict[str, Any] | None:
        return self._current_point

    def data_changed(
        self, values: dict[str, Any], mods: Iterable[dict[str, Any]]
    ) -> None:
        # For single-point scans, points are completed as soon as point_phase flips, at
        # which point we need to emit them. There are slight subtleties in the below, in
        # that the initial sync can happen at any point through the first point (before/
        # after it has been completed) or even during the next point already.
        mods = list(mods)
        if mods and mods[0]["action"] == ModAction.init.value:
            # Squirrel away any number of already existing points. We need to do this
            # separately (rather than just using `values`), as there might be more mods
            # from a next point already in the pipeline (and applied to values).
            for key, (_, value, _) in mods[0]["struct"].items():
                name = strip_prefix(key, self._prefix + "point.")
                if name == key:
                    continue
                self._next_point[name] = value
            mods.pop(0)

        if not self._series_initialised:
            channels_json = values.get(self._prefix + "channels")
            if not channels_json:
                return
            self._channel_schemata = json.loads(channels_json)
            self._series_initialised = True
            self.channel_schemata_changed.emit(self._channel_schemata)

        def emit_point():
            self._current_point = self._next_point
            self._next_point = {}
            self.point_changed.emit(self._current_point)

        for m in mods:
            if m["action"] != ModAction.setitem.value:
                continue

            if m["key"] == self._prefix + "point_phase":
                emit_point()

            name = strip_prefix(m["key"], self._prefix + "point.")
            if name == m["key"]:
                continue
            self._next_point[name] = m["value"][1]

        if self._current_point is None and values.get(self._prefix + "completed"):
            # If the scan is already completed on the initial sync, we still need to
            # emit at least one point.
            #
            # The flag is also set when an experiment fails, though, so avoid producing
            # errors for missing data if no channel has been pushed (i.e. the point
            # failed cleanly).
            if self._next_point:
                emit_point()


class SubscriberScanModel(ScanModel):
    def __init__(
        self,
        axes: list[dict[str, Any]],
        prefix: str,
        schema_revision: int,
        context: Context,
    ):
        super().__init__(axes, schema_revision, context)
        self._prefix = prefix
        self._series_initialised = False
        self._online_analyses_initialised = False
        self._channel_schemata = None
        self._annotation_json = None
        self._annotations = []
        self._analysis_results_json = None
        self._analysis_result_sources = {}
        self._point_data = {}

    def data_changed(
        self, values: dict[str, Any], mods: Iterable[dict[str, Any]]
    ) -> None:
        if not self._series_initialised:
            channels_json = values.get(self._prefix + "channels")
            if not channels_json:
                return
            self._channel_schemata = json.loads(channels_json)
            self._series_initialised = True
            self.channel_schemata_changed.emit(self._channel_schemata)

        if not self._online_analyses_initialised:
            analyses_json = values.get(self._prefix + "online_analyses")
            if not analyses_json:
                return
            self._set_online_analyses(json.loads(analyses_json))
            self._online_analyses_initialised = True

        annotation_json = values.get(self._prefix + "annotations")
        if annotation_json != self._annotation_json:
            schemata = []
            if annotation_json is not None:
                schemata = json.loads(annotation_json)
            self._set_annotation_schemata(schemata)
            self._annotation_json = annotation_json

        analysis_results_json = values.get(self._prefix + "analysis_results")
        if analysis_results_json != self._analysis_results_json:
            if analysis_results_json is not None:
                for name in json.loads(analysis_results_json).keys():
                    # Make sure source exists.
                    self.get_analysis_result_source(name)
            self._analysis_results_json = analysis_results_json
        for name, source in self._analysis_result_sources.items():
            source.set(values.get(self._prefix + "analysis_result." + name))

        point_data_changed = False
        for name in [f"axis_{i}" for i in range(len(self.axes))] + [
            "channel_" + c for c in self._channel_schemata.keys()
        ]:
            point_values = values.get(self._prefix + "points." + name, [])
            if not point_data_changed:
                # Check if points were appended or rewritten.
                if name in self._point_data:
                    imax = min(len(point_values), len(self._point_data[name]))
                    if point_values[:imax] != self._point_data[name][:imax]:
                        point_data_changed = True
            self._point_data[name] = point_values
        if point_data_changed:
            self.points_rewritten.emit(self._point_data)
        else:
            self.points_appended.emit(self._point_data)

    def get_annotations(self) -> list[Annotation]:
        return self._annotations

    def get_channel_schemata(self) -> dict[str, Any] | None:
        return self._channel_schemata

    def get_point_data(self) -> dict[str, Any]:
        return self._point_data

    def get_analysis_result_source(self, name: str) -> FixedDataSource | None:
        if name not in self._analysis_result_sources:
            self._analysis_result_sources[name] = FixedDataSource(None)
        return self._analysis_result_sources[name]

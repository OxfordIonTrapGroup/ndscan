import json
from typing import Any, Dict, Iterable, List, Optional, Union
from sipyco.sync_struct import ModAction
from ...utils import strip_prefix
from . import Annotation, Context, Model, Root, ScanModel, SinglePointModel


class SubscriberRoot(Root):
    """Scan root fed from artiq.applets.simple data_changed callbacks, listening to the
    top-level ndscan dataset."""
    def __init__(self, prefix: str, context: Context):
        """
        :param prefix: Prefix of the ndscan dataset tree to represent, e.g.
            ``"ndscan."`` for the default location.
        """
        super().__init__()

        self._prefix = prefix
        self._context = context
        self._model = None

        # For root dataset sources, scan metadata doesn't change once it's been set.
        self._title_set = False
        self._source_id_set = False
        self._axes_initialised = False

    def data_changed(self, data: Dict[str, Any], mods: Iterable[Dict[str,
                                                                     Any]]) -> None:
        def d(name):
            return data.get(self._prefix + name, (False, None))[1]

        if not self._title_set:
            fqn = d("fragment_fqn")
            if fqn:
                self._context.set_title(fqn)
                self._title_set = True

        if not self._source_id_set:
            source_id = d("source_id")
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
                self._model = SubscriberSinglePointModel(self._prefix, self._context)
            else:
                self._model = SubscriberScanModel(axes, self._prefix, self._context)

            self._axes_initialised = True
            self.model_changed.emit(self._model)

        self._model.data_changed(data, mods)

    def get_model(self) -> Union[Model, None]:
        return self._model


class SubscriberSinglePointModel(SinglePointModel):
    def __init__(self, prefix: str, context: Context):
        super().__init__(context)
        self._prefix = prefix
        self._series_initialised = False
        self._channel_schemata = None
        self._current_point = None
        self._next_point = {}

    def get_channel_schemata(self) -> Dict[str, Any]:
        if self._channel_schemata is None:
            raise ValueError("No complete point yet")
        return self._channel_schemata

    def get_point(self) -> Optional[Dict[str, Any]]:
        if self._current_point is None:
            raise ValueError("No complete point yet")
        return self._current_point

    def data_changed(self, data: Dict[str, Any], mods: Iterable[Dict[str,
                                                                     Any]]) -> None:
        # For single-point scans, points are completed as soon as point_phase flips, at
        # which point we need to emit them. There are slight subtleties in the below, in
        # that the initial sync can happen at any point through the first/current point.
        mods = list(mods)
        if mods and mods[0]["action"] == ModAction.init.value:
            # Squirrel away any number of already existing points. We need to do this
            # separately (rather than just using `data`), as there might be more mods
            # from a next point already in the pipeline (and applied to data).
            for key, (_, value) in mods[0]["struct"].items():
                name = strip_prefix(key, self._prefix + "point.")
                if name == key:
                    continue
                self._next_point[name] = value
            mods.pop(0)

        if not self._series_initialised:
            channels_json = data.get(self._prefix + "channels", (False, None))[1]
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

        if (self._current_point is None and data.get(self._prefix + "completed",
                                                     (False, False))[1]):
            # If the scan is already completed on the initial sync, we still
            # need to emit at least one point.
            emit_point()


class SubscriberScanModel(ScanModel):
    def __init__(self, axes: List[Dict[str, Any]], prefix: str, context: Context):
        super().__init__(axes, context)
        self._prefix = prefix
        self._series_initialised = False
        self._online_analyses_initialised = False
        self._channel_schemata = None
        self._annotation_json = None
        self._annotations = []
        self._point_data = {}

    def data_changed(self, data: Dict[str, Any], mods: Iterable[Dict[str,
                                                                     Any]]) -> None:
        if not self._series_initialised:
            channels_json = data.get(self._prefix + "channels", (False, None))[1]
            if not channels_json:
                return
            self._channel_schemata = json.loads(channels_json)
            self._series_initialised = True
            self.channel_schemata_changed.emit(self._channel_schemata)

        if not self._online_analyses_initialised:
            analyses_json = data.get(self._prefix + "online_analyses", (False, None))[1]
            if not analyses_json:
                return
            self._set_online_analyses(json.loads(analyses_json))
            self._online_analyses_initialised = True

        annotation_json = data.get(self._prefix + "annotations", (False, None))[1]
        if annotation_json != self._annotation_json:
            self._set_annotation_schemata(json.loads(annotation_json))
            self._annotation_json = annotation_json

        for name in (["axis_{}".format(i) for i in range(len(self.axes))] +
                     ["channel_" + c for c in self._channel_schemata.keys()]):
            self._point_data[name] = data.get(self._prefix + "points." + name,
                                              (False, []))[1]

        self.points_appended.emit(self._point_data)

    def get_annotations(self) -> List[Annotation]:
        return self._annotations

    def get_channel_schemata(self) -> Dict[str, Any]:
        return self._channel_schemata

    def get_point_data(self) -> Dict[str, Any]:
        return self._point_data

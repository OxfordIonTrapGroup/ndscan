import json
import logging
from typing import Any
from ...utils import strip_suffix
from . import (FixedDataSource, Model, Root, ScanModel, SinglePointModel)

logger = logging.getLogger(__name__)


class SubscanRoot(Root):
    def __init__(self, parent: SinglePointModel, schema_key: str):
        super().__init__()
        self._parent = parent
        self._schema_key = schema_key
        self._model = None
        self._schema = None
        self._schema_str = None
        self._parent.point_changed.connect(self._update)

        self.name = strip_suffix(self._schema_key, "_spec")
        if self.name == self._schema_key:
            raise ValueError("Unexpected scan schema channel name: {}".format(
                self._schema_key))

        self._update(parent.get_point())

    def _update(self, data: dict[str, Any]) -> None:
        if data is None:
            if self._model:
                self._model.quit()
                self.model_changed.emit(None)
            self._model = None
            self._schema = None
            self._schema_str = None
            return

        schema_str = data[self._schema_key]
        if schema_str == self._schema_str:
            return
        self._schema_str = schema_str
        self._schema = json.loads(schema_str)

        self._model = SubscanModel(self._schema, self._parent, self.name + "_")
        self.model_changed.emit(self._model)

    def get_model(self) -> Model | None:
        return self._model


class SubscanModel(ScanModel):
    """A scan selected out of a single point with a subscan channel.

    Point content changes are forwarded, but the schema is static; changes to the latter
    necessitate a new model instance.
    """
    def __init__(self, schema: dict[str, Any], parent: SinglePointModel,
                 result_prefix: str):
        super().__init__(schema["axes"], parent.schema_revision, parent.context)

        self._channel_schemata = schema["channels"]

        self._result_prefix = result_prefix
        self._point_data = {}
        self._parent = parent
        self._parent.point_changed.connect(self._update)

        # Do not require analysis results to be present for backwards-compatibility.
        self._analysis_results = {}
        self._analysis_result_mappings = []
        for result_name, path in schema.get("analysis_results", {}).items():
            for (channel_name,
                 channel_schema) in self._parent.get_channel_schemata().items():
                if channel_schema["path"] == path:
                    source = FixedDataSource(None)
                    self._analysis_results[result_name] = source
                    self._analysis_result_mappings.append((result_name, channel_name))

        self._set_online_analyses(schema.get("online_analyses", {}))
        self._set_annotation_schemata(schema.get("annotations", []))
        self._update(parent.get_point())

    def quit(self) -> None:
        self._parent.point_changed.disconnect(self._update)

    def _update(self, parent_data: dict[str, Any] | None) -> None:
        if parent_data is None:
            logger.debug("Ignoring update")
            return

        for name in ([f"axis_{i}" for i in range(len(self.axes))] +
                     ["channel_" + c for c in self._channel_schemata.keys()]):
            self._point_data[name] = parent_data[self._result_prefix + name]
        self.points_rewritten.emit(self._point_data)

        for r, c in self._analysis_result_mappings:
            self._analysis_results[r].set(parent_data[c])

    def get_channel_schemata(self) -> dict[str, Any]:
        return self._channel_schemata

    def get_point_data(self) -> dict[str, Any]:
        return self._point_data

    def get_analysis_result_source(self, name: str) -> FixedDataSource | None:
        return self._analysis_results.get(name, None)


def create_subscan_roots(model: SinglePointModel) -> dict[str, SubscanRoot]:
    schemata = model.get_channel_schemata()
    if schemata is None:
        return {}
    result = {}
    for key, schema in schemata.items():
        if schema["type"] != "subscan":
            continue
        root = SubscanRoot(model, key)
        result[root.name] = root
    return result

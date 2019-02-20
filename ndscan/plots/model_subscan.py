import json
from typing import Any, Dict
from PyQt5 import QtCore
from .model import *


class SubscanRoot(Root):
    def __init__(self, parent: SinglePointModel, schema_key: str):
        super().__init__()
        self._parent = parent
        self._schema_key = schema_key
        self._model = None
        self._schema = None
        self._schema_str = None
        self._parent.point_changed.connect(lambda data: self._update_schema(data[
            schema_key]))

    def _update_schema(self, schema_str: str) -> None:
        if schema_str == self._schema_str:
            return
        self._schema = json.loads(schema_str)

        SUFFIX = "spec"
        if self._schema_key[-len(SUFFIX):] != SUFFIX:
            raise ValueError("Unexpected scan schema channel name: {}".format(
                self._schema_key))
        self._model = SubscanModel(self._schema, self._parent,
                                   self._schema_key[:-len(SUFFIX)])
        self.model_changed.emit(self._model)

    def get_model(self) -> Model:
        if self._model is None:
            raise ValueError("Model not yet set")
        return self._model


class SubscanModel(ScanModel):
    def __init__(self, schema: List[Dict[str, Any]], parent: SinglePointModel,
                 result_prefix: str):
        super().__init__(schema["axes"], parent.context)

        self._channel_schemata = schema["channels"]
        self._result_prefix = result_prefix
        self._point_data = {}
        parent.point_changed.connect(self._update)

        _call_later(lambda: self.channel_schemata_changed.emit(self._channel_schemata))
        _call_later(lambda: self._update(parent.get_point()))

    def _update(self, parent_data: Dict[str, Any]) -> None:
        for name in (["axis_{}".format(i) for i in range(len(self.axes))] +
                     ["channel_" + c for c in self._channel_schemata.keys()]):
            self._point_data[name] = parent_data[self._result_prefix + name]

        self.points_rewritten.emit(self._point_data)

    def get_channel_schemata(self) -> Dict[str, Any]:
        return self._channel_schemata

    def get_point_data(self) -> Dict[str, Any]:
        return self._point_data


def _call_later(func):
    QtCore.QTimer.singleShot(0, func)

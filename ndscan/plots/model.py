import json
from typing import Any, Callable, Dict, Union
from quamash import QtCore
from ..utils import strip_prefix


class Context(QtCore.QObject):
    title_changed = QtCore.pyqtSignal(str)

    def __init__(self, set_dataset: Callable[[str, Any], None] = None):
        super().__init__()
        self.set_dataset = set_dataset
        self.title = ""

    def set_title(self, title: str) -> None:
        if title != self.title:
            self.title = title
            self.title_changed.emit(title)

    def is_online_master(self) -> bool:
        return self.set_dataset is not None

    def set_dataset(self, key: str, value: Any) -> None:
        self.set_dataset(key, value)


class ContinuousScanModel(QtCore.QObject):
    channel_schemata_changed = QtCore.pyqtSignal(dict)
    new_point_complete = QtCore.pyqtSignal(dict)

    def __init__(self, context: Context):
        super().__init__()
        self.context = context

    def get_channel_schemata(self) -> Dict[str, Any]:
        raise NotImplementedError

    def get_current_point(self) -> Dict[str, Any]:
        raise NotImplementedError


class DimensionalScanModel(QtCore.QObject):
    channel_schemata_changed = QtCore.pyqtSignal(dict)
    points_rewritten = QtCore.pyqtSignal(dict)
    points_appended = QtCore.pyqtSignal(dict)

    def __init__(self, axes: list, context: Context):
        super().__init__()
        self.axes = axes
        self.context = context


class Root(QtCore.QObject):
    model_changed = QtCore.pyqtSignal()

    def get_model(self) -> Union[ContinuousScanModel, DimensionalScanModel]:
        raise NotImplementedError


class SubscriberRoot(Root):
    """Scan root fed from artiq.applets.simple data_changed callbacks, listening to the
    top-level ndscan dataset."""

    def __init__(self, context: Context):
        super().__init__()

        self._context = context
        self._model = None

        # For root dataset sources, scan metadata doesn't change once it's been set.
        self._title_set = False
        self._axes_initialised = False

    def data_changed(self, data, mods):
        def d(name):
            return data.get("ndscan." + name, (False, None))[1]

        if not self._title_set:
            fqn = d("fragment_fqn")
            if fqn:
                self._context.set_title(fqn)
                self._title_set = True

        if not self._axes_initialised:
            axes_json = d("axes")
            if not axes_json:
                return
            axes = json.loads(axes_json)

            dim = len(axes)
            if dim == 0:
                self._model = SubscriberContinuousScanModel(self._context)
            else:
                self._model = SubscriberDimensionalScanModel(axes, self._context)

            self._axes_initialised = True
            self.model_changed.emit()

        self._model.data_changed(data, mods)

    def get_model(self) -> Union[ContinuousScanModel, DimensionalScanModel]:
        if self._model is None:
            raise ValueError("Model not yet set")
        return self._model


class SubscriberContinuousScanModel(ContinuousScanModel):
    def __init__(self, context: Context):
        super().__init__(context)
        self._series_initialised = False
        self._channel_schemata = None
        self._current_point = None
        self._next_point = {}

    def get_channel_schemata(self) -> Dict[str, Any]:
        if self._channel_schemata is None:
            raise ValueError("No complete point yet")
        return self._channel_schemata

    def get_current_point(self) -> Dict[str, Any]:
        if self._current_point is None:
            raise ValueError("No complete point yet")
        return self._current_point

    def data_changed(self, data, mods):
        if not self._series_initialised:
            channels_json = data.get("ndscan.channels", (False, None))[1]
            if not channels_json:
                return
            self._channel_schemata = json.loads(channels_json)
            self._series_initialised = True
            self.channel_schemata_changed.emit(self._channel_schemata)

        for m in mods:
            if m["action"] != "setitem":
                continue
            key = strip_prefix(m["key"], "ndscan.point.")
            if key == m["key"]:
                continue
            if key in self._channel_schemata:
                self._next_point[key] = m["value"][1]

        if len(self._next_point) == len(self._channel_schemata):
            self._current_point = self._next_point
            self._next_point = {}
            self.new_point_complete.emit(self._current_point)


class SubscriberDimensionalScanModel(DimensionalScanModel):
    def __init__(self, axes: list, context: Context):
        super().__init__(axes, context)
        self._series_initialised = False
        self._channel_schemata = None
        self._point_data = {}

    def data_changed(self, data, mods):
        if not self._series_initialised:
            channels_json = data.get("ndscan.channels", (False, None))[1]
            if not channels_json:
                return
            self._channel_schemata = json.loads(channels_json)
            self._series_initialised = True
            self.channel_schemata_changed.emit(self._channel_schemata)

        for name in (["axis_{}".format(i) for i in range(len(self.axes))] +
                     ["channel_" + c for c in self._channel_schemata.keys()]):
            self._point_data[name] = data.get("ndscan.points." + name, (False, []))[1]

        self.points_appended.emit(self._point_data)

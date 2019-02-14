from artiq.language import *
import artiq.language.units
from typing import Any, Dict, List


class ResultSink:
    def push(self, value):
        raise NotImplementedError


class ArraySink(ResultSink):
    def __init__(self):
        self.data = []

    def push(self, value):
        self.data.append(value)

    def get_all(self):
        return self.data

    def clear(self):
        self.data = []


class AppendingDatasetSink(ResultSink, HasEnvironment):
    def build(self, key, broadcast=True):
        self.key = key
        self.broadcast = broadcast
        self.has_pushed = False

    def push(self, value):
        if not self.has_pushed:
            self.set_dataset(self.key, [value], broadcast=self.broadcast)
            self.has_pushed = True
            return
        self.append_to_dataset(self.key, value)

    def get_all(self):
        return self.get_dataset(self.key) if self.has_pushed else []


class ScalarDatasetSink(ResultSink, HasEnvironment):
    def build(self, key, broadcast=True):
        self.key = key
        self.broadcast = broadcast
        self.has_pushed = False

    def push(self, value):
        self.set_dataset(self.key, value, broadcast=self.broadcast)
        self.has_pushed = True

    def get_last(self):
        return self.get_dataset(self.key) if self.has_pushed else None


class ResultChannel:
    def __init__(self,
                 path: List[str],
                 description: str = "",
                 display_hints: Dict[str, Any] = {},
                 save_by_default: bool = True):
        self.path = path
        self.description = description
        self.display_hints = display_hints
        self.save_by_default = save_by_default
        self.sink = None

    def describe(self) -> Dict[str, Any]:
        desc = {
            "path": self.path,
            "description": self.description,
            "type": self._get_type_string()
        }

        if self.display_hints:
            desc["display_hints"] = self.display_hints
        return desc

    def is_muted(self) -> bool:
        # TODO: Implement muting interface?
        return self.sink is not None

    def set_sink(self, sink: ResultSink):
        self.sink = sink

    @rpc(flags={"async"})
    def push(self, raw_value):
        value = self._coerce_to_type(raw_value)
        if self.sink:
            self.sink.push(value)

    def _get_type_string(self):
        raise NotImplementedError()

    def _coerce_to_type(self, value):
        raise NotImplementedError()


class NumericChannel(ResultChannel):
    def __init__(self,
                 path: List[str],
                 description: str = "",
                 display_hints: Dict[str, Any] = {},
                 min=None,
                 max=None,
                 unit: str = "",
                 scale=None):
        super().__init__(path, description, display_hints)
        self.min = min
        self.max = max

        if scale is None:
            if unit == "":
                scale = 1.0
            else:
                try:
                    scale = getattr(artiq.language.units, unit)
                except AttributeError:
                    raise KeyError("Unit {} is unknown, you must specify "
                                   "the scale manually".format(unit))
        self.scale = scale
        self.unit = unit

    def describe(self) -> Dict[str, Any]:
        result = super().describe()
        result["scale"] = self.scale
        if self.min is not None:
            result["min"] = self.min
        if self.max is not None:
            result["max"] = self.max
        if self.unit is not None:
            result["unit"] = self.unit
        return result


class FloatChannel(NumericChannel):
    def _get_type_string(self):
        return "float"

    def _coerce_to_type(self, value):
        return float(value)


class IntChannel(NumericChannel):
    def _get_type_string(self):
        return "int"

    def _coerce_to_type(self, value):
        return int(value)


class OpaqueChannel(ResultChannel):
    def _get_type_string(self):
        return "opaque"

    def _coerce_to_type(self, value):
        # Just pass through values, leaving it to the user to choose something
        # HD5- and PYON-compatible.
        return value

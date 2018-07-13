from artiq.language import *
from typing import Any, Callable, Dict, List, Type


class ResultSink:
    def push(self, value):
        raise NotImplementedError()

    def get_all(self):
        raise NotImplementedError()


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

    def get_all(self):
        return [self.get_dataset(self.key)] if self.has_pushed else []


class ResultChannel:
    def __init__(self, path: List[str], description: str = "", display_hints: Dict[str, Any] = {}):
        self.path = path
        self.description = description
        self.display_hints = display_hints
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
        # TODO: Implement muting interface.
        return False

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
    def __init__(self, path: List[str], description: str = "", display_hints: Dict[str, Any] = {}, min = None, max = None):
        super().__init__(path, description, display_hints)
        self.min = min
        self.max = max

    def describe(self) -> Dict[str, Any]:
        result = super().describe()
        if self.min is not None:
            result["min"] = self.min
        if self.max is not None:
            result["max"] = self.max
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
        # Just pass through values, leaving it to the user to choose something HD5- and PYON-compatible.
        return value

import logging

from artiq.language import *
from artiq.protocols import pyon
from collections import OrderedDict
from contextlib import suppress
from typing import Any, Callable, Dict, List, Type, TypeVar, Union
from .utils import path_matches_spec, strip_prefix

logger = logging.getLogger(__name__)


class ParamStore:
    def __init__(self, value):
        self._value = value
        self._change_callbacks = set()

    def register_change_callback(self, cb):
        self._change_callbacks.add(cb)

    def unregister_change_callback(self, cb):
        self._change_callbacks.remove(cb)

    def get_value(self):
        return self._value

    def set_value(self, value):
        self._value = value
        for cb in self._change_callbacks:
            cb()


class ParamHandle:
    def __init__(self):
        self._store = None
        self._changed_after_use = True

    def set_store(self, store) -> None:
        if self._store:
            self._store.unregister_change_callback(self._change_cb)
        self._store = store
        self._changed_after_use = True

    def get(self):
        return self._store.get_value()

    def use(self):
        self._changed_after_use = False
        return self._store.get_value()

    def changed_after_use(self) -> bool:
        return self._changed_after_use

    def _change_cb(self):
        # Once transform lambdas are supported, handle them here.
        self._changed_after_use = True


class FloatParam:
    def __init__(self, fqn: str, description: str, default: Union[str, float]):
        self.fqn = fqn
        self.description = description
        self.default = default

    def describe(self) -> Dict[str, any]:
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "float",
            "default": self.default
        }

    def apply_default(self, target: ParamHandle, get_dataset: Callable) -> None:
        if type(self.default) is str:
            value = float(_eval_default(self.default, get_dataset))
        else:
            value = self.default
        target.set_store(ParamStore(value))


class IntParam:
    def __init__(self, fqn: str, description: str, default: Union[str, int]):
        self.fqn = fqn
        self.description = description
        self.default = default

    def describe(self) -> Dict[str, any]:
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "int",
            "default": self.default
        }

    def apply_default(self, target: ParamHandle, get_dataset: Callable) -> None:
        if type(self.default) is str:
            value = int(_eval_default(self.default, get_dataset))
        else:
            value = self.default
        target.set_store(ParamStore(value))


def _eval_default(value: str, get_dataset: Callable):
    return eval(value, {"dataset": get_dataset})


class ResultChannel:
    def __init__(self, path: List[str], description: str = "", display_hints: Dict[str, any] = {}):
        self.path = path
        self.description = description
        self.display_hints = display_hints
        self.result_callback = None

    def describe(self) -> Dict[str, any]:
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

    def set_result_callback(self, cb: Callable):
        self.result_callback = cb

    @rpc(flags={"async"})
    def set(self, raw_value):
        value = self._coerce_to_type(raw_value)
        if self.result_callback:
            self.result_callback(value)

    def _get_type_string(self):
        raise NotImplementedError()

    def _coerce_to_type(self, value):
        raise NotImplementedError()


class NumericChannel(ResultChannel):
    def __init__(self, path: List[str], description: str = "", display_hints: Dict[str, any] = {}, min = None, max = None):
        super().__init__(path, description, display_hints)
        self.min = min
        self.max = max

    def describe(self) -> Dict[str, any]:
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


class Fragment(HasEnvironment):
    def build(self, fragment_path: List[str]):
        self._fragment_path = fragment_path
        self._subfragments = []
        self._free_params = OrderedDict()
        self._result_channels = {}

        klass = self.__class__
        mod = klass.__module__
        # KLUDGE: Strip prefix added by file_import() to make path matches compatible across
        # dashboard/artiq_run and the worker running the experiment. Should be fixed at the source.
        for f in ["artiq_run_", "artiq_worker_", "file_import_"]:
            mod = strip_prefix(mod, f)
        self.fqn = mod + "." + klass.__qualname__

        self._building = True
        self.build_fragment()
        self._building = False

    def device_setup(self) -> None:
        pass

    def device_reset(self, changes: list) -> None:
        # By default, just completely reinitialize.
        self.device_setup()

    def build_fragment(self) -> None:
        raise NotImplementedError("build_fragment() not implemented; add parameters/result channels here.")

    def setattr_fragment(self, name: str, fragment_class: Type["Fragment"]) -> None:
        assert self._building, "Can only call setattr_fragment() during build_fragment()"
        assert name.isidentifier(), "Subfragment name must be valid Python identifier"
        assert not hasattr(self, name), "Field '%s' already exists".format(name)

        frag = fragment_class(self, self._fragment_path + [name])
        self._subfragments.append(frag)
        setattr(self, name, frag)

    def setattr_param(self, name: str, param_class: Type, description: str, *args, **kwargs) -> None:
        assert self._building, "Can only call setattr_param() during build_fragment()"
        assert name.isidentifier(), "Parameter name must be valid Python identifier"
        assert not hasattr(self, name), "Field '%s' already exists".format(name)

        fqn = self.fqn + "." + name
        self._free_params[name] = param_class(fqn, description, *args, **kwargs)
        setattr(self, name, ParamHandle())

    def setattr_result(self, name: str, channel_class: Type = FloatChannel, *args, **kwargs) -> None:
        assert self._building, "Can only call setattr_result() during build_fragment()"
        assert name.isidentifier(), "Result channel name must be valid Python identifier"
        assert not hasattr(self, name), "Field '%s' already exists".format(name)

        path = "/".join(self._fragment_path + [name])
        channel = channel_class(path, *args, **kwargs)
        self._result_channels[path] = channel
        setattr(self, name, channel)

    def _build_param_tree(self, params: Dict[str, List[str]], schemata: Dict[str, dict]) -> None:
        path = "/".join(self._fragment_path)

        fqns = []
        for param in self._free_params.values():
            fqn = param.fqn
            schema = param.describe()
            if fqn in schemata:
                if schemata[fqn] != schema:
                    logger.warn("Mismatch in parameter schema '%s' for '%s'", fqn, path)
            else:
                schemata[fqn] = schema
            fqns.append(fqn)
        params[path] = fqns

        for s in self._subfragments:
            s._build_param_tree(params, schemata)

    def _apply_param_overrides(self, overrides: Dict[str, List[dict]]) -> None:
        for name, param in self._free_params.items():
            was_set = False
            for o in overrides.get(param.fqn, []):
                if path_matches_spec(self._fragment_path, o["path"]):
                    getattr(self, name).set_store(o["store"])
                    was_set = True
            if not was_set:
                param.apply_default(getattr(self, name), self.get_dataset)

        for s in self._subfragments:
            s._apply_param_overrides(overrides)

    def _get_always_shown_params(self) -> List[str]:
        return [(p.fqn, "/".join(self._fragment_path)) for p in self._free_params.values()]

    def _collect_result_channels(self, channels: dict):
        channels.update(self._result_channels)
        for s in self._subfragments:
            s._collect_result_channels(channels)


class ExpFragment(Fragment):
    def host_setup(self):
        """Called before kernel is entered for the first time.

        TODO: Semantics for multiple invocations.
        """
        pass

    def run_once(self):
        pass

    def analyze(self):
        pass

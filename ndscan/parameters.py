from artiq.language import *
from artiq.language import units
from typing import Callable, Dict, Tuple, Union
from .utils import eval_param_default
"""
Fragment-side parameter containers.

The ARTIQ compiler does not support templates or generics (neither in the sense
of typing.Generic, nor any other), yet requires the inferred types/signatures
of fields to match across all instances of a class. Hence, we have no option
but to hang our heads in shame and manually instantiate the parameter handling
machinery for all the value types.
"""


def type_string_to_param(name: str):
    """Resolves param schema type strings to *Param implementations."""
    return {"float": FloatParam, "int": IntParam, "string": StringParam}[name]


class InvalidDefaultError(ValueError):
    pass


class ParamStore:
    def __init__(self, identity: Tuple[str, str], value):
        self.identity = identity
        """(fqn, path_spec) pair representing the identity of this param store,
        i.e. the override/default value it was created for.
        """

        # KLUDGE: Work around type inference failing for empty lists.
        self._handles = []
        self._notify = self._do_nothing

        self.set_value(value)

    @host_only
    def register_handle(self, handle):
        self._handles.append(handle)
        self._notify = self._notify_handles

    @host_only
    def unregister_handle(self, handle):
        self._handles.remove(handle)

        if not self._handles:
            self._notify = self._do_nothing

    @portable
    def _notify_handles(self):
        for h in self._handles:
            h._changed_after_use = True

    @portable
    def _do_nothing(self):
        pass


class FloatParamStore(ParamStore):
    @portable
    def get_value(self) -> TFloat:
        return self._value

    @portable
    def set_value(self, value):
        self._value = self.coerce(value)
        self._notify()

    @portable
    def coerce(self, value):
        return float(value)


class IntParamStore(ParamStore):
    @portable
    def get_value(self) -> TInt32:
        return self._value

    @portable
    def set_value(self, value):
        self._value = self.coerce(value)
        self._notify()

    @portable
    def coerce(self, value):
        return int(value)


class StringParamStore(ParamStore):
    @portable
    def get_value(self) -> TStr:
        return self._value

    @portable
    def set_value(self, value):
        self._value = self.coerce(value)
        self._notify()

    @portable
    def coerce(self, value):
        return str(value)


class ParamHandle:
    def __init__(self):
        self._store = None
        self._changed_after_use = True

    def set_store(self, store) -> None:
        if self._store:
            self._store.unregister_handle(self)
        store.register_handle(self)
        self._store = store
        self._changed_after_use = True

    @portable
    def _change_cb(self):
        # Once transform lambdas are supported, handle them here.
        self._changed_after_use = True

    @portable
    def changed_after_use(self) -> TBool:
        return self._changed_after_use


class FloatParamHandle(ParamHandle):
    @portable
    def get(self) -> TFloat:
        return self._store.get_value()

    @portable
    def use(self) -> TFloat:
        self._changed_after_use = False
        return self._store.get_value()


class IntParamHandle(ParamHandle):
    @portable
    def get(self) -> TInt32:
        return self._store.get_value()

    @portable
    def use(self) -> TInt32:
        self._changed_after_use = False
        return self._store.get_value()


class StringParamHandle(ParamHandle):
    @portable
    def get(self) -> TStr:
        return self._store.get_value()

    @portable
    def use(self) -> TStr:
        self._changed_after_use = False
        return self._store.get_value()


class FloatParam:
    HandleType = FloatParamHandle
    StoreType = FloatParamStore
    CompilerType = TFloat

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: Union[str, float],
                 min: Union[float, None] = None,
                 max: Union[float, None] = None,
                 unit: str = "",
                 scale: Union[float, None] = None,
                 step: Union[float, None] = None):

        self.fqn = fqn
        self.description = description
        self.default = default
        self.min = min
        self.max = max

        if scale is None:
            if unit == "":
                scale = 1.0
            else:
                try:
                    scale = getattr(units, unit)
                except AttributeError:
                    raise KeyError("Unit {} is unknown, you must specify "
                                   "the scale manually".format(unit))
        self.scale = scale
        self.unit = unit

        self.step = step if step is not None else scale / 10.0

    def describe(self) -> Dict[str, any]:
        spec = {"scale": self.scale, "step": self.step}
        if self.min is not None:
            spec["min"] = self.min
        if self.max is not None:
            spec["max"] = self.max
        if self.unit:
            spec["unit"] = self.unit

        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "float",
            "default": str(self.default),
            "spec": spec
        }

    def default_store(self, identity: Tuple[str, str],
                      get_dataset: Callable) -> FloatParamStore:
        if type(self.default) is str:
            value = eval_param_default(self.default, get_dataset)
        else:
            value = self.default
        if self.min is not None and value < self.min:
            raise InvalidDefaultError("Value {} below minimum of {}".format(
                value, self.min))
        if self.max is not None and value > self.max:
            raise InvalidDefaultError("Value {} above maximum of {}".format(
                value, self.max))
        return FloatParamStore(identity, value)


class IntParam:
    HandleType = IntParamHandle
    StoreType = IntParamStore
    CompilerType = TInt32

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: Union[str, int],
                 min=0,
                 unit: str = "",
                 scale=None):
        self.fqn = fqn
        self.description = description
        self.default = default
        self.min = min

        if scale is None:
            if unit == "":
                scale = 1
            else:
                try:
                    scale = getattr(units, unit)
                except AttributeError:
                    raise KeyError("Unit {} is unknown, you must specify "
                                   "the scale manually".format(unit))
        if scale != 1:
            raise NotImplementedError(
                "Non-unity scales not implemented for integer parameters")

    def describe(self) -> Dict[str, any]:
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "int",
            "default": str(self.default),
            "spec": {
                "scale": 1
            }
        }

    def default_store(self, identity: Tuple[str, str],
                      get_dataset: Callable) -> IntParamStore:
        if type(self.default) is str:
            value = eval_param_default(self.default, get_dataset)
        else:
            value = self.default
        return IntParamStore(identity, value)


class StringParam:
    HandleType = StringParamHandle
    StoreType = StringParamStore
    CompilerType = TStr

    def __init__(self, fqn: str, description: str, default: str):
        self.fqn = fqn
        self.description = description
        self.default = default

    def describe(self) -> Dict[str, any]:
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "string",
            "default": str(self.default)
        }

    def default_store(self, identity: Tuple[str, str],
                      get_dataset: Callable) -> StringParamStore:
        default = eval_param_default(self.default, get_dataset)
        return StringParamStore(identity, default)

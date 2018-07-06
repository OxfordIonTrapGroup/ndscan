from artiq.language import *
from artiq.language import units
from typing import Callable, Dict, Union

"""
Fragment-side parameter containers.

The ARTIQ compiler does not support templates or generics (neither in the sense
of typing.Generic, nor any other), yet requires the inferred types/signatures
of fields to match across all instances of a class. Hence, we have no option
but to hang our heads in shame and manually instantiate the parameter handling
machinery for all the value types.

(Common code could possibly be cleaned up slightly using inheritance, but at
this point, why bother?)
"""


def type_string_to_param(name: str):
    """Resolves param schema type strings to *Param implementations."""
    return {
        "float": FloatParam,
        "int": IntParam
    }[name]


class InvalidDefaultError(ValueError):
    pass


class FloatParamStore:
    def __init__(self, value):
        self._change_callbacks = [self._do_nothing] # set is not iterable on kernel
        self.set_value(value)

    def register_change_callback(self, cb):
        self._change_callbacks.append(cb)

    def unregister_change_callback(self, cb):
        self._change_callbacks.remove(cb)

    @portable
    def get_value(self) -> TFloat:
        return self._value

    @portable
    def set_value(self, value):
        self._value = float(value)
        for cb in self._change_callbacks:
            cb()

    @portable
    def _do_nothing(self):
        pass


class IntParamStore:
    def __init__(self, value):
        self._change_callbacks = [self._do_nothing] # set is not iterable on kernel
        self.set_value(value)

    def register_change_callback(self, cb):
        self._change_callbacks.append(cb)

    def unregister_change_callback(self, cb):
        self._change_callbacks.remove(cb)

    @portable
    def get_value(self) -> TInt32:
        return self._value

    @portable
    def set_value(self, value):
        self._value = int(value)
        for cb in self._change_callbacks:
            cb()

    @portable
    def _do_nothing(self):
        pass


class FloatParamHandle:
    def __init__(self):
        self._store = None
        self._changed_after_use = True

    def set_store(self, store: FloatParamStore) -> None:
        if self._store:
            self._store.unregister_change_callback(self._change_cb)
        self._store = store
        self._changed_after_use = True

    @portable
    def get(self) -> TFloat:
        return self._store.get_value()

    @portable
    def use(self) -> TFloat:
        self._changed_after_use = False
        return self._store.get_value()

    @portable
    def changed_after_use(self) -> TBool:
        return self._changed_after_use

    def _change_cb(self):
        # Once transform lambdas are supported, handle them here.
        self._changed_after_use = True


class IntParamHandle:
    def __init__(self):
        self._store = None
        self._changed_after_use = True

    def set_store(self, store: IntParamStore) -> None:
        if self._store:
            self._store.unregister_change_callback(self._change_cb)
        self._store = store
        self._changed_after_use = True

    @portable
    def get(self) -> TInt32:
        return self._store.get_value()

    @portable
    def use(self) -> TInt32:
        self._changed_after_use = False
        return self._store.get_value()

    @portable
    def changed_after_use(self) -> TBool:
        return self._changed_after_use

    def _change_cb(self):
        # Once transform lambdas are supported, handle them here.
        self._changed_after_use = True


class FloatParam:
    HandleType = FloatParamHandle
    StoreType = FloatParamStore
    CompilerType = TFloat

    def __init__(self, fqn: str, description: str, default: Union[str, float],
        min: Union[float, None] = None, max: Union[float, None] = None,
        unit: str = "", scale: Union[float, None] = None,
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
            "default": self.default,
            "spec": spec
        }

    def apply_default(self, target: FloatParamHandle, get_dataset: Callable) -> None:
        if type(self.default) is str:
            value = _eval_default(self.default, get_dataset)
        else:
            value = self.default
        if self.min is not None and value < self.min:
            raise InvalidDefaultError("Value {} below minimum of {}".format(value, self.min))
        if self.max is not None and value > self.max:
            raise InvalidDefaultError("Value {} above maximum of {}".format(value, self.max))
        target.set_store(FloatParamStore(value))


class IntParam:
    HandleType = IntParamHandle
    StoreType = IntParamStore
    CompilerType = TInt32

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

    def apply_default(self, target: IntParamHandle, get_dataset: Callable) -> None:
        if type(self.default) is str:
            value = _eval_default(self.default, get_dataset)
        else:
            value = self.default
        target.set_store(IntParamStore(value))


def _eval_default(value: str, get_dataset: Callable):
    return eval(value, {"dataset": get_dataset})

"""Fragment-side parameter containers.
"""

# The ARTIQ compiler does not support templates or generics (neither in the sense
# of typing.Generic, nor any other), yet requires the inferred types/signatures
# of fields to match across all instances of a class. Hence, we have no option
# but to hang our heads in shame and manually instantiate the parameter handling
# machinery for all supported value types, in particular to handle cases where e.g.
# both an int and a float parameter is scanned at the same time.

from artiq.language import host_only, portable, units
from collections import OrderedDict
from enum import Enum
from numpy import int32
from typing import Any
from ..utils import eval_param_default, GetDataset

__all__ = ["FloatParam", "IntParam", "StringParam", "BoolParam", "enum_param_factory"]

#: Maps type string to Param implementation. EnumParams are dynamically added to this
#: collection.
_type_string_to_param = {}


def type_string_to_param(name: str):
    """Resolve a param schema type string to the corresponding Param implementation."""
    return _type_string_to_param[name]


class InvalidDefaultError(ValueError):
    """Raised when a default value is outside the specified range of valid parameter
    values."""


class ParamStore:
    """
    :param identity: ``(fqn, path_spec)`` pair representing the identity of this param
        store, i.e. the override/default value it was created for.
    :param value: The initial value.
    """
    def __init__(self, identity: tuple[str, str], value):
        self.identity = identity

        # KLUDGE: To work around ARTIQ compiler type inference failing for empty lists,
        # we rebind the function to notify parameter handles of changes if there are
        # none registered.
        self._handles = []
        self._notify = self._do_nothing

        self._value = self.coerce(value)

    @host_only
    def register_handle(self, handle):
        self._handles.append(handle)
        self._notify = self._notify_handles

    @host_only
    def unregister_handle(self, handle):
        self._handles.remove(handle)

        if not self._handles:
            self._notify = self._do_nothing


class FloatParamStore(ParamStore):
    @portable
    def _notify_handles(self):
        for h in self._handles:
            h._changed_after_use = True

    @portable
    def _do_nothing(self):
        pass

    @portable
    def get_value(self) -> float:
        return self._value

    @portable
    def set_value(self, value):
        new_value = self.coerce(value)
        if new_value == self._value:
            return
        self._value = new_value
        self._notify()

    @portable
    def coerce(self, value):
        return float(value)


class IntParamStore(ParamStore):
    @portable
    def _notify_handles(self):
        for h in self._handles:
            h._changed_after_use = True

    @portable
    def _do_nothing(self):
        pass

    @portable
    def get_value(self) -> int32:
        return self._value

    @portable
    def set_value(self, value):
        new_value = self.coerce(value)
        if new_value == self._value:
            return
        self._value = new_value
        self._notify()

    @portable
    def coerce(self, value):
        return int(value)


class StringParamStore(ParamStore):
    @portable
    def _notify_handles(self):
        for h in self._handles:
            h._changed_after_use = True

    @portable
    def _do_nothing(self):
        pass

    @portable
    def get_value(self) -> str:
        return self._value

    @portable
    def set_value(self, value):
        new_value = self.coerce(value)
        if new_value == self._value:
            return
        self._value = new_value
        self._notify()

    @portable
    def coerce(self, value):
        return str(value)


class BoolParamStore(ParamStore):
    @portable
    def _notify_handles(self):
        for h in self._handles:
            h._changed_after_use = True

    @portable
    def _do_nothing(self):
        pass

    @portable
    def get_value(self) -> bool:
        return self._value

    @portable
    def set_value(self, value):
        new_value = self.coerce(value)
        if new_value == self._value:
            return
        self._value = new_value
        self._notify()

    @portable
    def coerce(self, value):
        return bool(value)


class ParamHandle:
    """
    Each instance of this class corresponds to exactly one attribute of a fragment that
    can be used to access the underlying parameter store.

    :param owner: The owning fragment.
    :param name: The name of the attribute in the owning fragment bound to this
        object.
    """
    def __init__(self, owner: Any, name: str):
        # `owner` will typically be a Fragment instance; no type hint to avoid circular
        # dependency.
        self.owner = owner
        self.name = name
        assert name.isidentifier(), ("ParamHandle name should be the identifier it is "
                                     "referred to as in the owning fragment.")

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
    def changed_after_use(self) -> bool:
        return self._changed_after_use


class FloatParamHandle(ParamHandle):
    @portable
    def get(self) -> float:
        return self._store.get_value()

    @portable
    def use(self) -> float:
        self._changed_after_use = False
        return self._store.get_value()


class IntParamHandle(ParamHandle):
    @portable
    def get(self) -> int32:
        return self._store.get_value()

    @portable
    def use(self) -> int32:
        self._changed_after_use = False
        return self._store.get_value()


class StringParamHandle(ParamHandle):
    @portable
    def get(self) -> str:
        return self._store.get_value()

    @portable
    def use(self) -> str:
        self._changed_after_use = False
        return self._store.get_value()


class BoolParamHandle(ParamHandle):
    @portable
    def get(self) -> bool:
        return self._store.get_value()

    @portable
    def use(self) -> bool:
        self._changed_after_use = False
        return self._store.get_value()


def resolve_numeric_scale(scale: float | None, unit: str) -> float:
    if scale is not None:
        return scale
    if unit == "":
        return 1
    try:
        return getattr(units, unit)
    except AttributeError:
        raise KeyError("Unit '{}' is unknown, you must specify "
                       "the scale manually".format(unit))


class ParamBase:
    def __init__(self, **kwargs):
        # Store kwargs for param rebinding
        self.init_params = kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)


class FloatParam(ParamBase):
    HandleType = FloatParamHandle
    StoreType = FloatParamStore
    CompilerType = float

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: str | float,
                 *,
                 min: float | None = None,
                 max: float | None = None,
                 unit: str = "",
                 scale: float | None = None,
                 step: float | None = None,
                 is_scannable: bool = True):

        ParamBase.__init__(self,
                           fqn=fqn,
                           description=description,
                           default=default,
                           min=min,
                           max=max,
                           unit=unit,
                           scale=scale,
                           step=step,
                           is_scannable=is_scannable)
        self.scale = resolve_numeric_scale(scale, unit)
        self.step = step if step is not None else self.scale / 10.0

    def describe(self) -> dict[str, Any]:
        spec = {
            "is_scannable": self.is_scannable,
            "scale": self.scale,
            "step": self.step
        }
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
            "spec": spec,
        }

    def eval_default(self, get_dataset: GetDataset) -> float:
        if isinstance(self.default, str):
            return eval_param_default(self.default, get_dataset)
        return self.default

    def make_store(self, identity: tuple[str, str], value: float) -> FloatParamStore:
        if self.min is not None and value < self.min:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} below minimum of {self.min}")
        if self.max is not None and value > self.max:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} above maximum of {self.max}")
        return FloatParamStore(identity, value)


class IntParam(ParamBase):
    HandleType = IntParamHandle
    StoreType = IntParamStore
    CompilerType = int32

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: str | int,
                 *,
                 min: int | None = 0,
                 max: int | None = None,
                 unit: str = "",
                 scale: int | None = None,
                 is_scannable: bool = True):

        ParamBase.__init__(self,
                           fqn=fqn,
                           description=description,
                           default=default,
                           min=min,
                           max=max,
                           unit=unit,
                           scale=scale,
                           is_scannable=is_scannable)
        self.scale = resolve_numeric_scale(scale, unit)
        if self.scale != 1:
            raise NotImplementedError(
                "Non-unity scales not implemented for integer parameters")

        self.is_scannable = is_scannable

    def describe(self) -> dict[str, Any]:
        spec = {"is_scannable": self.is_scannable, "scale": self.scale}
        if self.min is not None:
            spec["min"] = self.min
        if self.max is not None:
            spec["max"] = self.max
        if self.unit:
            spec["unit"] = self.unit
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "int",
            "default": str(self.default),
            "spec": spec
        }

    def eval_default(self, get_dataset: GetDataset) -> int:
        if isinstance(self.default, str):
            return eval_param_default(self.default, get_dataset)
        return self.default

    def make_store(self, identity: tuple[str, str], value: int) -> IntParamStore:
        if self.min is not None and value < self.min:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} below minimum of {self.min}")
        if self.max is not None and value > self.max:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} above maximum of {self.max}")

        return IntParamStore(identity, value)


class StringParam(ParamBase):
    HandleType = StringParamHandle
    StoreType = StringParamStore
    CompilerType = str

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: str,
                 is_scannable: bool = True):

        ParamBase.__init__(self,
                           fqn=fqn,
                           description=description,
                           default=default,
                           is_scannable=is_scannable)

    def describe(self) -> dict[str, Any]:
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "string",
            "default": str(self.default),
            "spec": {
                "is_scannable": self.is_scannable
            }
        }

    def eval_default(self, get_dataset: GetDataset) -> str:
        return eval_param_default(self.default, get_dataset)

    def make_store(self, identity: tuple[str, str], value: str) -> StringParamStore:
        return StringParamStore(identity, value)


class BoolParam(ParamBase):
    HandleType = BoolParamHandle
    StoreType = BoolParamStore
    CompilerType = bool

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: str | bool,
                 is_scannable: bool = True):
        super().__init__(fqn=fqn,
                         description=description,
                         default=default,
                         is_scannable=is_scannable)

    def describe(self) -> dict[str, Any]:
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "bool",
            "default": str(self.default),
            "spec": {
                "is_scannable": self.is_scannable
            }
        }

    def eval_default(self, get_dataset: GetDataset) -> bool:
        if isinstance(self.default, str):
            return eval_param_default(self.default, get_dataset)
        return self.default

    def make_store(self, identity: tuple[str, str], value: bool) -> BoolParamStore:
        return BoolParamStore(identity, value)


def enum_param_factory(enum: Enum):
    """Create a new parameter type based on the given `Enum`.

    :returns: A tuple of ``(EnumParam, EnumParamHandle, EnumParamStore)`` types.
    """
    class EnumParamStore(ParamStore):
        @portable
        def _notify_handles(self):
            for h in self._handles:
                h._changed_after_use = True

        @portable
        def _do_nothing(self):
            pass

        @portable
        def get_value(self) -> enum:
            return self._value

        @portable
        def set_value(self, value):
            new_value = self.coerce(value)
            if new_value == self._value:
                return
            self._value = new_value
            self._notify()

        @portable
        def coerce(self, value):
            return enum[value]

    class EnumParamHandle(ParamHandle):
        @portable
        def get(self) -> enum:
            return self._store.get_value()

        @portable
        def use(self) -> enum:
            self._changed_after_use = False
            return self._store.get_value()

    # Create unique identifier for the enum.
    type_string = f"enum_{enum.__name__}_{id(enum)}"

    class EnumParam:
        HandleType = EnumParamHandle
        StoreType = EnumParamStore
        CompilerType = enum

        def __init__(self,
                     fqn: str,
                     description: str,
                     default: enum | str,
                     is_scannable: bool = True):
            self.fqn = fqn
            self.description = description

            # `self.default` is either an instance of the `enum`, or a string value to
            # be `eval`uated later, mapping to the name of a member of `enum`.
            if isinstance(default, enum):
                self.default = repr(default.name)
            else:
                self.default = default

            self.is_scannable = is_scannable

        def _option_to_str(self, option):
            return (option.value if isinstance(option, str) else option.name)

        def describe(self) -> dict[str, Any]:
            # Mapping names of `enum` members to display strings. At this point, we
            # decide to display `enum.value` instead of `enum.name` if the former is
            # a string.
            enum_display_map = OrderedDict(
                (o.name, o.value if isinstance(o.value, str) else o.name) for o in enum)
            return {
                "fqn": self.fqn,
                "description": self.description,
                "type": type_string,
                "default": self.default,
                "spec": {
                    "enum_display_map": enum_display_map,
                    "is_scannable": self.is_scannable
                }
            }

        def eval_default(self, get_dataset: GetDataset) -> enum:
            default = eval_param_default(self.default, get_dataset)
            return enum[default]

        def make_store(self, identity: tuple[str, str], value: enum) -> EnumParamStore:
            return EnumParamStore(identity, value)

    # Add dynamically created EnumParam to the global collection.
    _type_string_to_param[type_string] = EnumParam

    return (EnumParam, EnumParamHandle, EnumParamStore)


_type_string_to_param.update({
    "float": FloatParam,
    "int": IntParam,
    "string": StringParam,
    "bool": BoolParam
})

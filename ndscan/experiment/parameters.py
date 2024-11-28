"""Fragment-side parameter containers.

In practical use, these will be instantiated by calling :meth:`Fragment.setattr_param`
with the appropriate type argument (:class:`FloatParam`, :class:`IntParam`,
:class:`StringParam`, :class:`BoolParam`, :class:`EnumParam`).
"""

# The ARTIQ compiler does not support templates or generics (neither in the sense
# of typing.Generic, nor any other), yet requires the inferred types/signatures
# of fields to match across all instances of a class. Hence, we have no option
# but to hang our heads in shame and manually instantiate the parameter handling
# machinery for all supported value types, in particular to handle cases where e.g.
# both an int and a float parameter is scanned at the same time.
from artiq.language import host_only, portable, units
from enum import Enum
from numpy import int32
from typing import Any, TYPE_CHECKING
from ..utils import eval_param_default, GetDataset

__all__ = [
    "InvalidDefaultError", "ParamStore", "ParamHandle", "FloatParam", "IntParam",
    "StringParam", "BoolParam", "EnumParam"
]

if TYPE_CHECKING:
    from .fragment import Fragment


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
    def _register_handle(self, handle):
        # Private to this module (part of the handle change_after_used tracking).
        self._handles.append(handle)
        self._notify = self._notify_handles

    @host_only
    def _unregister_handle(self, handle):
        # Private to this module (part of the handle change_after_used tracking).
        self._handles.remove(handle)

        if not self._handles:
            self._notify = self._do_nothing

    #: The type to use for this parameter in the RPC layer (to be overridden by
    #: subclasses).
    RpcType = Any

    @portable
    def get_value(self) -> Any:
        raise NotImplementedError

    @portable
    def set_value(self, value: Any) -> None:
        raise NotImplementedError

    @portable
    def coerce(self, value: Any) -> Any:
        raise NotImplementedError

    @host_only
    def to_rpc_type(self, value) -> RpcType:
        """For types that need to be represented differently in the RPC layer (enums),
        convert the value from overrides/scan generators/etc. to the type used across
        the RPC interface.
        """
        return value

    @portable
    def set_from_rpc(self, value) -> None:
        """For types that need to be represented differently in the RPC layer (enums),
        convert the value back to the type used in the kernel.
        """
        self.set_value(value)

    @classmethod
    def value_from_pyon(cls, value):
        """
        """
        return value


class FloatParamStore(ParamStore):
    RpcType = float

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

    @portable
    def set_from_rpc(self, value) -> None:
        self.set_value(value)


class IntParamStore(ParamStore):
    RpcType = int32

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
        return int32(value)

    @portable
    def set_from_rpc(self, value) -> None:
        self.set_value(value)


class StringParamStore(ParamStore):
    RpcType = str

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
        return value

    @portable
    def set_from_rpc(self, value) -> None:
        self.set_value(value)


class BoolParamStore(ParamStore):
    RpcType = bool

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

    @portable
    def set_from_rpc(self, value) -> None:
        self.set_value(value)


class ParamHandle:
    """
    Each instance of this class corresponds to exactly one attribute of a fragment that
    can be used to access the underlying parameter store.

    :param owner: See :attr:`owner`.
    :param name: See :attr:`name`.
    :param parameter: The parameter initially associated with this handle (see
        :attr:`parameter`).
    """
    def __init__(self, owner: "Fragment", name: str, parameter):
        #: The :class:`Fragment` owning this parameter handle.
        self.owner = owner

        #: The name of the attribute in the owning fragment that corresponds to this
        #: object.
        self.name = name

        #: Points to the parameter currently associated with this handle, tracking
        #: binding of the parameter.
        self.parameter = parameter

        assert name.isidentifier(), ("ParamHandle name should be the identifier it is "
                                     "referred to as in the owning fragment.")

        self._store = None
        self._changed_after_use = True

    def set_store(self, store: ParamStore) -> None:
        """
        """
        if self._store:
            self._store._unregister_handle(self)
        store._register_handle(self)
        self._store = store
        self._changed_after_use = True

    @portable
    def changed_after_use(self) -> bool:
        """
        """
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
    HandleType = ParamHandle
    StoreType = ParamStore

    def __init__(self, **kwargs):
        # Store kwargs for param rebinding
        self.init_params = kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)

    def describe(self) -> dict[str, Any]:
        """
        """
        raise NotImplementedError

    def eval_default(self, get_dataset: GetDataset) -> Any:
        """
        """
        raise NotImplementedError

    def make_store(self, identity: tuple[str, str], value: float) -> ParamStore:
        """
        """
        raise NotImplementedError


class FloatParam(ParamBase):
    """
    """

    HandleType = FloatParamHandle
    StoreType = FloatParamStore
    CompilerType = float  # deprecated (not used in ndscan anymore); will go away

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
        """"""
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
        """"""
        if isinstance(self.default, str):
            return eval_param_default(self.default, get_dataset)
        return self.default

    def make_store(self, identity: tuple[str, str], value: float) -> FloatParamStore:
        """"""
        if self.min is not None and value < self.min:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} below minimum of {self.min}")
        if self.max is not None and value > self.max:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} above maximum of {self.max}")
        return FloatParamStore(identity, value)


class IntParam(ParamBase):
    """
    """

    HandleType = IntParamHandle
    StoreType = IntParamStore
    CompilerType = int32  # deprecated (not used in ndscan anymore); will go away

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
        """"""
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
        """"""
        if isinstance(self.default, str):
            return eval_param_default(self.default, get_dataset)
        return self.default

    def make_store(self, identity: tuple[str, str], value: int) -> IntParamStore:
        """"""
        if self.min is not None and value < self.min:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} below minimum of {self.min}")
        if self.max is not None and value > self.max:
            raise InvalidDefaultError(
                f"Value {value} for parameter {self.fqn} above maximum of {self.max}")

        return IntParamStore(identity, value)


def _raise_not_implemented(*args):
    raise NotImplementedError


class StringParam(ParamBase):
    """
    """

    HandleType = StringParamHandle
    StoreType = StringParamStore
    CompilerType = str  # deprecated (not used in ndscan anymore); will go away

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: str,
                 is_scannable: bool = True):
        try:
            eval_param_default(default, _raise_not_implemented)
        except NotImplementedError:
            # This parsed and called dataset(), so okay.
            pass
        except Exception:
            # Contrary to usual ndscan style, do not put quotation marks around the
            # value here and rather put it inside parentheses for clarity, as the user
            # error is likely to be missing quotes. Also do not chain this onto the
            # eval() error, as that does not add any extra information.
            raise InvalidDefaultError(
                "Default value for StringParam must be valid PYON, missing quotes? " +
                f"(got: {default})") from None
        ParamBase.__init__(self,
                           fqn=fqn,
                           description=description,
                           default=default,
                           is_scannable=is_scannable)

    def describe(self) -> dict[str, Any]:
        """"""
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
        """"""
        return eval_param_default(self.default, get_dataset)

    def make_store(self, identity: tuple[str, str], value: str) -> StringParamStore:
        """"""
        return StringParamStore(identity, value)


class BoolParam(ParamBase):
    """
    """

    HandleType = BoolParamHandle
    StoreType = BoolParamStore
    CompilerType = bool  # deprecated (not used in ndscan anymore); will go away

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
        """"""
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
        """"""
        if isinstance(self.default, str):
            return eval_param_default(self.default, get_dataset)
        return self.default

    def make_store(self, identity: tuple[str, str], value: bool) -> BoolParamStore:
        """"""
        return BoolParamStore(identity, value)


_enum_compiler_type_cache = {}


def _get_enum_compiler_types(
        enum_type: type[Enum]) -> tuple[type[ParamStore], type[ParamHandle]]:
    if enum_type not in _enum_compiler_type_cache:
        # Cannot have `-> enum_type` annotations on get_value()/get()/use() here, as
        # this gives an "is not an ARTIQ type" error (despite working just fine when
        # relying on type inference).

        class EnumParamStore(ParamStore):
            RpcType = int32
            # TODO: Make sure this is emitted efficiently as a global by the compiler.
            instances = [o for o in enum_type]

            @portable
            def _notify_handles(self):
                for h in self._handles:
                    h._changed_after_use = True

            @portable
            def _do_nothing(self):
                pass

            @portable
            def get_value(self):
                return self._value

            @portable
            def set_value(self, value):
                if value is self._value:
                    return
                self._value = value
                self._notify()

            @portable
            def coerce(self, value):
                # Can't ensure type matches on compiler, since enums are arbitrary
                # classes as far as the ARTIQ compiler is concerned.
                return value

            @host_only
            def to_rpc_type(self, value: enum_type) -> RpcType:
                return self.instances.index(value)

            @portable
            def set_from_rpc(self, value: RpcType):
                self.set_value(self.instances[value])

            @classmethod
            def value_from_pyon(cls, value):
                return enum_type[value]

        class EnumParamHandle(ParamHandle):
            @portable
            def get(self):
                return self._store.get_value()

            @portable
            def use(self):
                self._changed_after_use = False
                return self._store.get_value()

        _enum_compiler_type_cache[enum_type] = (EnumParamStore, EnumParamHandle)
    return _enum_compiler_type_cache[enum_type]


class EnumParam(ParamBase):
    """
    """

    # EnumParam can't support HandleType/StoreType as class attributes, as we need
    # one class per actual enum type

    def __init__(self,
                 fqn: str,
                 description: str,
                 default: Enum | str,
                 enum_class: type[Enum] | None = None,
                 is_scannable: bool = True):
        if enum_class is None:
            if isinstance(default, Enum):
                enum_class = type(default)
            elif isinstance(default, str):
                raise ValueError("enum_class must be specified if default is a string")
            else:
                raise InvalidDefaultError("Unexpected default for EnumParam " +
                                          f"'{default}' (type {type(default)})")
        if isinstance(default, str):
            try:
                enum_class[eval_param_default(default, _raise_not_implemented)]
            except NotImplementedError:
                # This parsed and called dataset(), so okay.
                pass
            except Exception:
                raise InvalidDefaultError(
                    "str default values for EnumParm must be valid PYON strings " +
                    "(including quotes) that evaluate to the name of an enum member " +
                    f"(got: \"{default}\")")
        self.StoreType, self.HandleType = _get_enum_compiler_types(enum_class)
        super().__init__(fqn=fqn,
                         description=description,
                         default=default,
                         enum_class=enum_class,
                         is_scannable=is_scannable)

    def describe(self) -> dict[str, Any]:
        """"""
        # Mapping names of `enum` members to display strings. At this point, we
        # decide to display `enum.value` instead of `enum.name` if the former is
        # a string.
        members = {
            o.name: o.value if isinstance(o.value, str) else o.name
            for o in self.enum_class
        }
        # Enums are not PYON-compatible, so we need to express enum values by their
        # names. As strings in default values are always eval()d (they could be
        # dataset()) calls, use repr() to add quotes.
        default = self.default
        if isinstance(default, Enum):
            default = repr(default.name)
        return {
            "fqn": self.fqn,
            "description": self.description,
            "type": "enum",
            "default": default,
            "spec": {
                "members": members,
                "is_scannable": self.is_scannable
            }
        }

    def eval_default(self, get_dataset: GetDataset) -> Enum:
        """"""
        if isinstance(self.default, str):

            def to_member(value):
                if isinstance(value, str):
                    return self.enum_class[value]
                if isinstance(value, self.enum_class):
                    return value
                raise InvalidDefaultError("Unexpected default for EnumParam " +
                                          f"'{value}' (type {type(value)})")

            # Converting the overall result rather than wrapping get_dataset is a bit
            # overly permissive in that it also allows "'foo'" to refer to Foo.foo,
            # rather than just when used as "dataset('bar', 'foo')"
            return to_member(eval_param_default(self.default, get_dataset))
        return self.default

    def make_store(self, identity: tuple[str, str], value: Enum) -> ParamStore:
        """"""
        return self.StoreType(identity, value)

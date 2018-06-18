from artiq.language import *
from artiq.protocols import pyon
from collections import OrderedDict
from contextlib import suppress
from typing import Any, Callable, Dict, List, Type, TypeVar


ValueT = TypeVar("ValueT")
class Parameter:
    def __init__(self, fqn: str):
        self.fqn = fqn
        self._value = None # XXX
        self._changed_after_use = True

    def get(self) -> ValueT:
        return self._value

    def use(self) -> ValueT:
        self._changed_after_use = False
        return self.get()

    def set(self, value: ValueT) -> None:
        self._changed_after_use = True
        self._value = value


class Fragment(HasEnvironment):
    def build(self, fragment_path: List[str]):
        self._fragment_path = fragment_path
        self._subfragments = []
        self._free_params = OrderedDict()

        self._building = True
        self.build_fragment()
        self._building = False

    def device_setup(self) -> None:
        pass

    def device_reset(self, changes: list) -> None:
        # By default, just completely reinitialize.
        self.device_setup()

    def build_fragment(self) -> None:
        raise NotImplementedError("build_fragment() not implemented; add parameters/result channels here")

    def setattr_fragment(self, name: str, fragment_class: Type["Fragment"]) -> None:
        assert self._building, "Can only call setattr_fragment during build_fragment()"
        f = fragment_class(self, self._fragment_path + [name])
        self._subfragments.append(f)
        setattr(self, name, f)

    def setattr_param(self, name: str, description: str) -> None:
        klass = self.__class__
        fqn = ".".join([klass.__module__, klass.__qualname__, name])

        self._free_params[name] = {"fqn": fqn, "description": description}
        setattr(self, name, Parameter(fqn))

    def setattr_result(self, name: str, description: str = "", display_hints: Dict[str, any] = {}) -> None:
        # TODO
        pass

    def _build_param_tree(self, params: Dict[str, List[str]], schemata: Dict[str, dict]) -> None:
        fqns = []
        for k, v in self._free_params.items():
            fqn = v["fqn"]
            if fqn in schemata:
                if schemata[fqn] != v:
                    logger.warn("Mismatch in parameter schema '%s' for '%s'", fqn, path)
            else:
                schemata[fqn] = v
            fqns.append(fqn)
        params["/".join(self._fragment_path)] = fqns

        for s in self._subfragments:
            s._build_param_tree(params, schemata)

    def _apply_param_overrides(self, overrides: List[Dict[str, dict]]) -> None:
        for s in self._subfragments:
            s._apply_param_overrides(overrides)

    def _get_always_shown_params(self):
        return [(p["fqn"], "/".join(self._fragment_path)) for p in self._free_params.values()]


class ExpFragment(Fragment):
    def host_setup(self):
        """Called before kernel is entered for the first time.

        TODO: Semantics for multiple invocations.
        """
        pass

    def run_once(self):
        pass

    def analyze(self):
        pas

import logging

from artiq.language import *
from artiq.protocols import pyon
from collections import OrderedDict
from contextlib import suppress
from typing import Any, Callable, Dict, List, Type, TypeVar
from .utils import path_matches_spec, strip_prefix

logger = logging.getLogger(__name__)


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

        klass = self.__class__
        mod = klass.__module__
        # KLUDGE: Strip prefix added by file_import() to make path matches compatible across
        # dashboard/artiq_run and the worker running the experiment. Should be fixed at the source.
        for f in ["artiq_run_", "artiq_worker_", "file_import_"]:
            mod = strip_prefix(mod, f)
        self._fqn_prefix = mod + "." + klass.__qualname__

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
        assert self._building, "Can only call setattr_fragment() during build_fragment()"
        assert name.isidentifier(), "Subfragment name must be valid Python identifier"

        f = fragment_class(self, self._fragment_path + [name])
        self._subfragments.append(f)
        setattr(self, name, f)

    def setattr_param(self, name: str, description: str) -> None:
        assert self._building, "Can only call setattr_param() during build_fragment()"
        assert name.isidentifier(), "Parameter name must be valid Python identifier"

        fqn = self._fqn_prefix + "." + name
        self._free_params[name] = {"fqn": fqn, "description": description}
        setattr(self, name, Parameter(fqn))

    def setattr_result(self, name: str, description: str = "", display_hints: Dict[str, any] = {}) -> None:
        assert self._building, "Can only call setattr_result() during build_fragment()"
        assert name.isidentifier(), "Result channel name must be valid Python identifier"
        # TODO

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

    def _apply_param_overrides(self, overrides: Dict[str, List[dict]]) -> None:
        for name, schema in self._free_params.items():
            for o in overrides.get(schema["fqn"], []):
                if path_matches_spec(self._fragment_path, o["path"]):
                    # TODO: Default handling/…
                    getattr(self, name).set(o["value"])
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

from artiq.language import *
from artiq.protocols import pyon
from contextlib import suppress
from typing import Callable, Dict, List, Type


class Fragment(HasEnvironment):
    def build(self, fragment_path: List[str]):
        self._fragment_path = fragment_path
        self._subfragments = []
        self._free_params = dict()

        self._building = True
        self.build_fragment()
        self._building = False

    def device_setup(self):
        pass

    def device_reset(self, changes):
        # By default, just completely reinitialize.
        self.device_setup()

    def build_fragment(self):
        raise NotImplementedError("build_fragment() not implemented; add parameters/result channels here")

    def setattr_fragment(self, name: str, fragment_class: Type["Fragment"]):
        assert self._building, "Can only call setattr_fragment during build_fragment()"
        f = fragment_class(self, self._fragment_path + [name])
        self._subfragments.append(f)
        setattr(self, name, f)

    def setattr_param(self, name: str, description: str):
        # TODO: Actually do things.
        self._free_params[name] = {"description": description}

    def setattr_result(self, name: str, description: str = "", display_hints: Dict[str, any] = {}):
        # TODO
        pass

    def _build_param_schema(self):
        result = dict()
        for k, v in self._free_params.items():
            result["/".join(self._fragment_path + [k])] = v
        for s in self._subfragments:
            result.update(s._build_param_schema())
        return result


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

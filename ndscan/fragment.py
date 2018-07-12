from copy import deepcopy
import logging
import numpy

from artiq.language import *
from collections import OrderedDict
from contextlib import suppress
from typing import Any, Callable, Dict, Generic, List, Type, Union
from .parameters import *
from .result_channels import *
from .utils import path_matches_spec, strip_prefix

logger = logging.getLogger(__name__)


class Fragment(HasEnvironment):
    def build(self, fragment_path: List[str], *args, **kwargs):
        self._fragment_path = fragment_path
        self._subfragments = []
        self._free_params = OrderedDict()
        self._rebound_subfragment_params = dict() #: Own key to subfragment handles.
        self._result_channels = {}

        klass = self.__class__
        mod = klass.__module__
        # KLUDGE: Strip prefix added by file_import() to make path matches compatible across
        # dashboard/artiq_run and the worker running the experiment. Should be fixed at the source.
        for f in ["artiq_run_", "artiq_worker_", "file_import_"]:
            mod = strip_prefix(mod, f)
        self.fqn = mod + "." + klass.__qualname__

        self._building = True
        self.build_fragment(*args, **kwargs)
        self._building = False

    def host_setup(self):
        """Called before kernel is entered for the first time.

        TODO: Define semantics for multiple invocations.
        """
        pass

    @kernel
    def device_setup(self) -> None:
        pass

    @kernel
    def device_reset(self) -> None:
        # By default, just completely reinitialize.
        self.device_setup()

    def build_fragment(self, *args, **kwargs) -> None:
        raise NotImplementedError("build_fragment() not implemented; override it to add parameters/result channels.")

    def setattr_fragment(self, name: str, fragment_class: Type["Fragment"], *args, **kwargs) -> None:
        assert self._building, "Can only call setattr_fragment() during build_fragment()"
        assert name.isidentifier(), "Subfragment name must be valid Python identifier"
        assert not hasattr(self, name), "Field '%s' already exists".format(name)

        frag = fragment_class(self, self._fragment_path + [name], *args, **kwargs)
        self._subfragments.append(frag)
        setattr(self, name, frag)

    def setattr_param(self, name: str, param_class: Type, description: str, *args, **kwargs) -> None:
        assert self._building, "Can only call setattr_param() during build_fragment()"
        assert name.isidentifier(), "Parameter name must be valid Python identifier"
        assert not hasattr(self, name), "Field '%s' already exists".format(name)

        fqn = self.fqn + "." + name
        self._free_params[name] = param_class(fqn, description, *args, **kwargs)
        setattr(self, name, param_class.HandleType())

    def setattr_param_rebind(self, name: str, original_owner, original_name, **kwargs) -> None:
        assert self._building, "Can only call setattr_param_rebind() during build_fragment()"
        assert name.isidentifier(), "Parameter name must be valid Python identifier"
        assert not hasattr(self, name), "Field '%s' already exists".format(name)

        # Set up our own copy of the parameter.
        original_param = original_owner._free_params[original_name]
        param = deepcopy(original_param)
        param.fqn = self.fqn + "." + name
        for k, v in kwargs.items():
            setattr(param, k, v)
        self._free_params[name] = param
        setattr(self, name, param.HandleType())

        # Deregister it from the original owner and make sure we set the store
        # to our own later.
        del original_owner._free_params[original_name]
        original_handle = getattr(original_owner, original_name)
        self._rebound_subfragment_params.setdefault(name, []).append(original_handle)

    def setattr_result(self, name: str, channel_class: Type = FloatChannel, *args, **kwargs) -> None:
        assert self._building, "Can only call setattr_result() during build_fragment()"
        assert name.isidentifier(), "Result channel name must be valid Python identifier"
        assert not hasattr(self, name), "Field '%s' already exists".format(name)

        path = "/".join(self._fragment_path + [name])
        channel = channel_class(path, *args, **kwargs)
        self._result_channels[path] = channel
        setattr(self, name, channel)

    def _collect_params(self, params: Dict[str, List[str]], schemata: Dict[str, dict]) -> None:
        """Collect parameters of this fragment and all its subfragments.

        :param params: Dictionary to write the list of instance paths for each
            parameter to, indexed by FQN.
        :param schemeta: Dictionary to write the schemata for each parameter to,
            indexed by FQN.
        """
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
            s._collect_params(params, schemata)

    def _apply_param_overrides(self, overrides: Dict[str, List[dict]]) -> None:
        for name, param in self._free_params.items():
            store = None
            for o in overrides.get(param.fqn, []):
                if path_matches_spec(self._fragment_path, o["path"]):
                    store = o["store"]
            if not store:
                store = param.default_store(self.get_dataset)

            getattr(self, name).set_store(store)
            for handle in self._rebound_subfragment_params.get(name, []):
                handle.set_store(store)

        for s in self._subfragments:
            s._apply_param_overrides(overrides)

    def _get_always_shown_params(self) -> List[str]:
        return [(p.fqn, "/".join(self._fragment_path)) for p in self._free_params.values()]

    def _collect_result_channels(self, channels: dict):
        channels.update(self._result_channels)
        for s in self._subfragments:
            s._collect_result_channels(channels)


class ExpFragment(Fragment):
    def run_once(self):
        pass

    def analyze(self):
        pass

from artiq.language import HasEnvironment, kernel, kernel_from_string, portable, rpc
from collections import OrderedDict
from collections.abc import Iterable
from copy import deepcopy
import logging
from typing import Any

from .default_analysis import DefaultAnalysis, ResultPrefixAnalysisWrapper
from .parameters import ParamHandle, ParamStore, ParamBase
from .result_channels import ResultChannel, FloatChannel
from .utils import is_kernel, path_matches_spec
from ..utils import strip_prefix

__all__ = [
    "Fragment", "ExpFragment", "AggregateExpFragment", "TransitoryError",
    "RestartKernelTransitoryError"
]

logger = logging.getLogger(__name__)


@rpc(flags={"async"})
def _log_failed_cleanup_host(path: str) -> None:
    logger.error(f"device_cleanup() failed for '{path}'.")


@portable
def _log_failed_cleanup(path: str) -> None:
    """Log error message for failed subfragment ``device_cleanup``.

    Single kernel (well, portable) function, rather than directly an RPC call, to
    tighten up kernel codegen for multiple cleanups.
    """
    # TODO: Figure out how to funnel the original exception details over RPC to provide
    # a more usable error message to the user.
    _log_failed_cleanup_host(path)


class Fragment(HasEnvironment):
    """Main building block."""
    def build(self, fragment_path: list[str], *args, **kwargs):
        """Initialise this fragment instance; called from the ``HasEnvironment``
        constructor.

        This sets up the machinery for registering parameters and result channels with
        the fragment tree, and then calls :meth:`build_fragment` to actually perform the
        fragment-specific setup. This method should not typically be overwritten.

        :param fragment_path: Full path of the fragment, as a list starting from the
            root. For instance, ``[]`` for the top-level fragment, or ``["foo", "bar"]``
            for a subfragment created by ``setattr_fragment("bar", …)`` in a fragment
            created by ``setattr_fragment("foo", …)``.
        :param args: Arguments to be forwarded to :meth:`build_fragment`.
        :param kwargs: Keyword arguments to be forwarded to :meth:`build_fragment`.
        """
        self._fragment_path = fragment_path
        self._subfragments = []

        #: Maps names of non-overridden parameters of this fragment (i.e., matching the
        #: attribute names of the respective ParamHandles) to *Param instances.
        self._free_params = OrderedDict()

        #: Maps own attribute name to the ParamHandles of the rebound parameters in
        #: their original subfragment, for parameters of this Fragment which are
        #: rebind targets.
        self._rebound_subfragment_params: dict[str, list[ParamHandle]] = dict()

        #: Maps own attribute name to the ParamHandle that this parameter was
        #: rebound to, for parameters of this Fragment which have been rebound.
        self._rebound_own_params: dict[str, ParamHandle] = dict()

        #: List of (param, store) tuples of parameters set to their defaults after
        #: init_params().
        self._default_params = []

        #: Maps full path of own result channels to ResultChannel instances.
        self._result_channels = {}

        #: Subfragments detached from the normal fragment execution (setup/cleanup,
        #: result channels; e.g. for subscans).
        self._detached_subfragments = set()

        klass = self.__class__
        mod = klass.__module__
        # KLUDGE: Strip prefix added by file_import() to make path matches compatible
        # across dashboard/artiq_run and the worker running the experiment. Should be
        # fixed at the source.
        for f in ["artiq_run_", "artiq_worker_", "file_import_"]:
            mod = strip_prefix(mod, f)
        self.fqn = mod + "." + klass.__qualname__

        # Mangle the arguments into the FQN, so they can be used to parametrise
        # the parameter definitions.
        # TODO: Also handle kwargs, make sure this generates valid identifiers.
        for a in args:
            self.fqn += "_"
            self.fqn += str(a)

        self._building = True
        self.build_fragment(*args, **kwargs)
        self._building = False

        # Now that we know all subfragments, synthesise code for device_setup() and
        # device_cleanup() to forward to subfragments.
        code = ""
        for s in self._subfragments:
            if s in self._detached_subfragments:
                continue
            if s._has_trivial_device_setup():
                continue
            code += f"self.{s._fragment_path[-1]}.device_setup()\n"
        if code:
            self._all_subfragment_setup_trivial = False
            self._device_setup_subfragments_impl = kernel_from_string(["self"],
                                                                      code[:-1],
                                                                      portable)
        else:
            self._all_subfragment_setup_trivial = True
            # TODO: Make this work across multiple types to save on empty …_impl().
            # self.device_setup_subfragments = self._noop
            self._device_setup_subfragments_impl = kernel_from_string(["self"], "pass",
                                                                      portable)

        code = ""
        for s in self._subfragments[::-1]:
            if s in self._detached_subfragments:
                continue
            if s._has_trivial_device_cleanup():
                continue
            frag = "self." + s._fragment_path[-1]
            code += "try:\n"
            code += f"    {frag}.device_cleanup()\n"
            code += "except:\n"
            code += f"    log_failed_cleanup('{s._stringize_path()}')\n"
        if code:
            self._all_subfragment_cleanup_trivial = False
            self._device_cleanup_subfragments_impl = kernel_from_string(
                ["self", "log_failed_cleanup"], code[:-1], portable)
        else:
            self._all_subfragment_cleanup_trivial = True
            # TODO: Make this work across multiple types to save on empty …_impl().
            # self.device_cleanup_subfragments = self._noop
            self._device_cleanup_subfragments_impl = kernel_from_string(
                ["self", "log_failed_cleanup"], "pass", portable)

    def _has_trivial_device_setup(self):
        assert not self._building
        empty_setup = self.device_setup.__func__ is Fragment.device_setup
        return empty_setup and self._all_subfragment_setup_trivial

    def _has_trivial_device_cleanup(self):
        assert not self._building
        empty_cleanup = self.device_cleanup.__func__ is Fragment.device_cleanup
        return empty_cleanup and self._all_subfragment_cleanup_trivial

    def host_setup(self):
        """Perform host-side initialisation.

        For fragments used as part of an on-core-device scan (i.e. with an ``@kernel``
        :meth:`device_setup`/:meth:`run_once`), this will be called on the host,
        immediately before the top-level kernel function is entered.

        Typical uses include initialising member variables for latter use in ``@kernel``
        functions, and setting parameters not modifiable from kernels (e.g. because
        modifying a parameter requires launching another experiment to effect the
        change).

        The default implementation calls :meth:`host_setup` recursively on all
        subfragments. When overriding it in a fragment with subfragments, consider
        forwarding to the default implementation (see example).

        Example::

            def host_setup(self):
                initialise_some_things()

                # To continue to initialise all subfragments, invoke the parent
                # implementation:
                super().host_setup()
        """
        for s in self._subfragments:
            if s in self._detached_subfragments:
                continue
            s.host_setup()

    @portable
    def device_setup(self) -> None:
        """Perform core-device-side initialisation.

        A typical implementation will make sure that any hardware state represented by
        the fragment (e.g. some DAC voltages, DDS frequencies, etc.) is updated to
        match the fragment parameters.

        If the fragment is used as part of a scan, ``device_setup()`` will be called
        immediately before each ``ExpFragment.run_once()`` call (and, for on-core-device
        scans, from within the same kernel).

        The default implementation calls :meth:`device_setup_subfragments` to initialise
        all subfragments. When overriding it, consider forwarding to it too unless a
        special initialisation order, etc. is required (see example).

        Example::

            @kernel
            def device_setup(self):
                self.device_setup_subfragments()

                self.core.break_realtime()
                if self.my_frequency.changed_after_use():
                    self.my_dds.set(self.my_frequency.use())
                self.my_ttl.on()
        """
        self.device_setup_subfragments()

    @portable
    def device_setup_subfragments(self) -> None:
        """Call :meth:`device_setup` on all subfragments.

        This is the default implementation for :meth:`device_setup`, but is kept
        separate so that subfragments overriding :meth:`device_setup` can still access
        it. (ARTIQ Python does not support calling superclass implementations in a
        polymorphic way – ``Fragment.device_setup(self)`` could be used from one
        subclass, but would cause the argument type to be inferred as that subclass.
        Only direct member function calls are special-cased to be generic on the
        `self` type.)
        """
        # Forward to implementation generated using kernel_from_string().
        self._device_setup_subfragments_impl(self)

    def host_cleanup(self):
        """Perform host-side cleanup after an experiment has been run.

        This is the equivalent of :meth:`host_setup` to be run *after* the main
        experiment. It is executed on the host after the top-level kernel function, if
        any, has been left, as control is about to leave the experiment (whether because
        a scan has been finished, or the experiment is about to be paused for a
        higher-priority run to be scheduled in.

        Typically, fragments should strive to completely initialise the state of all
        their dependencies for robustness. As such, manual cleanup should almost never
        be necessary.

        By default, calls `host_cleanup()` on all subfragments, in reverse
        initialisation order. The default implementation catches all exceptions thrown
        from cleanups and converts them into log messages to ensure no cleanups are
        skipped. As there will be no exception propagating to the caller to mark the
        experiment as failed, failing cleanups should be avoided.

        Example::

            def host_cleanup(self):
                tear_down_some_things()
                super().host_setup()
        """
        for s in self._subfragments[::-1]:
            if s in self._detached_subfragments:
                continue
            try:
                s.host_cleanup()
            except Exception:
                logger.exception("Cleanup failed for '%s'", s._stringize_path())

    @portable
    def device_cleanup(self) -> None:
        """Perform core-device-side teardown.

        This is the equivalent of :meth:`device_setup`, run after the main experiment.
        It is executed on the core device every time the top-level kernel is about to be
        left.

        Thus, if the fragment is used as part of a scan, ``device_cleanup()`` will be
        typically be called once at the end of each scan (while :meth:`device_setup`
        will be called once per scan point).

        The default implementation calls :meth:`device_clean_subfragments` to clean up
        all subfragments in reverse initialisation order. When overriding it, consider
        forwarding to it too (see example).

        Example::

            @kernel
            def device_cleanup(self):
                clean_up_some_things()
                self.device_cleanup_subfragments()
        """
        self.device_cleanup_subfragments()

    @portable
    def device_cleanup_subfragments(self) -> None:
        """Call :meth:`device_cleanup` on all subfragments.

        This is the default implementation for :meth:`device_cleanup`, but is kept
        separate so that subfragments overriding :meth:`device_cleanup` can still access
        it.

        :meth:`device_cleanup` is invoked on all subfragments in reverse initialisation
        order. To ensure no cleanups are skipped, any exceptions thrown from cleanups
        are caught and converted into log messages. As there will be no exception
        propagating to the caller to mark the experiment as failed, failing cleanups
        should be avoided.

        (ARTIQ Python does not support calling superclass implementations in a
        polymorphic way – ``Fragment.device_setup(self)`` could be used from one
        concrete fragment in the whole project, but would cause the argument type to be
        inferred as that subclass. Only direct member function calls are special-cased
        to be generic on the `self` type.)
        """
        # Forward to implementation generated using kernel_from_string().
        self._device_cleanup_subfragments_impl(self, _log_failed_cleanup)

    def build_fragment(self, *args, **kwargs) -> None:
        """Initialise this fragment, building up the hierarchy of subfragments,
        parameters and result channels.

        This is where any constructor-type initialisation should take place, similar to
        the role ``build()`` has for a bare ``HasEnvironment``.

        While this method executes, the various ``setattr_*()`` functions can be used
        to create subfragments, parameters, and result channels.

        :param args: Any extra arguments that were passed to the ``HasEnvironment``
            constructor.
        :param kwargs: Any extra keyword arguments that were passed to the
            ``HasEnvironment`` constructor.
        """
        raise NotImplementedError("build_fragment() not implemented; "
                                  "override it to add parameters/result channels.")

    def setattr_fragment(self, name: str, fragment_class: type["Fragment"], *args,
                         **kwargs) -> "Fragment":
        """Create a subfragment of the given name and type.

        Can only be called during :meth:`build_fragment`.

        :param name: The fragment name; part of the fragment path. Must be a valid
            Python identifier; the fragment will be accessible as ``self.<name>``.
        :param fragment_class: The type of the subfragment to instantiate.
        :param args: Any extra arguments to forward to the subfragment
            :meth:`build_fragment` call.
        :param kwargs: Any extra keyword arguments to forward to the subfragment
            :meth:`build_fragment` call.
        :return: The newly created fragment instance.
        """
        assert self._building, ("Can only call setattr_fragment() "
                                "during build_fragment()")
        assert name.isidentifier(), "Subfragment name must be valid Python identifier"
        assert not hasattr(self, name), f"Field '{name}' already exists"

        frag = fragment_class(self, self._fragment_path + [name], *args, **kwargs)
        self._subfragments.append(frag)
        setattr(self, name, frag)

        return frag

    def setattr_param(self, name: str, param_class: type, description: str, *args,
                      **kwargs) -> ParamHandle:
        """Create a parameter of the given name and type.

        Can only be called during :meth:`build_fragment`.

        :param name: The parameter name, to be part of its FQN. Must be a valid Python
            identifier; the parameter handle will be accessible as ``self.<name>``.
        :param param_class: The type of parameter to instantiate.
        :param description: The human-readable parameter name.
        :param args: Any extra arguments to pass to the ``param_class`` constructor.
        :param kwargs: Any extra keyword arguments to pass to the the ``param_class``
            constructor.
        :return: The newly created parameter handle.
        """
        assert self._building, "Can only call setattr_param() during build_fragment()"
        assert name.isidentifier(), "Parameter name must be valid Python identifier"
        assert not hasattr(self, name), f"Field '{name}' already exists"

        fqn = self.fqn + "." + name
        param = param_class(fqn, description, *args, **kwargs)
        self._free_params[name] = param

        handle = param.HandleType(self, name)
        setattr(self, name, handle)
        return handle

    def setattr_param_like(self,
                           name: str,
                           original_owner: "Fragment",
                           original_name: str | None = None,
                           **kwargs) -> ParamHandle:
        """Create a new parameter using an existing parameter as a template.

        The newly created parameter will inherent its type, and all the metadata that
        is not overridden by the optional keyword arguments, from the template
        parameter.

        This is often combined with :meth:`bind_param` to rebind parameters from one or
        more subfragments; see also :meth:`setattr_param_rebind`, which combines the
        two.

        Can only be called during :meth:`build_fragment`.

        :param name: The new parameter's name, to be part of its FQN. Must be a valid
            Python identifier; the parameter handle will be accessible as
            ``self.<name>``.
        :param original_owner: The fragment owning the parameter to use as a template.
        :param original_name: The name of the parameter to use as a template (i.e.
            ``<original_owner>.<original_name>``). If ``None``, defaults to ``name``.
        :param kwargs: Any attributes to override in the template parameter metadata.
        :return: The newly created parameter handle.
        """
        assert self._building, ("Can only call setattr_param_like() during "
                                "build_fragment()")

        assert name.isidentifier(), "Parameter name must be valid Python identifier"
        assert not hasattr(self, name), f"Field '{name}' already exists"
        if original_name is None:
            original_name = name
        assert hasattr(original_owner, original_name), \
            f"Original owner does not have a field of name '{original_name}'"
        assert original_name in original_owner._free_params, (
            "Field '{}' is not a free parameter of original owner; "
            "already rebound?".format(original_name))

        template_param = original_owner._free_params[original_name]
        init_params = deepcopy(template_param.init_params)
        init_params.update(kwargs)
        init_params["fqn"] = self.fqn + "." + name
        new_param = template_param.__class__(**init_params)
        self._free_params[name] = new_param
        new_handle = new_param.HandleType(self, name)
        setattr(self, name, new_handle)
        return new_handle

    def setattr_param_rebind(self,
                             name: str,
                             original_owner: "Fragment",
                             original_name: str | None = None,
                             **kwargs) -> ParamHandle:
        """Convenience function combining :meth:`setattr_param_like` and
        :meth:`bind_param` to override a subfragment parmeter.

        The most common use case for this is to specialise the operation of a generic
        subfragment. For example, there might be a fragment ``Fluoresce`` that drives
        a cycling transition in an ion with parameters for intensity and detuning.
        Higher-level fragments for Doppler cooling, readout, etc. might then use
        ``Fluoresce``, rebinding its intensity and detuning parameters to values and
        defaults appropriate for those particular tasks.

        Can only be called during :meth:`build_fragment`.

        :param name: The parameter name, to be part of its FQN. Must be a valid Python
            identifier; the parameter handle will be accessible as ``self.<name>``.
        :param original_owner: The fragment owning the parameter to rebind.
        :param original_name: The name of the original parameter (i.e.
            ``<original_owner>.<original_name>``). If ``None``, defaults to ``name``.
        :param kwargs: Any attributes to override in the parameter metadata, which
            defaults to that of the original parameter.
        :return: The newly created parameter handle.
        """
        if original_name is None:
            original_name = name
        handle = self.setattr_param_like(name, original_owner, original_name, **kwargs)
        original_owner.bind_param(original_name, handle)
        return handle

    def setattr_result(self,
                       name: str,
                       channel_class: type[ResultChannel] = FloatChannel,
                       *args,
                       **kwargs) -> ResultChannel:
        """Create a result channel of the given name and type.

        Can only be called during :meth:`build_fragment`.

        :param name: The result channel name, to be part of its full path. Must be a
            valid Python identifier. The channel instance will be accessible as
            ``self.<name>``.
        :param channel_class: The type of result channel to instantiate.
        :param args: Any extra arguments to pass to the ``channel_class`` constructor.
        :param kwargs: Any extra keyword arguments to pass to the the ``channel_class``
            constructor.
        :return: The newly created result channel instance.
        """
        assert self._building, "Can only call setattr_result() during build_fragment()"
        path = "/".join(self._fragment_path + [name])
        channel = channel_class(path, *args, **kwargs)
        self._register_result_channel(name, path, channel)
        return channel

    def _register_result_channel(self, name: str, path: str,
                                 channel: ResultChannel) -> None:
        assert name.isidentifier(), ("Result channel name must be a valid "
                                     "Python identifier")
        assert not hasattr(self, name), f"Field '{name}' already exists"
        self._result_channels[path] = channel
        setattr(self, name, channel)

    def override_param(self,
                       param_name: str,
                       initial_value: Any = None) -> tuple[Any, ParamStore]:
        """Override the parameter with the given name and set it to the provided value.

        See :meth:`bind_param`, which also overrides the parameter, but sets it to
        follow another parameter instead.

        :param param_name: The name of the parameter.
        :param initial_value: The initial value for the parameter. If ``None``, the
            default from the parameter schema is used.

        :return: A tuple ``(param, store)`` of the parameter metadata and the newly
            created :class:`.ParamStore` instance that the parameter handles are now
            bound to.
        """
        param = self._free_params.get(param_name, None)
        assert param is not None, f"Not a free parameter: '{param_name}'"
        del self._free_params[param_name]

        if initial_value is None:
            initial_value = param.eval_default(self._get_dataset_or_set_default)
        store = param.make_store((param.fqn, self._stringize_path()), initial_value)
        for handle in self._get_all_handles_for_param(param_name):
            handle.set_store(store)
        return param, store

    def _find_param_source(self, param_name: str) -> ParamHandle:
        """Find the top-level source of the parameter with the given name.

        Follow the chain of rebindings to find the Fragment which currently has
        this parameter as a free parameter, and return the ParamHandle for this
        parameter within it. 

        This is used to support "transitive" binding of parameters, where a
        parameter is bound to another parameter that is itself bound to another
        parameter, etc. 
        
        Note that binding to parameters that are *overridden* instead of rebound
        is not supported. 

        :param param_name: The name of the parameter to find the target for.
        :return: The ParamHandle for the parameter in the fragment that
            has this parameter as a free parameter.
        :raises KeyError: If the parameter is not free or rebound (e.g. it was
            overridden).
        """
        if param_name in self._free_params:
            return getattr(self, param_name)
        else:
            try:
                rebound_param = self._rebound_own_params[param_name]

            # Do not support binding to parameters that are overridden
            except KeyError:
                raise KeyError(
                    f"Parameter '{param_name}' is not free or rebound. Was it overridden?"
                )

            return rebound_param.owner._find_param_source(rebound_param.name)

    def bind_param(self, param_name: str, source: ParamHandle) -> Any:
        """Override the fragment parameter with the given name such that its value
        follows that of another parameter.

        The most common use case for this is to specialise the operation of a generic
        subfragment. For example, there might be a fragment ``Fluoresce`` that drives
        a cycling transition in an ion with parameters for intensity and detuning.
        Higher-level fragments for Doppler cooling, readout, etc. might then use
        ``Fluoresce``, binding its intensity and detuning parameters to values and
        defaults appropriate for those particular tasks.

        See :meth:`override_param`, which sets the parameter to a fixed value/store.

        :param param_name: The name of the parameter to be bound (i.e.
            ``self.<param_name>``). Must be a free parameter of this fragment (not
            already bound or overridden).
        :param source: The parameter to bind to. Must not be overridden in the
            source fragment (but can be bound).
        """
        param = self._free_params.get(param_name, None)
        assert param is not None, f"Not a free parameter: '{param_name}'"

        # To support "transitive" binding of parameters, follow the chaining of
        # rebindings until a free parameter is reached.
        toplevel_source = source.owner._find_param_source(source.name)

        own_type = type(self._free_params[param_name])
        source_type = type(toplevel_source.owner._free_params[toplevel_source.name])
        assert own_type == source_type, (
            f"Cannot bind {own_type.__name__} '{param_name}' " +
            f"to source of type {source_type.__name__}")

        del self._free_params[param_name]

        self._rebound_own_params[param_name] = toplevel_source

        toplevel_source.owner._rebound_subfragment_params.setdefault(toplevel_source.name, []).extend(
            self._get_all_handles_for_param(param_name))

        return param

    def _collect_params(self, params: dict[str, list[str]], schemata: dict[str, dict],
                        sample_instances: dict[str, ParamBase]) -> None:
        """Collect free parameters of this fragment and all its subfragments.

        :param params: Dictionary to write the list of FQNs for each fragment to,
            indexed by the fragment path in string form.
        :param schemata: Dictionary to write the schemata for each parameter to,
            indexed by FQN.
        """
        path = self._stringize_path()

        fqns = []
        for param in self._free_params.values():
            fqn = param.fqn

            if fqn in sample_instances:
                assert isinstance(sample_instances[fqn], type(param)), \
                    f"Parameter type mismatch for '{fqn}' in '{path}'"
            else:
                sample_instances[fqn] = param

            schema = param.describe()
            if fqn in schemata:
                if schemata[fqn] != schema:
                    logger.warn("Mismatch in parameter schema '%s' for '%s'", fqn, path)
            else:
                schemata[fqn] = schema

            fqns.append(fqn)
        params[path] = fqns

        for s in self._subfragments:
            s._collect_params(params, schemata, sample_instances)

    def detach_fragment(self, fragment: "Fragment") -> None:
        """Detach a subfragment from the execution machinery, causing its setup and
        cleanup methods not to be invoked and its result channels not to be collected.

        Its parameters will still be available in the global tree as usual, but the
        the actual execution can be customised this way, e.g. for the implementation of
        subscans.

        :param fragment: The fragment to detach; must be a direct subfragment of this
            fragment.
        """
        assert self._building, ("Can only call detach subfragments while parent still" +
                                "in build_fragment()")
        assert fragment in self._subfragments, \
            "Can only detach subfragments directly from their parent fragment"
        assert fragment not in self._detached_subfragments, \
            "Subfragment already detached (is there already another subscan?)"
        self._detached_subfragments.add(fragment)

    def init_params(self,
                    overrides: dict[str, list[tuple[str, ParamStore]]] = {}) -> None:
        """Initialise free parameters of this fragment and all its subfragments.

        If, for a given parameter, a relevant override is given, the specified
        ParamStore is used. Otherwise, the default value is evaluated and a new store
        pointing to it created.

        This method should be called before any of the fragment's user-defined functions
        are used (but after the constructor -> :meth:`build` -> :meth`build_fragment()`
        has completed). Most likely, the top-level fragment will be called from an
        :mod:`ndscan.experiment.entry_point`, which already take care of this. In cases
        where fragments are used in a different context, for example from a standalone
        ``EnvExperiment``, this method must be called manually.

        :param overrides: A dictionary mapping parameter FQNs to lists of overrides.
            Each override is specified as a tuple `(pathspec, store)` of a path spec and
            the store to use for parameters the path of which matches the spec.
        """
        for name, param in self._free_params.items():
            store = None
            for override_pathspec, override_store in overrides.get(param.fqn, []):
                if path_matches_spec(self._fragment_path, override_pathspec):
                    store = override_store
            if not store:
                for default_param, default_store in self._default_params:
                    if param == default_param:
                        store = default_store
                        break
            if not store:
                identity = (param.fqn, self._stringize_path())
                try:
                    value = param.eval_default(self._get_dataset_or_set_default)
                except Exception:
                    raise ValueError("Error while evaluating default "
                                     "value for '{}'".format(identity))
                store = param.make_store(identity, value)
                self._default_params.append((param, store))

            for handle in self._get_all_handles_for_param(name):
                handle.set_store(store)

        for s in self._subfragments:
            s.init_params(overrides)

    def recompute_param_defaults(self) -> None:
        """Recompute default values of the parameters of this fragment and all its
        subfragments.

        For parameters where the default value was previously used, the expression is
        evaluated again – thus for instance fetching new dataset values –, and assigned
        to the existing parameter store. An informative message is logged if the value
        changed, such that changes remain traceable after the fact (e.g. when an
        experiment is resumed after a calibration interruption).
        """
        for param, store in self._default_params:
            value = param.eval_default(self._get_dataset_or_set_default)
            old_value = store.get_value()
            if old_value != value:
                logger.info("Updating %s: %s -> %s", store.identity, old_value, value)
                store.set_value(value)
        for s in self._subfragments:
            s.recompute_param_defaults()

    def make_namespaced_identifier(self, name: str) -> str:
        """Mangle passed name and path to this fragment into a string, such that calls
        from different fragments give different results for the same name.

        This can, for instance, be useful when naming DMA sequences, or interacting with
        other kinds of global registries, where multiple fragment instances should not
        conflict with each other.

        The returned string will consist of characters that are valid Python identifiers
        and slashes.
        """
        return "/".join(self._fragment_path + [name])

    def get_always_shown_params(self) -> list[ParamHandle]:
        """Return handles of parameters to always show in user interfaces when this
        fragment is the root of the fragment tree (vs. other parameters for which
        overrides need to be explicitly added).

        This can be overridden in fragment implementations to customise the user
        interface; by default, all free parameters are shown in the order they were
        created in :meth:`build_fragment`.

        Example::

            class MyFragment(Fragment):
                def build_fragment(self):
                    self.setattr_fragment("child", MyChildFragment)
                    self.setattr_param("foo", ...)
                    self.setattr_param("bar", ...)
                    self.setattr_param("baz", ...)

                def get_always_shown_params(self):
                    shown = super().get_always_shown_params()

                    # Don't show self.bar.
                    shown.remove(self.bar)

                    # Always show an important parameter from a
                    # child fragment.
                    shown.add(self.child.very_important_param)

                    return shown
        """
        return [getattr(self, name) for name in self._free_params.keys()]

    def _get_all_handles_for_param(self, name: str) -> list[ParamHandle]:
        return [getattr(self, name)] + self._rebound_subfragment_params.get(name, [])

    def _stringize_path(self) -> str:
        return "/".join(self._fragment_path)

    def _collect_result_channels(self, channels: dict[str, ResultChannel]) -> None:
        channels.update(self._result_channels)
        for s in self._subfragments:
            if s in self._detached_subfragments:
                continue
            s._collect_result_channels(channels)

    def _get_dataset_or_set_default(self, key, default=None) -> Any:
        try:
            return self.get_dataset(key)
        except KeyError:
            if default is None:
                raise KeyError(f"Dataset '{key}' does not exist, but no " +
                               "fallback default value specified") from None
            try:
                self.set_dataset(key, default, broadcast=True, persist=True)
                logger.warning("Set dataset '%s' to default value (%s)", key, default)
            except AttributeError:
                logger.debug(
                    "Failed to set dataset '%s' to default value (%s); " +
                    "probably running in examine phase", key, default)
            return default


class ExpFragment(Fragment):
    """Fragment that supports the notion of being run to produce results."""
    def prepare(self) -> None:
        """Prepare this instance for execution
        (see ``artiq.language.environment.Experiment.prepare``).

        This is invoked only once per (sub)scan, after :meth:`Fragment.build_fragment`
        but before :meth:`.host_setup`. At this point, parameters, datasets and devices
        can be accessed, but devices must not yet be.

        For top-level scans, this can (and will) be executed in the `prepare` scheduler
        pipeline stage.

        Unless running in the `prepare` pipeline state is absolutely necessary for
        runtime performance, lazily running the requisite initialisation code in
        :meth:`host_setup` is usually preferable, as this naturally composes across the
        ndscan fragment tree.
        """

    def run_once(self) -> None:
        """Execute the experiment described by the fragment once with the current
        parameters, producing one set of results (if any)."""

    def get_default_analyses(self) -> Iterable[DefaultAnalysis]:
        """Return list of :class:`.DefaultAnalysis` instances describing analyses
        (fits, etc.) for this fragment.

        Analyses are only run if the fragment is scanned along the axes required for
        them to apply.

        This is a class method in spirit, and might become one in the future.
        """
        return []


def _skip_common_prefix(target: list, reference: list) -> list:
    i = 0
    while i < len(target) and i < len(reference) and target[i] == reference[i]:
        i += 1
    return target[i:]


class AggregateExpFragment(ExpFragment):
    r"""Combines multiple :class:`ExpFragment`\ s by executing them one after each other
    each time :meth:`run_once` is called.

    To use, derive from the class and, in the subclass ``build_fragment()`` method
    forward to the parent implementation after constructing all the relevant fragments::

        class FooBarAggregate(AggregateExpFragment):
            def build_fragment(self):
                self.setattr_fragment("foo", FooFragment)
                self.setattr_fragment("bar", BarFragment)

                # Any number of customisations can be made as usual,
                # e.g. to provide a convenient parameter to scan the
                # fragments in lockstep:
                self.setattr_param_rebind("freq", self.foo)
                self.bar.bind_param("freq", self.freq)

                # Let AggregateExpFragment default implementations
                # take care of the rest, e.g. have self.run_once()
                # call self.foo.run_once(), then self.bar.run_once().
                super().build_fragment([self.foo, self.bar])

        ScanFooBarAggregate = make_fragment_scan_exp(FooBarAggregate)
    """
    def build_fragment(self, exp_fragments: list[ExpFragment]) -> None:
        """
        :param exp_fragments: The "child" fragments to execute. The fragments will be
            run in the given order. No special treatment is given to the
            ``{host,device}_{setup,cleanup}()`` methods, which will just be executed
            through the recursive default implementations unless overridden by the user.
        """
        if not exp_fragments:
            raise ValueError("At least one child ExpFragment should be given")
        self._exp_fragments = exp_fragments

        # Since polymorphism is not supported by the ARTIQ compiler, make named
        # attributes for the exp fragments and make a _run_once_impl() helper function
        # that calls them one after each other.
        for i, exp in enumerate(exp_fragments):
            setattr(self, f"_exp_fragment_{i}", exp)
        self._run_once_impl = kernel_from_string(["self"], "\n".join(
            [f"self._exp_fragment_{i}.run_once()" for i in range(len(exp_fragments))]),
                                                 portable)

        # If the child fragment run_once() methods are @kernel, then make our run_once()
        # run on the kernel too. Reassigning the member function is a bit janky, but so
        # would it be to update the decorator, as `artiq_embedded` is an immutable tuple
        # and the type is not public.
        is_kernels = [is_kernel(e.run_once) for e in exp_fragments]
        if all(is_kernels):
            self.run_once = self._kernel_run_once
        else:
            if any(is_kernels):
                logger.warning("Mixed host/@kernel run_once() methods among passed " +
                               "ExpFragments; execution will be slow as the " +
                               "kernel(s) will be recompiled for each scan point.")

    def prepare(self) -> None:
        ""
        for exp in self._exp_fragments:
            exp.prepare()

    def run_once(self) -> None:
        """Execute the experiment by forwarding to each child fragment.

        Invokes all child fragments in the order they are passed to
        :meth:`build_fragment`. This method can be overridden if more complex behaviour
        is desired.

        If all child fragments have a ``@kernel`` ``run_once()``, this is implemented on
        the core device as well to avoid costly kernel recompilations in a scan.
        """
        return self._run_once_impl(self)

    @kernel
    def _kernel_run_once(self) -> None:
        return self._run_once_impl(self)

    def get_always_shown_params(self) -> list[ParamHandle]:
        """Collect always-shown params from each child fragment, plus any parameters
        directly defined in this fragment as usual.
        """
        result = super().get_always_shown_params()
        for exp in self._exp_fragments:
            result += exp.get_always_shown_params()
        return result

    def get_default_analyses(self) -> Iterable[DefaultAnalysis]:
        """Collect default analyses from each child fragment.

        The analyses are wrapped in a proxy that prepends any result channel names with
        the fragment path to ensure results from different analyses do not collide.
        """
        analyses = []
        for exp in self._exp_fragments:
            exp_analyses = exp.get_default_analyses()
            prefix = "_".join(
                _skip_common_prefix(exp._fragment_path, self._fragment_path)) + "_"
            analyses += [
                ResultPrefixAnalysisWrapper(analysis, prefix)
                for analysis in exp_analyses
            ]
        return analyses


class TransitoryError(Exception):
    r"""Transitory error encountered while executing a fragment, which is expected to
    clear itself up if it is attempted again without any further changes.

    Such errors are never raised by the ndscan infrastructure itself, but can be thrown
    from user fragment implementations in response to e.g. some temporary hardware
    conditions such as a momentarily insufficient level of laser power.

    :mod:`ndscan.experiment.entry_point`\ s will attempt to handle transitory errors,
    e.g. by retrying execution some amount of times. If fragments are manually executed
    from user code, it will often be appropriate to do this as well, unless the user
    code base does not use transitory errors at all.
    """


class RestartKernelTransitoryError(TransitoryError):
    """:class:`.TransitoryError` where, as part of recovering from it, the kernel should
    be restarted before retrying.

    This can be used for cases where remedying the error requires
    :meth:`.Fragment.host_setup()` to be run again, such as for cases where the
    experiments needs to yield back to the scheduler (e.g. for an ion loss event to be
    remedied by a second reloading experiment).
    """

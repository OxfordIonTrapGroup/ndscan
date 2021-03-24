from artiq.language import *
from collections import OrderedDict
from copy import deepcopy
import logging
from typing import Any, Dict, List, Iterable, Type, Tuple, Union

from .default_analysis import DefaultAnalysis
from .parameters import ParamHandle, ParamStore
from .result_channels import ResultChannel, FloatChannel
from .utils import path_matches_spec
from ..utils import strip_prefix

__all__ = ["Fragment", "ExpFragment", "TransitoryError", "RestartKernelTransitoryError"]

logger = logging.getLogger(__name__)


class Fragment(HasEnvironment):
    """Main building block."""
    def build(self, fragment_path: List[str], *args, **kwargs):
        """Initialise this fragment instance; called from the ``HasEnvironment``
        constructor.

        This sets up the machinery for registering parameters and result channels with
        the fragment tree, and then calls :meth:`build_fragment` to actually perform the
        fragment-specific setup. This method should not typically be overwritten.

        :params fragment_path: Full path of the fragment, as a list starting from the
            root. For instance, ``[]`` for the top-level fragment, or ``["foo", "bar"]``
            for a subfragment created by ``setattr_fragment("bar", …)`` in a fragment
            created by ``setattr_fragment("foo", …)``.
        :params args: Arguments to be forwarded to :meth:`build_fragment`.
        :params kwargs: Keyword arguments to be forwarded to :meth:`build_fragment`.
        """
        self._fragment_path = fragment_path
        self._subfragments = []

        #: Maps names of non-overridden parameters of this fragment (i.e., matching the
        #: attribute names of the respective ParamHandles) to *Param instances.
        self._free_params = OrderedDict()

        #: Maps own attribute name to the ParamHandles of the rebound parameters in
        #: their original subfragment (currently always only one, as there is only a
        #: rebinding API that targets single paths).
        self._rebound_subfragment_params = dict()

        #: List of (param, store) tuples of parameters set to their defaults after
        #: init_params().
        self._default_params = []

        #: Maps full path of own result channels to ResultChannel instances.
        self._result_channels = {}

        #: Subfragments the ResultChannels of which should not be re-exported (e.g.
        #: for subscans).
        self._absorbed_results_subfragments = set()

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
        self._device_setup_subfragments_impl = kernel_from_string(["self"], "\n".join([
            "self.{}.device_setup()".format(s._fragment_path[-1])
            for s in self._subfragments
        ]) or "pass", portable)

        code = ""
        for s in self._subfragments[::-1]:
            frag = "self." + s._fragment_path[-1]
            code += "try:\n"
            code += "    {}.device_cleanup()\n".format(frag)
            code += "except Exception:\n"
            code += "    logger.error(\"Cleanup failed for '{}'.\")\n".format(
                s._stringize_path())
        self._device_cleanup_subfragments_impl = kernel_from_string(
            ["self", "logger"], code[:-1] if code else "pass", portable)

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
        self._device_cleanup_subfragments_impl(self, logger)

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

    def setattr_fragment(self, name: str, fragment_class: Type["Fragment"], *args,
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
        assert not hasattr(self, name), "Field '{}' already exists".format(name)

        frag = fragment_class(self, self._fragment_path + [name], *args, **kwargs)
        self._subfragments.append(frag)
        setattr(self, name, frag)

        return frag

    def setattr_param(self, name: str, param_class: Type, description: str, *args,
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
        assert not hasattr(self, name), "Field '{}' already exists".format(name)

        fqn = self.fqn + "." + name
        self._free_params[name] = param_class(fqn, description, *args, **kwargs)

        handle = param_class.HandleType(self, name)
        setattr(self, name, handle)
        return handle

    def setattr_param_rebind(self,
                             name: str,
                             original_owner: "Fragment",
                             original_name: Union[str, None] = None,
                             **kwargs) -> ParamHandle:
        """Create a parameter that overrides the value of a subfragment parameter.

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
        assert (self._building
                ), "Can only call setattr_param_rebind() during build_fragment()"
        assert name.isidentifier(), "Parameter name must be valid Python identifier"
        assert not hasattr(self, name), "Field '{}' already exists".format(name)

        if original_name is None:
            original_name = name
        assert hasattr(original_owner, original_name), \
            "Original owner does not have a field of name '{}'".format(original_name)
        assert original_name in original_owner._free_params, (
            "Field '{}' is not a free parameter of original owner; "
            "already rebound?".format(original_name))

        # Set up our own copy of the parameter.
        original_param = original_owner._free_params[original_name]
        param = deepcopy(original_param)
        param.fqn = self.fqn + "." + name
        for k, v in kwargs.items():
            setattr(param, k, v)
        self._free_params[name] = param
        handle = param.HandleType(self, name)
        setattr(self, name, handle)

        # Deregister it from the original owner and make sure we set the store
        # to our own later.
        original_handles = original_owner._get_all_handles_for_param(original_name)
        del original_owner._free_params[original_name]
        assert name not in self._rebound_subfragment_params
        self._rebound_subfragment_params[name] = original_handles

        return handle

    def setattr_result(self,
                       name: str,
                       channel_class: Type[ResultChannel] = FloatChannel,
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
        assert not hasattr(self, name), "Field '{}' already exists".format(name)
        self._result_channels[path] = channel
        setattr(self, name, channel)

    def override_param(self,
                       param_name: str,
                       initial_value: Any = None) -> Tuple[Any, ParamStore]:
        """Override the parameter with the given name and set it to the provided value.

        :param param_name: The name of the parameter.
        :param initial_value: The initial value for the parameter. If ``None``, the
            default from the parameter schema is used.

        :return: A tuple ``(param, store)`` of the parameter metadata and the newly
            created :class:`.ParamStore` instance that the parameter handles are now
            bound to.
        """
        param = self._free_params.get(param_name, None)
        assert param is not None, "Not a free parameter: '{}'".format(param_name)
        del self._free_params[param_name]

        if initial_value is None:
            initial_value = param.eval_default(self._get_dataset_or_set_default)
        store = param.make_store((param.fqn, self._stringize_path()), initial_value)
        for handle in self._get_all_handles_for_param(param_name):
            handle.set_store(store)
        return param, store

    def _collect_params(self, params: Dict[str, List[str]],
                        schemata: Dict[str, dict]) -> None:
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

    def init_params(self,
                    overrides: Dict[str, List[Tuple[str, ParamStore]]] = {}) -> None:
        """Initialise free parameters of this fragment and all its subfragments.

        If, for a given parameter, a relevant override is given, the specified
        ParamStore is used. Otherwise, the default value is evaluated and a new store
        pointing to it created.

        This method should be called before any of the fragment's user-defined functions
        are used (but after the constructor -> :meth:`build` -> :meth`build_fragment()`
        has completed). Most likely, the top-level fragment will be called from a
        :mod:`ndscan.experiment.entry_point` which already take care of this. In cases
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
        to the existing parameter store.
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

    def get_always_shown_params(self) -> List[ParamHandle]:
        """Return handles of parameters to always show in user interfaces when this
        fragment is the root of the fragment tree (vs. other parameters for which
        overrides need to be explicitly added).

        This can be overridden in fragment implementations to customise the user
        interface; by default, all free parameters are shown in the order they were
        created in :meth:`build_fragment`.

        Example::

            class MyFragment(Fragment):
                def build_fragment(self):
                    setattr_fragment("child", MyChildFragment)
                    setattr_param("foo", ...)
                    setattr_param("bar", ...)
                    setattr_param("baz", ...)

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

    def _get_all_handles_for_param(self, name: str) -> List[ParamHandle]:
        return [getattr(self, name)] + self._rebound_subfragment_params.get(name, [])

    def _stringize_path(self) -> str:
        return "/".join(self._fragment_path)

    def _collect_result_channels(self, channels: Dict[str, ResultChannel]) -> None:
        channels.update(self._result_channels)
        for s in self._subfragments:
            if s in self._absorbed_results_subfragments:
                continue
            s._collect_result_channels(channels)

    def _get_dataset_or_set_default(self, key, default) -> Any:
        try:
            try:
                return self.get_dataset(key)
            except KeyError:
                logger.warning("Setting dataset '%s' to default value (%s)", key,
                               default)
                self.set_dataset(key, default, broadcast=True, persist=True)
                return default
        except Exception as e:
            # FIXME: This currently occurs when build()ing experiments with dataset
            # defaults from within an examine worker, i.e. when scanning the repository
            # or recomputing arguments, because datasets can't be accessed there. We
            # should probably silently ignore missing datasets there, and set them
            # accordingly when the experiment is actually run.
            logger.warning("Unexpected error evaluating dataset default: %s", e)
            return default


class ExpFragment(Fragment):
    """Fragment that supports the notion of being run to produce results."""
    def prepare(self) -> None:
        """Prepare this instance for execution
        (see ``artiq.language.environment.Experiment.prepare``).

        This is invoked only once per (sub)scan, after :meth:`.build_fragment` but
        before :meth:`.host_setup`. At this point, parameters, datasets and devices
        be accessed, but devices must not yet be.

        For top-level scans, this can (and will) be executed in the `prepare` scheduler
        pipeline stage.
        """
        pass

    def run_once(self) -> None:
        """Execute the experiment described by the fragment once with the current
        parameters, producing one set of results (if any)."""
        pass

    def get_default_analyses(self) -> Iterable[DefaultAnalysis]:
        """Return list of :class:`.DefaultAnalysis` instances describing analyses
        (fits, etc.) for this fragment.

        Analyses are only run if the fragment is scanned along the axes required for
        them to apply.

        This is a class method in spirit, and might become one in the future.
        """
        return []


class TransitoryError(Exception):
    """Transitory error encountered while executing a fragment, which is expected to
    clear itself up if it is attempted again without any further changes.
    """
    pass


class RestartKernelTransitoryError(TransitoryError):
    """Transitory error where the kernel should be restarted before retrying."""
    pass

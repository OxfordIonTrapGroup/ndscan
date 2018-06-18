from artiq.language import *
from artiq.protocols import pyon
from contextlib import suppress
from typing import Callable, Dict, List, Type
from .fragment import Fragment, ExpFragment


# We don't want to export FragmentScanExperiment to hide it from experiment
# class discovery.
__all__ = ["make_fragment_scan_exp", "PARAMS_ARG_KEY"]

PARAMS_ARG_KEY = "ndscan_params"


class ScanSpec:
    def __init__(self, dims):
        self.dims = []

    def is_continuous(self) -> bool:
        return not self.dims


class FragmentScanExperiment(EnvExperiment):
    argument_ui = "ndscan"

    def build(self, fragment_init: Callable[[], ExpFragment]):
        self.setattr_device("core")
        self.setattr_device("scheduler")

        self.fragment = fragment_init()

        desc = {
            "schema": self.fragment._build_param_schema(),
            "always_shown_params": self.fragment._get_always_shown_params()
        }
        params = self.get_argument(PARAMS_ARG_KEY, PYONValue(default=desc))

        overrides = {}
        if params:
            overrides = params.get("overrides", {})
        self.fragment._apply_param_overrides(overrides)

    def prepare(self):
        assert self.fragment, "Fragment to scan not set"

        # Validate things, etc.
        # Set up scan spec from arguments.
        self._scan = ScanSpec([])

    def run(self):
        if self._scan.is_continuous():
            self._run_continuous()
        else:
            self._run_scan()

    def analyze(self):
        # See whether there are any default fits set up for the chosen
        # parameter(s), otherwise call:
        self.fragment.analyze()

    def _run_continuous(self):
        # Init things.

        # Issue CBC for applet display.
        with suppress(TerminationRequested):
            while True:
                self.fragment.host_setup()
                self._krun_continuous()
                self.core.comm.close()
                self.scheduler.pause()

    @kernel
    def _krun_continuous(self):
        self.core.reset()

        first = True
        while not self.scheduler.check_pause():
            if first:
                self.fragment.device_setup()
                first = False
            else:
                self.fragment.device_reset([])
            self.fragment.run_once()
            # TODO: Broadcast result channels, or is this done explicitly before?

    def _run_scan(self):
        # Setup machinery.

        # Issue CBC for applet display.

        # For each scan level, …

        pass

    def _set_completed(self):
        # Set completion marker dataset.
        pass


def make_fragment_scan_exp(fragment_class: Type[ExpFragment]):
    class FragmentScanShim(FragmentScanExperiment):
        def build(self):
            super().build(lambda: fragment_class(self, []))

    doc = fragment_class.__doc__
    if not doc:
        doc = fragment_class.__name__
    FragmentScanShim.__doc__ = __doc__

    return FragmentScanShim
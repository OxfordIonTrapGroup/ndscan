from artiq.language import *

# TODO: Have a single user-facing module similar to artiq.language,
# possibly exposing all of artiq.language as well for convenience.
from ndscan.experiment import *
from ndscan.fragment import *


class Freq422(Fragment):
    def build_fragment(self):
        # These are set such that zero offset is on resonance (resp. nominal
        # amount off-resonance) for all the beams.
        self.setattr_param("dp_nominal", "422 double pass AOM nominal frequency")
        self.setattr_param("rd_nominal", "422 RD single pass AOM nominal frequency")
        self.setattr_param("dp_nominal", "422 BD single pass AOM nominal frequency")
        self.setattr_param("sigma_nominal", "422 sigma single pass AOM nominal frequency")

        # Used for global scanning to compensate cavity drifts. If we develop a
        # constant offset we don't want to fix (e.g. balancing versus blade
        # trap), this should be applied to nominal_dp_freq instead.
        self.setattr_param("offset", "422 master offset frequency (optical)")

        # DESIGN NOTE: Need to use setattr_* for parameters (i.e. including a way
        # for Fragment internals to keep track of where they go) to be able to
        # update them for scans.

    @portable
    def get_dp_freq(self, offset):
        return self.dp_nominal + (self.offset + offset) / 2

    @portable
    def get_rd_freq(self, offset):
        return self.rd_nominal + offset


class DopplerCooling(Fragment):
    def build_fragment(self):
        self.setattr_fragment("freq_422", Freq422)

    def device_setup(self):
        # Setup profiles, compute frequencies.
        pass

    @kernel
    def leave_on(self):
        # on() the given pulses
        pass


class EITCooling(Fragment):
    def build_fragment(self):
        self.setattr_fragment("freq_422", Freq422)

    def device_setup(self):
        # Setup profiles, compute frequencies.
        pass


class Cooling(Fragment):
    def build_fragment(self):
        self.setattr_fragment("doppler", DopplerCooling)
        self.setattr_fragment("eit", EITCooling)
        self.setattr_param("use_eit", "Use EIT cooling")

    def device_setup(self):
        self.doppler.init()
        if self.use_eit:
            self.eit.init()

    def cool(self):
        self.doppler.cool()
        if self.use_eit:
            self.eit.cool()

    def leave_on(self):
        self.doppler.leave_on()


class Readout(Fragment):
    def build_fragment(self):
        self.setattr_param("freq_offset_422_pi", "Readout 422 pi offset")
        self.setattr_param("freq_offset_422_sigma", "Readout 422 sigma offset")
        self.setattr_param("duration", "Readout duration")

        # TODO: Take number of shots as parameter, to be re-bound by SingleIonExp.

        self.setattr_result("counts", "Counts")
        self.setattr_result("p")
        self.setattr_result("p_err", display_hints={"error_bar_for": "p"})

    def device_setup(self):
        # Compute frequencies, setup profiles.
        pass

    @kernel
    def collect(self):
        # Actually turn on lasers and count photons.
        pass

    @kernel
    def finish_point(self):
        # Call host to update derived result channels.
        pass


class StatePrep(Fragment):
    def build_fragment(self):
        self.setattr_param("freq_offset_422", "State prep 422 sigma offset")
        self.setattr_param("duration", "State prep duration")

    def device_setup(self):
        # Compute frequencies, setup profiles.
        pass

    @kernel
    def do(self):
        # Do the state prep thing.
        pass


class SingleIonExp(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("cooling", Cooling)
        self.setattr_fragment("readout", Readout)
        self.setattr_fragment("state_prep", StatePrep)
        self.setattr_param("num_shots", "Number of shots")

        self.setattr_device("core")

    def run_once(self):
        self.readout.init()
        for _ in self.num_shots:
            self.core.break_realtime()
            self.cooling.cool()
            self.state_prep.do()
            self.run_shot()
            self.readout.collect()
        self.cooling.leave_on()
        self.readout.finish_point()

    @kernel
    def run_shot(self):
        pass


class Flop674(SingleIonExp):
    def build_fragment(self):
        super().build_fragment()
        self.setattr_param("freq_offset_674", "674 frequency offset")
        self.setattr_param("t_674", "674 duration")

        #self.setattr_device("ttl_674")

    def device_setup(self):
        # Set up profiles.
        pass

    def run_shot(self):
        self.ttl_674.pulse(self.t_674)


ScanFlop674 = make_fragment_scan_exp(Flop674)

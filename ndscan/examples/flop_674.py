from artiq.language import *

# TODO: Have a single user-facing module similar to artiq.language,
# possibly exposing all of artiq.language as well for convenience.
from ndscan.experiment import *
from ndscan.fragment import *


class Freq422(Fragment):
    def build_fragment(self):
        # These are set such that zero offset is on resonance (resp. nominal
        # amount off-resonance) for all the beams.
        self.setattr_param("dp_nominal", FloatParam, "422 double pass AOM nominal frequency", "dataset('sr.freq_422.dp_nominal', 219e6)")
        # self.setattr_param("rd_nominal", FloatParam, "422 RD single pass AOM nominal frequency")
        # self.setattr_param("sp_nominal", FloatParam, "422 BD single pass AOM nominal frequency")
        # self.setattr_param("sigma_nominal", FloatParam, "422 sigma single pass AOM nominal frequency")

        # Used for global scanning to compensate cavity drifts. If we develop a
        # constant offset we don't want to fix (e.g. balancing versus blade
        # trap), this should be applied to nominal_dp_freq instead.
        self.setattr_param("offset", FloatParam, "422 master offset frequency (optical)", "dataset('sr.freq_422.offset', 0.0)")

    @portable
    def get_dp_freq(self, offset=0.0):
        return self.dp_nominal + (self.offset + offset) / 2

    @portable
    def get_rd_freq(self, offset=0.0):
        return self.rd_nominal + offset


class DopplerCooling(Fragment):
    def build_fragment(self):
        self.setattr_fragment("freq_422", Freq422)

    @kernel
    def device_setup(self):
        # Setup profiles, compute frequencies.
        pass

    @kernel
    def cool(self):
        pass

    @kernel
    def leave_on(self):
        # on() the given pulses
        pass


class EITCooling(Fragment):
    def build_fragment(self):
        self.setattr_fragment("freq_422", Freq422)

    @kernel
    def device_setup(self):
        # Setup profiles, compute frequencies.
        pass

    @kernel
    def cool(self):
        pass


class Cooling(Fragment):
    def build_fragment(self):
        self.setattr_fragment("doppler", DopplerCooling)
        self.setattr_fragment("eit", EITCooling)
        self.setattr_param("use_eit", IntParam, "Use EIT cooling", 0)

    @kernel
    def device_setup(self):
        self.doppler.device_setup()
        if self.use_eit.get():
            self.eit.device_setup()

    @kernel
    def cool(self):
        self.doppler.cool()
        if self.use_eit.get():
            self.eit.cool()

    @kernel
    def leave_on(self):
        self.doppler.leave_on()


class Readout(Fragment):
    def build_fragment(self):
        self.setattr_device("core")

        # self.setattr_param("freq_offset_422_pi", FloatParam, "Readout 422 pi offset")
        # self.setattr_param("freq_offset_422_sigma", FloatParam, "Readout 422 sigma offset")
        self.setattr_param("duration", FloatParam, "Readout duration", "dataset('sr.readout.duration', 200e-6)")

        # TODO: Take number of shots as parameter, to be re-bound by SingleIonExp.

        self.setattr_result("counts", OpaqueChannel, "Counts")
        self.setattr_result("p")
        self.setattr_result("p_err", display_hints={"error_bar_for": "p"})

    @kernel
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
        # self.setattr_param("freq_offset_422", FloatParam, "State prep 422 sigma offset")
        # self.setattr_param("duration", FloatParam, "State prep duration")
        pass

    @kernel
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
        self.setattr_param("num_shots", IntParam, "Number of shots", 100)

        self.setattr_device("core")

    @kernel
    def run_once(self):
        for _ in range(self.num_shots.get()):
            self.core.break_realtime()
            self.cooling.cool()
            self.state_prep.do()
            self.run_shot()
            self.readout.collect()
        self.cooling.leave_on()
        self.readout.finish_point()


class Flop674(SingleIonExp):
    def build_fragment(self):
        super().build_fragment()
        self.setattr_param("freq_offset_674", FloatParam, "674 frequency offset", 0.0)
        self.setattr_param("t_674", FloatParam, "674 duration", 10e-6)

        #self.setattr_device("ttl_674")

    @kernel
    def device_setup(self):
        # Set up profiles.
        pass

    @kernel
    def run_shot(self):
        print(self.t_674.get())
        # self.ttl_674.pulse(self.t_674)


ScanFlop674 = make_fragment_scan_exp(Flop674)

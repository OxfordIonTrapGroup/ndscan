from artiq.language import *
from ndscan.experiment import make_fragment_scan_exp
from ndscan.fragment import ExpFragment
from ndscan.scan_generator import LinearGenerator
from ndscan.subscan import setattr_subscan
from sim_rabi_flop import RabiFlopSim


class PiTimeFitSim(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("flop", RabiFlopSim)
        setattr_subscan(self, "scan", self.flop, [(self.flop, "duration")])

    def run_once(self):
        coords, points = self.scan.run([(self.flop.duration,
                                         LinearGenerator(0, 1e-6, 3, True))])
        print("durations:", coords[self.flop.duration])
        print("ion states:", points[self.flop.readout.p])


ScanSimPiTimeFit = make_fragment_scan_exp(PiTimeFitSim)

from ndscan.experiment import *
import oitg.fitting
from rabi_flop import RabiFlopSim


class PiTimeFitSim(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("flop", RabiFlopSim)
        setattr_subscan(self, "scan", self.flop, [(self.flop, "duration")])
        self.setattr_result("t_pi", unit="us")
        self.setattr_result("t_pi_err", display_hints={"error_bar_for": self.t_pi.path})

    def run_once(self):
        coords, points = self.scan.run([(self.flop.duration,
                                         LinearGenerator(0, 1e-6, 31, True))])
        x = coords[self.flop.duration]
        y = points[self.flop.readout.p]
        y_err = points[self.flop.readout.p_err]
        fit_results, fit_errs = oitg.fitting.rabi_flop.fit(x, y, y_err)
        self.t_pi.push(fit_results["t_pi"])
        self.t_pi_err.push(fit_errs["t_pi"])


ScanSimPiTimeFit = make_fragment_scan_exp(PiTimeFitSim)

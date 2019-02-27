from ndscan.experiment import *
from ndscan.default_analysis import *
from rabi_flop import RabiFlopSim


class RabiFlopWithAnalysis(RabiFlopSim):
    """Rabi flop example with a custom default analysis and fit procedure

    (Usually, get_default_analyses() would directly be defined in the respective
    ExpFragment.)
    """

    def get_default_analyses(self):
        return [CustomAnalysis({"t": self.duration}, self._analyse_time_scan)]

    def _analyse_time_scan(self, axis_values, result_values):
        x = axis_values["t"]
        y = result_values[self.readout.p]
        y_err = result_values[self.readout.p_err]
        fit_results, fit_errs, fit_xs, fit_ys, = oitg.fitting.rabi_flop.fit(
            x, y, y_err, evaluate_function=True)
        return [
            Annotation("location", {self.duration: fit_results["t_pi"]}),
            Annotation("curve", {
                self.duration: fit_xs.tolist(),
                self.readout.p: fit_ys.tolist()
            })
        ]


RabiFlopWithAnalysisScan = make_fragment_scan_exp(RabiFlopWithAnalysis)

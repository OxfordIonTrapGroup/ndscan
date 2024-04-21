"""
Shows how a simple experiment can be extended with custom fitting code, and used as a
subscan from other fragments.
"""
from ndscan.experiment import *
import oitg.fitting
from rabi_flop import RabiFlopSim


class RabiFlopWithAnalysis(RabiFlopSim):
    """Rabi flop example, extended by a custom default analysis and fit procedure

    (Usually, get_default_analyses() would directly be defined in the respective
    ExpFragment; we just extend RabiFlopSim here to avoid code duplication while keeping
    the other example simple.)
    """
    def get_default_analyses(self):
        return [
            CustomAnalysis([self.duration], self._analyse_time_scan, [
                OpaqueChannel("fit_xs"),
                OpaqueChannel("fit_ys"),
                FloatChannel("t_pi", "Fitted π time", unit="us"),
                FloatChannel("t_pi_err", "Fitted π time error", unit="us")
            ])
        ]

    def _analyse_time_scan(self, axis_values, result_values, analysis_results):
        x = axis_values[self.duration]
        y = result_values[self.readout.p]
        y_err = result_values[self.readout.p_err]

        fit_results, fit_errs, fit_xs, fit_ys = oitg.fitting.sinusoid.fit(
            x, y, y_err, evaluate_function=True, evaluate_n=100)

        analysis_results["t_pi"].push(fit_results["t_pi"])
        analysis_results["t_pi_err"].push(fit_errs["t_pi"])
        analysis_results["fit_xs"].push(fit_xs)
        analysis_results["fit_ys"].push(fit_ys)

        # We can also return custom annotations to be displayed, which can make use of
        # the analysis results.
        return [
            annotations.axis_location(axis=self.duration,
                                      position=analysis_results["t_pi"],
                                      position_error=analysis_results["t_pi_err"]),
            annotations.curve_1d(x_axis=self.duration,
                                 x_values=analysis_results["fit_xs"],
                                 y_axis=self.readout.p,
                                 y_values=analysis_results["fit_ys"])
        ]


RabiFlopWithAnalysisScan = make_fragment_scan_exp(RabiFlopWithAnalysis)


class PiTimeFitSim(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("flop", RabiFlopWithAnalysis)
        self.setattr_param("max_duration",
                           FloatParam,
                           "Maximum pulse duration",
                           unit="us",
                           default=1 * us)
        self.setattr_param("num_points", IntParam, "Number of points", default=31)

        # With expose_analysis_results == True (the default), setattr_subscan() creates
        # results channels in this fragment that contain the analysis results from the
        # subscan (e.g. t_pi).
        setattr_subscan(self,
                        "scan",
                        self.flop, [(self.flop, "duration")],
                        expose_analysis_results=True)

    def run_once(self):
        self.scan.run([(self.flop.duration,
                        LinearGenerator(0, self.max_duration.get(),
                                        self.num_points.get(), True))])


PiTimeFitSimScan = make_fragment_scan_exp(PiTimeFitSim)

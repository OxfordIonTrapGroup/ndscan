from artiq.language import *
from ndscan.experiment import *
from ndscan.fragment import *
import random
import numpy as np
import time


class Readout(Fragment):
    def build_fragment(self):
        self.setattr_param("num_shots", IntParam, "Number of shots", 100)
        self.setattr_param("mean_0", FloatParam, "Dark counts over readout duration",
                           0.1)
        self.setattr_param("mean_1", FloatParam, "Bright counts over readout duration",
                           20.0)
        self.setattr_param("threshold", IntParam, "Threshold", 5)

        self.setattr_result("counts", OpaqueChannel)
        self.setattr_result("p")
        self.setattr_result("p_err", display_hints={"error_bar_for": "p"})

        self.setattr_result("z")
        self.setattr_result("z_err", display_hints={"error_bar_for": "z"})

        self.setattr_result("half", display_hints={"priority": -1})

    def simulate_shots(self, p):
        num_shots = self.num_shots.get()

        counts = np.empty(num_shots, dtype=np.int16)
        for i in range(num_shots):
            mean = self.mean_0.get() if random.random() > p else self.mean_1.get()
            counts[i] = np.random.poisson(mean)
        self.counts.push(counts)

        p = 0.0
        for c in counts:
            if c >= self.threshold.get():
                p += 1.0
        p /= num_shots

        p_err = np.sqrt(p * (1 - p) / num_shots)

        self.p.push(p)
        self.p_err.push(p_err)

        self.z.push(2 * p - 1)
        self.z_err.push(2 * p_err)

        self.half.push(np.random.normal(0.5, 0.05))


class RabiFlopSim(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("readout", Readout)

        self.setattr_param(
            "rabi_freq", FloatParam, "Rabi frequency", 1.0 * MHz, unit="MHz")
        self.setattr_param(
            "duration", FloatParam, "Pulse duration", 0.5 * us, unit="us")
        self.setattr_param("detuning", FloatParam, "Detuning", 0.0 * MHz, unit="MHz")

    def run_once(self):
        omega0 = 2 * np.pi * self.rabi_freq.get()
        delta = 2 * np.pi * self.detuning.get()
        omega = np.sqrt(omega0**2 + delta**2)
        p = (omega0 / omega * np.sin(omega / 2 * self.duration.get()))**2
        self.readout.simulate_shots(p)
        time.sleep(0.1)


ScanRabiFlopSim = make_fragment_scan_exp(RabiFlopSim)

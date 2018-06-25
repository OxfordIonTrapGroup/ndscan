from artiq.language import *
from ndscan.experiment import *
from ndscan.fragment import *

import random
import numpy as np


class Readout(Fragment):
    def build_fragment(self):
        self.setattr_param("num_shots", Int16Param, "Number of shots", 100)
        self.setattr_param("mean_0", Float64Param, "Dark counts over readout duration", 0.1)
        self.setattr_param("mean_1", Float64Param, "Bright counts over readout duration", 20.0)
        self.setattr_param("threshold", Int16Param, "Threshold", 5)

        self.setattr_result("counts") # FIXME: Type
        self.setattr_result("p")
        self.setattr_result("p_err", display_hints={"error_bar_for": "p"})

    def simulate_shots(self, p):
        num_shots = self.num_shots.get()

        counts = np.empty(num_shots, dtype=np.int16)
        for i in range(num_shots):
            if random.random() > p:
                counts[i] = np.random.poisson(self.mean_0.get())
            else:
                counts[i] = np.random.poisson(self.mean_1.get())
        self.counts.set(counts)

        p = 0.0
        for c in counts:
            if c >= self.threshold.get():
                p += 1.0
        p /= num_shots

        self.p.set(p)
        self.p_err.set(np.sqrt(p * (1 - p) / num_shots))


class RabiFlopSim(ExpFragment):
    def build_fragment(self):
        self.setattr_fragment("readout", Readout)

        self.setattr_param("rabi_freq", Float64Param, "Rabi frequency", 1.0)
        self.setattr_param("duration", Float64Param, "Pulse duration", 0.5)
        self.setattr_param("detuning", Float64Param, "Detuning", 0.0)

    def run_once(self):
        omega0 = 2 * np.pi * self.rabi_freq.get()
        delta = 2 * np.pi * self.detuning.get()
        omega = np.sqrt(omega0**2 + delta**2)
        p = (omega0 / omega * np.sin(omega / 2 * self.duration.get()))**2
        self.readout.simulate_shots(p)


ScanRabiFlopSim = make_fragment_scan_exp(RabiFlopSim)

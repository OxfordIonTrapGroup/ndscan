ndscan
======

[![Documentation](https://readthedocs.org/projects/pip/badge/?version=latest&style=flat)][1]

This is a framework for composing complex experiments from modular building
blocks in the [ARTIQ](https://github.com/m-labs/artiq) laboratory control
system, with particular support for flexible n-dimensional scans.

`ndscan` was originally developed by David Nadlinger for use in the Oxford
[Ion Trap Quantum Computing Group](https://www.physics.ox.ac.uk/research/ion-trap-quantum-computing-group),
but is expected to be useful in any laboratory environment where complex
experiments are actively built and debugged.


Getting started
---------------

To get started with `ndscan`, first prepare a Python 3.5+ environment with
ARTIQ as usual (Conda or some form of virtualenv is recommended). While the
intention is for ndscan to be a pure add-on to ARTIQ, some required patches
have not made their way into the official (m-labs) upstream repository yet.
For now, use the
[ion-trap/master](http://gitlab.physics.ox.ac.uk/ion-trap/artiq/) 
branch; if you are an external user, you can find the necessary commits at
[dnadlinger/artiq@ndscan](https://github.com/dnadlinger/artiq/tree/ndscan).

Once your environment is set up, install this package. For example, to use
ndscan directly from the Git checkout:

    (artiq) $ python setup.py develop

If you haven't already, also install the
[oitg](http://github.com/OxfordIonTrapGroup/oitg) package.

You are then ready to run the sample experiments. Start the ARITQ master
process as usual

    (artiq) $ artiq_master

and launch the dashboard with the ndscan plugin loaded

    (artiq) $ artiq_dashboard -p ndscan.dashboard_plugin

Now, navigate to the `examples/sim_rabi_flop.py` file in the experiment
explorer, and you should be able to play around with the scan interface.
(Applets are created automatically; you might want to set the CCB mode to
"Create and enable/disable applets" in the applets dock.)


Developer notes
---------------

 - Format _all_ code using [YAPF](https://github.com/google/yapf), and make
   sure `flake8 ndscan` passes. Configuration files for both are included, and
   are picked up automatically. It is just not worth spending time to obsess
   or argue about formatting details.

 - `conda` contains a very rudimentary [Conda](https://conda.io/en/latest/)
   package definition for integration with the continuous integration pipeline
   internal to the Ion Trap Quantum Computing group. The package is not 
   currently published on a public Conda channel, and likely won't ever be.


[1]: https://ndscan.readthedocs.io/en/latest/ "Read the Docs build status"

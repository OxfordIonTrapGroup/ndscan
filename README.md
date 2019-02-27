ndscan – N-dimensional scans for ARTIQ
======================================

`ndscan` extends the [ARTIQ](https://github.com/m-labs/artiq) real-time
laboratory control system with the concept of _fragments_ – building blocks that
accept parameters and produce result data – and tools for productively working
with experiments consisting of many such parts. In particular, `ndscan` allows
users to easily override parameters from anywhere in the system on the fly, and
to acquire data by iterating over (“scanning”) any number of them at a time.

See the [documentation](https://oxfordiontrapgroup.github.io/ndscan) for more
information. 

**`ndscan` is pre-alpha software. While the system has been carefully
architected and the library is in active use within the
[Ion Trap Quantum Computing Group](https://www.physics.ox.ac.uk/research/ion-trap-quantum-computing-group),
the current implementation is very much a minimum viable product in spirit
and mostly lacks documentation and tests.**


Quickstart guide
----------------

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

Now, navigate to the `examples/flop.py` file in the experiment explorer, and
you should be able to play around with the scan interface. (Applets are created
automatically; you might want to set the CCB mode to "Create and enable/disable
applets" in the applets dock.)


Developer notes
---------------

 - Format _all_ code using [YAPF](https://github.com/google/yapf), and make
   sure `flake8 ndscan` passes. Configuration files for both are included, and
   are picked up automatically. It is just not worth spending time to obsess
   or argue about formatting details.

 - Unit tests are run using `python -m unittest -v discover test`.

 - `conda` contains a very rudimentary [Conda](https://conda.io/en/latest/)
   package definition for integration with the continuous integration pipeline
   internal to the Ion Trap Quantum Computing group. The package is not 
   currently published on a public Conda channel, and likely won't ever be.


Contact
-------

If you are using `ndscan` (or it seems potentially interesting to you), feedback
would be very much appreciated, either using the
[GitHub issue tracker](https://github.com/OxfordIonTrapGroup/ndscan/issues)
or via email at david.nadlinger@physics.ox.ac.uk.

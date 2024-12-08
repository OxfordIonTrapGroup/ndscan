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

**`ndscan` is beta-grade software. While the system has been carefully
architected and the library is in active use within the
[Ion Trap Quantum Computing Group](https://www.physics.ox.ac.uk/research/ion-trap-quantum-computing-group),
large parts of the implementation are still best considered a minimum viable
product.** If you cannot work out how a particular use case would be addressed
using the library, please do get in touch: Many common lab scenarios will already
be accommodated by the current design, but perhaps the necessary interfaces were
not publicly exposed to keep the library easy to learn and flexible in terms
of implementation during initial development. Other tasks might require
extensions to the library, but detailed design ideas for them might already
exist.


Quickstart guide
----------------

To get started with `ndscan`, first prepare a Python 3.10+ environment with
ARTIQ as usual (Nix, Poetry or some form of virtualenv is recommended).
**This development version (`master` branch) depends on ARTIQ 8+ due to
backwards-incompatible changes in ARTIQ's APIs. If you still need to use an
older ARTIQ version, please see the
[release-0.3](https://github.com/OxfordIonTrapGroup/ndscan/tree/release-0.3)
branch instead** (or be prepared to resolve some issues yourself by
selectively reverting a few changes).

Once your environment is set up, install this package. For example, to use
`ndscan` directly from the Git checkout without directly using the
[Poetry](https://python-poetry.org/) dependency manager (see below):

    (artiq) $ pip install -e .

If you haven't already, also install the
[oitg](http://github.com/OxfordIonTrapGroup/oitg) package.

You are then ready to run the sample experiments. Start the ARTIQ master
process as usual

    (artiq) $ artiq_master

and launch the dashboard with the ndscan plugin loaded

    (artiq) $ artiq_dashboard -p ndscan.dashboard_plugin

Now, navigate to the `examples/rabi_flop.py` file in the experiment explorer,
and you should be able to play around with the scan interface. (Applets are
created automatically; you might want to set the CCB mode to "Create and
enable/disable applets" in the applets dock.)

To avoid old scan results continuously accumulating in the dataset
database, run the janitor process:

    (artiq) $ ndscan_dataset_janitor

`ndscan_dataset_janitor` tracks when experiments finish and cleans up the
generated datasets after a few minutes of delay. It should typically be
started alongside `artiq_master`.


Developer notes
---------------

Please refer to the [documentation](https://oxfordiontrapgroup.github.io/ndscan)
for more details, in particular the
[coding conventions](https://oxfordiontrapgroup.github.io/ndscan/coding-conventions.html)
and
[design retrospective](https://oxfordiontrapgroup.github.io/ndscan/design-retrospective.html)
sections.

`ndscan` comes with a standard development environment, managed through
[Poetry](https://python-poetry.org/), and using the
[poe](https://poethepoet.natn.io/) task runner. To get started, first install
Poetry as described on its website, if you have not already. Then, all the
commands are readily available:

    # Activate an isolated virtualenv for ndscan
    $ poetry shell

    # First time only: install all dependencies (including developer tooling)
    (ndscan-py3.10) $ poetry install --with dev

    # Before committing, ensure that the code follows the standard format and
    # lint checks (flake8) and unit tests are passing
    (ndscan-py3.10) $ poe fmt
    (ndscan-py3.10) $ poe lint
    (ndscan-py3.10) $ poe test


Contact
-------

If you are using `ndscan` (or it seems potentially interesting to you), feedback
would be very much appreciated, either using the
[GitHub issue tracker](https://github.com/OxfordIonTrapGroup/ndscan/issues)
or via email at david.nadlinger@physics.ox.ac.uk.

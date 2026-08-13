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


Release notes
-------------

### Latest Git (unreleased; since commit f35626c)

#### New features

- **Dashboard interface**:
  - *Parameter tree dialog*: The argument editor gained a tree view of the full
    fragment/parameter hierarchy, shown in a separate dialog. Use the new button
    at the bottom of the override list or Ctrl+Alt+T to open.
  - *Enum member scans*: The argument editor now allows selecting a subset of
    enum members to be scanned.
  - *Parameter explanations*: Parameters accept a new `explanation` field for
    longer free-form descriptions, shown as tooltips in the argument editor and
    in the parameter tree. Suggested use cases are to describe the precise
    physical meaning or consequence of a parameter, or how to set/calibrate it,
    which previously often has been implicit knowledge in a lab setting.
- **Plot applet**:
  - *Dockable subscan and slice panes*: Subscans and alternate plots are now
    shown as dockable panes right next to the main scan, replacing the old
    separate-window UI. Shift+click on a subscan entry shows/hides all subscans
    at once.
  - *Time slider for scan history*: Plots now include a slider for scrubbing
    back through the point acquisition history of a scan, letting you replay how
    data came in. Slice panes follow the rolled-back state.
  - *1D slices of N-dimensional scans*: Points in 2D image plots are now
    selectable (including for subscans), and 1-dimensional slices through the
    selected point can be pulled up as additional plot panes.
  - *Keyboard navigation for point selection*: Highlighted points can be stepped
    through with cursor keys in both 1D and 2D plots.
  - *Clipboard export*: Ctrl+C copies the current plot to the clipboard, with a
    brief flash as visual confirmation.
- **Experiment code/execution**:
  - *OnlineFit run experiment-side*: `OnlineFit` analyses are now run after
    experiment/subscan completion by default, and expose their parameters/error
    estimates as analysis results (requires a recent oitg version with
    `FitBase.all_parameter_names`). Previously, they were only being run and
    displayed live in the applet on the client side. This avoids having to
    manually re-implement the online fits to e.g. save the fit results to a
    dataset in a `TopLevelRunner` calibration experiment, or to make accessible
    to parent fragments in a subscan.
- **`results` tooling**:
  - *Richer metadata in ndscan_show*: `ndscan_show` displays the experiment
    class name and any vanilla ARTIQ arguments stored in the results file.

#### Improvements

- **Dashboard interface**:
  - *Per-point repeats*: New `num_repeats_per_point` scan option to repeat each
    point a given number of times before moving on (in addition to the existing
    whole-scan repeats). This can be useful to gather statistics if moving
    between points is slow (e.g. due to waiting for external hardware to
    settle).
  - The scan options section is expanded by default (#443).
  - Layout whitespace has been reduced considerably for a more compact editor,
    consistent across platforms and ARTIQ branches (previously an
    Oxford-internal ARTIQ patch).
  - Scan parameters (ranges) can now be loaded back into the argument editor
    from previous scans.
- **Plot applet**:
  - *Y-axis label layout*: Long y-axis labels are now line-wrapped to fit the
    plot pane height, and the layout is refreshed after zooming such that tick
    values no longer intersect axis labels.
  - *Enum/categorical axes in plots*: Cursor labels now use the `EnumParam` spec
    member values consistently, "set parameter from crosshair" coerces the raw
    coordinate to the actual parameter type instead of always writing a float,
    and the accessible view range for categorical axes is restricted so you
    cannot scroll to coordinates without a defined meaning.
  - *Error bar autoscaling*: When error bars are nonsensically large (e.g. as
    sometimes resulting from bad fits), they are ignored for the purpose of plot
    autoscaling (though still at their full size when zoomed out).
- **Experiment code/execution**:
  - *Int-native scans*: Scans over `IntParams` now directly generate integer
    points; rounding is improved and refining scans terminate once the range is
    exhausted.
  - *Overrides for one-shot fragments*: `run_fragment_once()` and
    `create_and_run_fragment_once()` now accept parameter overrides, making it
    easier to run fragments with modified parameters outside a scan.
  - *Fewer redundant lifecycle calls*: `device_cleanup()` is no longer called
    after every single subscan point (only when the subscan fragment is cleaned
    up), reducing overhead in kernel subscan loops.
  - *Improved result channel lifetime errors*: Missing or duplicate `push()`
    calls per point now raise a dedicated `ResultLifecycleError` naming the
    offending channel, and are also caught in no-axis (single/time series) mode.
  - `Fragment.setattr_…()` methods validate their class argument type up front
    for clearer errors when e.g. calling `setattr_param()` with a `FloatChannel`
    type argument.
- **`results` tooling**:
  - *results.pyplot improvements*: `auto_plot()` now returns the generated
    figures for further customisation, shows error bars, and uses
    `constrained_layout` for better spacing.
- **Tooling migration**: Development tooling moved from Poetry to uv (keeping
  the poe task runner) and from flake8/yapf to Ruff. Installing via `pip
  install` is unaffected.

#### Bug fixes

- **Dashboard interface**:
  - Infinite-repeat settings are now correctly read back.
  - Fixed erroneous use of `min`/`max` as half-span limits for centred scans in
    the argument editor.
- **Plot applet**:
  - Applets now cleanly reset their state when the scan published under the
    subscribed dataset prefix is replaced by one with a different schema (e.g.
    for `TopLevelRunner` used with a constant dataset prefix), and correctly
    treat point datasets shrinking as a rewrite of the displayed data.
  - The context menu is now available even when a plot has no panes (e.g.
    rolling plots with only subscan data), and subscans are shown by default
    when a fragment has no top-level channels; submenus outside plot items now
    work.
  - Fixed point selection in 2D image plots failing due to an exact
    floating-point comparison (#472).
  - Fixed crashes when using cursor keys with no point selected.
  - Fixed incorrect neighbour-index calculation when stepping through points.
  - Subscan data channels are now resolved by path so experiments with
    multiple/nested subscans are displayed correctly.
  - Fixed several regressions around model initialisation: HDF5 loading errors
    when default annotations are present, single-point scans with subscans (both
    live and loaded from HDF5), and rolling plots the last (only saved) point in
    `ndscan_show`.
  - Fixed memory leaks/slowdowns when changing plot models, when the source-id
    label is destroyed, and when closing subscan panes.
  - Fixed loading of string data from HDF5 result files in `ndscan_show`.
- **Experiment code/execution**:
  - Fixed subscans being misconfigured after transitory-error interruptions
    (both host and kernel subscans).
  - Failed online fit executions (e.g. for fit results exposed as subscan
    analysis result channels) now push NaN values instead of leaving the results
    unset/`None`.
- **`results` tooling**:
  - `ndscan_show` console output/`ndscan_to_txt` argument dumps now handle
    (refining) centre-span scan ranges (#497) and no longer double-quote list
    scan ranges
- **Compatibility fixes**:
  - Handle both ARTIQ 8 and ARTIQ 9 (`apply_color()` API change)
  - Restore PyQt5 compatibility for now (`QShortcut` location), though this is
    no longer routinely tested.
  - NumPy 2 compatibility (`np.inf`).
  - Touch events are disabled to silence `qt.pointer.dispatch` warning spam on
    macOS.

#### Packaging notes

- sipyco is pinned to v1.9 in the default distribution to avoid the PYONv2
  incompatibility until a compatibility shim is implemented in ndscan (though it
  can be overridden using `tool.uv.override-dependencies` in client projects;
  some Oxford users are on newer versions already).
- qasync `>= 0.28`, as this fixes a horrendous Windows threading bug surfacing
  on newer (`> 3.10`) Python versions (see
  [CabbageDevelopment/qasync#128](https://github.com/CabbageDevelopment/qasync/issues/128)).


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
[uv](https://docs.astral.sh/uv/), and using the
[poe](https://poethepoet.natn.io/) task runner. To get started, install
_uv_ as described on its website, if you have not already. All the other
dependencies will be set up automatically once you create a virtual
environment:

    # Create an isolated virtualenv in .venv
    $ uv venv

    # Install all dependencies
    $ uv sync

    # Activate the venv as usual
    # . .venv/bin/activate

You can also use `uv run` to avoid having to manually create/update the venv:

    # Before committing, ensure that the code follows the standard format and
    # lint checks (flake8) and unit tests are passing
    $ uv run poe fmt
    $ uv run poe lint
    $ uv run poe test

For now, using Python 3.12 is recommended. This is because wheels (binary
builds) for the specific versions of some dependencies from the lock file are
currently not available for newer versions (at least not on macOS). In
principle, newer versions ought to work, however, and upgrading the
dependencies may be all that is required.


Contact
-------

If you are using `ndscan` (or it seems potentially interesting to you), feedback
would be very much appreciated, either using the
[GitHub issue tracker](https://github.com/OxfordIonTrapGroup/ndscan/issues)
or via email at david.nadlinger@physics.ox.ac.uk.

"""Experiment-side ``ndscan`` interface

``ndscan.experiment`` contains the code for implementing the ``ndscan`` primitives from
ARTIQ experiments (as opposed to submitting the experiments with certain parameters, or
later analysing and plotting the generated data).

The top-level module provides a single convenient way to import commonly used symbols
from experiment client code, like ``artiq.experiment`` does for upstream ARTIQ::

    # Import commonly used symbols, including all of artiq.experiment:
    from ndscan.experiment import *
"""

# The wildcard imports below aren't actually unused, as we re-export them.
# flake8: noqa: F401

import artiq.experiment
from artiq.experiment import *

from . import (default_analysis, entry_point, fragment, parameters, result_channels,
               scan_generator, subscan)
from .default_analysis import *
from .entry_point import *
from .fragment import *
from .parameters import *
from .result_channels import *
from .scan_generator import *
from .subscan import *

__all__ = []
__all__.extend(artiq.experiment.__all__)
__all__.extend(default_analysis.__all__)
__all__.extend(entry_point.__all__)
__all__.extend(fragment.__all__)
__all__.extend(parameters.__all__)
__all__.extend(result_channels.__all__)
__all__.extend(scan_generator.__all__)
__all__.extend(subscan.__all__)

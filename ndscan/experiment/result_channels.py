"""
Result handling building blocks.
"""

from artiq.language import HasEnvironment, rpc
import artiq.language.units
from typing import Any, Dict, List, Optional
from .utils import dump_json

__all__ = [
    "LastValueSink", "ArraySink", "AppendingDatasetSink", "ScalarDatasetSink",
    "ResultChannel", "NumericChannel", "FloatChannel", "IntChannel", "OpaqueChannel"
]


class ResultSink:
    """
    """
    def push(self, value: Any) -> None:
        raise NotImplementedError


class LastValueSink(ResultSink):
    """Sink that stores the last-pushed value."""
    def __init__(self):
        self.value = None

    def push(self, value: Any) -> None:
        self.value = value

    def get_last(self) -> Any:
        """Return the last-pushed value, or ``None`` if none yet."""
        return self.value


class ArraySink(ResultSink):
    """Sink that stores all pushed values in a list."""
    def __init__(self):
        self.data = []

    def push(self, value: Any) -> None:
        self.data.append(value)

    def get_all(self) -> List[Any]:
        """Return a list of all previously pushed values."""
        return self.data

    def get_last(self) -> Any:
        """Return the last-pushed value, or ``None`` if none yet."""
        return self.data[-1] if self.data else None

    def clear(self) -> None:
        """Clear the list of previously pushed values."""
        self.data = []


class AppendingDatasetSink(ResultSink, HasEnvironment):
    def build(self, key: str, broadcast: bool = True) -> None:
        """
        :param key: Dataset key to store results in. Set to an array on the first push,
            and subsequently appended to.
        :param broadcast: Whether to set the dataset in broadcast mode.
        """
        self.key = key
        self.broadcast = broadcast
        self.last_value = None

    def push(self, value: Any) -> None:
        assert value is not None
        if self.last_value is None:
            self.set_dataset(self.key, [value], broadcast=self.broadcast)
        else:
            self.append_to_dataset(self.key, value)
        self.last_value = value

    def get_last(self) -> Any:
        """Return the last pushed value (or None)."""
        return self.last_value

    def get_all(self) -> List[Any]:
        """Read back the previously pushed values from the target dataset (if any)."""
        return [] if (self.last_value is None) else self.get_dataset(self.key)


class ScalarDatasetSink(ResultSink, HasEnvironment):
    """Sink that writes pushed results to a dataset, overwriting its previous value
    if any."""
    def build(self, key: str, broadcast: bool = True) -> None:
        """
        :param key: Dataset key to write the value to.
        :param broadcast: Whether to set the dataset in broadcast mode.
        """
        self.key = key
        self.broadcast = broadcast
        self.has_pushed = False

    def push(self, value: Any) -> None:
        self.set_dataset(self.key, value, broadcast=self.broadcast)
        self.has_pushed = True

    def get_last(self) -> Any:
        """Return the last pushed value, or ``None`` if none yet."""
        return self.get_dataset(self.key) if self.has_pushed else None


class ResultChannel:
    """
    :param path: The path to the channel in the fragment tree (e.g. ``"readout/p"``).
    :param description: A human-readable name of the channel. If non-empty, will be
        preferred to the path to e.g. display in plot axis labels.
    :param display_hints: A dictionary of additional settings that can be used to
        indicate how to best display results to the user (see above):

        .. list-table::
          :header-rows: 1
          :widths: 10 20 40

          * - Key
            - Argument
            - Description
          * - ``coordinate_type``
            - String describing the coordinate type.
            - For numeric channels, describes the coordinate system for the resulting
              values, which can be used to select a more appropriate visualisation than
              the default, which corresponds to straightforward linear coordinates
              (optionally bounded if ``min``/``max`` are set). Currently implemented:
              ``cyclic``, where the values are cyclical between ``min`` and ``max``
              (e.g. a phase between 0 and 2π).
          * - ``error_bar_for``
            - Path of the linked result channel
            - Indicates that this (numeric) result channel should be used to determine
              the size of the error bars for the given other channel.
          * - ``priority``
            - Integer
            - Specifies a sort order between result channels, used e.g. to control the
              way various axes are laid out. Channels are sorted from highest to lowest
              priority (default: 0). Channels with negative priorities are not displayed
              by default unless explicitly enabled.
          * - ``share_axis_with``
            - Path of the linked result channel
            - Indicates that this result channel should be drawn on the same plot axis
              as the given other channel.
    """
    def __init__(self,
                 path: str,
                 description: str = "",
                 display_hints: Optional[Dict[str, Any]] = None,
                 save_by_default: bool = True):
        self.path = path
        self.description = description
        self.display_hints = {} if display_hints is None else display_hints
        self.save_by_default = save_by_default
        self.sink = None

    def __repr__(self) -> str:
        return "<{}@{}: {}>".format(type(self).__name__, hex(id(self)), self.path)

    def describe(self) -> Dict[str, Any]:
        """
        """
        desc = {
            "path": self.path,
            "description": self.description,
            "type": self._get_type_string()
        }

        if self.display_hints:
            desc["display_hints"] = self.display_hints
        return desc

    def is_muted(self) -> bool:
        """
        """
        # TODO: Implement muting interface?
        return self.sink is not None

    def set_sink(self, sink: ResultSink) -> None:
        """
        """
        self.sink = sink

    @rpc(flags={"async"})
    def push(self, raw_value) -> None:
        """
        """
        value = self._coerce_to_type(raw_value)
        if self.sink:
            self.sink.push(value)

    def _get_type_string(self):
        raise NotImplementedError()

    def _coerce_to_type(self, value):
        raise NotImplementedError()


class NumericChannel(ResultChannel):
    r"""Base class for :class:`ResultChannel`\ s of numerical results, with scale/unit
    semantics and optional range limits.

    :param min: Optionally, a lower limit that is not exceeded by data points (can
        be used e.g. by plotting code to determine sensible value ranges to show).
    :param max: Optionally, an upper limit that is not exceeded by data points (can
        be used e.g. by plotting code to determine sensible value ranges to show).
    :param unit: Name of the unit the results are given in (e.g. ``"ms"``, ``"kHz"``).
    :param scale: Unit scaling. If ``None``, the default scaling as per ARTIQ's unit
        handling machinery (``artiq.language.units``) is used.
    """
    def __init__(self,
                 path: str,
                 description: str = "",
                 display_hints: Optional[Dict[str, Any]] = None,
                 min=None,
                 max=None,
                 unit: str = "",
                 scale=None):
        super().__init__(path, description, display_hints)
        self.min = min
        self.max = max

        if scale is None:
            if unit == "":
                scale = 1.0
            else:
                try:
                    scale = getattr(artiq.language.units, unit)
                except AttributeError:
                    raise KeyError("Unit {} is unknown, you must specify "
                                   "the scale manually".format(unit))
        self.scale = scale
        self.unit = unit

    def describe(self) -> Dict[str, Any]:
        """"""
        result = super().describe()
        result["scale"] = self.scale
        if self.min is not None:
            result["min"] = self.min
        if self.max is not None:
            result["max"] = self.max
        if self.unit is not None:
            result["unit"] = self.unit
        return result


class FloatChannel(NumericChannel):
    """:class:`NumericChannel` that accepts floating-point results."""
    def _get_type_string(self):
        return "float"

    def _coerce_to_type(self, value):
        return float(value)


class IntChannel(NumericChannel):
    """:class:`NumericChannel` that accepts integer results."""
    def _get_type_string(self):
        return "int"

    def _coerce_to_type(self, value):
        return int(value)


class OpaqueChannel(ResultChannel):
    """:class:`ResultChannel` that stores arbitrary data, with ndscan making no attempts
    to further interpret or display it.

    As such, opaque channels can be used to store any ancillary data for scan points,
    which can later be used in custom analysis code (whether as part of a default
    analysis that runs as part of the experiment code, or when manually analysing the
    experimental data later).

    Any values pushed are just passed through to the ARTIQ dataset layer; it is up to
    the user to choose something compatibile with HDF5 and PYON.
    """
    def _get_type_string(self):
        return "opaque"

    def _coerce_to_type(self, value):
        return value


class SubscanChannel(ResultChannel):
    """Channel that stores the scan metadata for a subscan.

    Serialised as a JSON string for HDF5 compatibility.
    """
    def _get_type_string(self):
        return "subscan"

    def _coerce_to_type(self, value):
        return dump_json(value)

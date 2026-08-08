import logging
from typing import Any

import numpy as np

from . import ScanModel

logger = logging.getLogger(__name__)


class HistoryFromScanModel(ScanModel):
    """A 1-dimensional slice of an N-dimensional scan.

    Point content changes are forwarded, but the schema is static; changes to the latter
    necessitate a new model instance.
    """

    def __init__(
        self,
        parent: ScanModel,
        state: int = -1,
    ):
        """
        Model describing a prior state of a `ScanModel`

        :param parent: The parent scan model.
        :param state: State to roll back to. This is either the index of the entry
            to roll back to, or -1 for pinning to the latest state
        """

        self._parent = parent
        self._channel_schemata = self._parent.get_channel_schemata()

        super().__init__(self._parent.axes, parent.schema_revision, parent.context)

        self._sliced_data = {}

        self._parent.points_appended.connect(self._append_data)
        self._parent.points_rewritten.connect(self._rewrite_data)
        self._parent.annotations_changed.connect(self._update_annotations)
        self._parent.channel_schemata_changed.connect(
            lambda *args: self.channel_schemata_changed.emit(*args)
        )
        self._state = state
        self._rewrite_data()

        self._update_annotations(self._parent._annotations)

    def _update_annotations(self, annotations):
        self._annotation_schemata = self._parent._annotation_schemata
        self._annotations = annotations
        self.annotations_changed.emit(annotations)

    def _update_state(self, state: int) -> None:
        parent_data = self._parent.get_point_data()

        num_points = len(next(iter(parent_data.values()), []))

        if state == -1 or num_points == 0:
            sliced_data = parent_data
        elif 0 <= state < num_points:
            sliced_data = self.slice_data(parent_data, state)
        else:
            raise ValueError(
                f"Cannot rollback to index {state} "
                f"(have only {num_points} points available)"
            )
        self._state = state

        data_rewritten = False
        for name, incoming_values in sliced_data.items():
            # Check if points were appended or rewritten.
            if name in self._sliced_data:
                imax = min(len(incoming_values), len(self._sliced_data[name]))
                if not np.array_equal(
                    incoming_values[:imax], self._sliced_data[name][:imax]
                ):
                    data_rewritten = True

        self._sliced_data = sliced_data

        if data_rewritten:
            self.points_rewritten.emit(self._sliced_data)
        else:
            self.points_appended.emit(self._sliced_data)

    def _rewrite_data(self, *args) -> None:
        point_data = self._parent.get_point_data()
        num_points = len(next(iter(point_data.values()), []))

        if self._state >= num_points:
            self.set_state(-1)

        self._update_state(self._state)

    def _append_data(self, *args) -> None:
        point_data = self._parent.get_point_data()
        num_points = len(next(iter(point_data.values()), []))

        if self._state >= num_points:
            self.set_state(-1)

        if self._state == -1:
            self._update_state(self._state)

    def slice_data(
        self,
        source_data: dict[str, Any],
        state: int,
    ) -> dict[str, Any]:
        """Extract the sliced data from the parent point data.

        :param source_data: The point data from the parent model.
        :param state: The index of the target to which to slice up to.
        :return: The sliced point data.
        """

        return {axis: values[: state + 1] for axis, values in source_data.items()}

    def get_channel_schemata(self) -> dict[str, Any]:
        return self._channel_schemata

    def get_point_data(self) -> dict[str, Any]:
        return self._sliced_data

    def quit(self) -> None:
        self._parent.points_appended.disconnect(self._append_data)
        self._parent.points_rewritten.disconnect(self._rewrite_data)
        self._state.point_changed.disconnect(self._update_state)

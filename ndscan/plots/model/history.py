import logging
from typing import Any

import numpy as np

from . import ScanModel

logger = logging.getLogger(__name__)


class HistoryFromScanModel(ScanModel):
    """Restricts a given base model to a given number of points (`cutoff`) in
    acquisition order, hence reproducing the state at a prior point.

    Annotations are passed through, without storing/replaying changes.

    Point content changes are forwarded, but the schema is static; changes to the latter
    necessitate a new model instance.
    """

    def __init__(
        self,
        parent: ScanModel,
        cutoff: int = -1,
    ):
        """
        Model describing a prior state of a `ScanModel`

        :param parent: The parent scan model.
        :param cutoff: State to roll back to. This is either the index of the entry
            to roll back to, or -1 for following the latest state as new points come in.
        """
        self.parent = parent
        super().__init__(self.parent.axes, parent.schema_revision, parent.context)

        self._sliced_data = {}

        self.parent.channel_schemata_changed.connect(self.channel_schemata_changed)
        self.parent.points_appended.connect(self._append_data)
        self.parent.points_rewritten.connect(self._rewrite_data)
        self.parent.annotations_changed.connect(self._update_annotations)

        self._cutoff = cutoff
        self._rewrite_data()

        self._update_annotations(self.parent._annotations)

    def _update_annotations(self, annotations):
        self._annotation_schemata = self.parent._annotation_schemata
        self._annotations = annotations
        self.annotations_changed.emit(annotations)

    def update_cutoff(self, cutoff: int) -> None:
        parent_data = self.parent.get_point_data()

        num_points = len(next(iter(parent_data.values()), []))

        if cutoff == -1 or num_points == 0:
            sliced_data = parent_data
        elif 0 <= cutoff < num_points:
            sliced_data = self.slice_data(parent_data, cutoff)
        else:
            raise ValueError(
                f"Cannot rollback to index {cutoff} "
                f"(have only {num_points} points available)"
            )
        self._cutoff = cutoff

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
        point_data = self.parent.get_point_data()
        num_points = len(next(iter(point_data.values()), []))

        if self._cutoff >= num_points:
            self.update_cutoff(-1)

        self.update_cutoff(self._cutoff)

    def _append_data(self, *args) -> None:
        point_data = self.parent.get_point_data()
        num_points = len(next(iter(point_data.values()), []))

        if self._cutoff >= num_points:
            self.update_cutoff(-1)

        if self._cutoff == -1:
            self.update_cutoff(self._cutoff)

    def slice_data(
        self,
        source_data: dict[str, Any],
        cutoff: int,
    ) -> dict[str, Any]:
        """Extract the sliced data from the parent point data.

        :param source_data: The point data from the parent model.
        :param cutoff: The index of the target to which to slice up to.
        :return: The sliced point data.
        """
        return {axis: values[: cutoff + 1] for axis, values in source_data.items()}

    def get_channel_schemata(self) -> dict[str, Any]:
        return self.parent.get_channel_schemata()

    def get_point_data(self) -> dict[str, Any]:
        return self._sliced_data

    def quit(self) -> None:
        self.parent.points_appended.disconnect(self._append_data)
        self.parent.points_rewritten.disconnect(self._rewrite_data)
        self.parent.annotations_changed.disconnect(self._update_annotations)
        self.parent.channel_schemata_changed.disconnect(self.channel_schemata_changed)

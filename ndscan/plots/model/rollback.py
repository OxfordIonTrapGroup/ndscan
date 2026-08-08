import logging
from typing import Any

import numpy as np

from . import ScanModel

logger = logging.getLogger(__name__)


# class RollbackRoot(Root):
#     def __init__(
#         self,
#         parent: ScanModel,
#         channel_schemata: dict[str, Any],
#         target_idx: int,
#     ):
#         super().__init__()

#         self._parent = parent
#         self._channel_schemata = channel_schemata
#         self._target_idx = target_idx
#         self._model = RollbackScanModel(parent, target_idx)

#         self._selected_point = None
#         self.rollback_to_point(target_idx)

#     def get_model(self) -> Model | None:
#         return self._model

#     def get_rollback_target_idx(self) -> int:
#         return self._target_idx

#     def set_rollback_target_idx(self, target_idx: int) -> None:
#         self._target_idx = target_idx
#         self._model.set_target_idx(target_idx)


class RollbackScanModel(ScanModel):
    """A 1-dimensional slice of an N-dimensional scan.

    Point content changes are forwarded, but the schema is static; changes to the latter
    necessitate a new model instance.
    """

    def __init__(
        self,
        parent: ScanModel,
        target_idx: int = -1,
    ):
        """
        Rollback a parent N-dimensional scan model to a `target_idx` entry.

        :param parent: The parent scan model.
        :param target_idx: The index of the target point.
        """

        self._parent = parent
        self._channel_schemata = self._parent.get_channel_schemata()

        super().__init__(self._parent.axes, parent.schema_revision, parent.context)

        self._sliced_data = {}

        self._parent.points_appended.connect(self._append_data)
        self._parent.points_rewritten.connect(self._rewrite_data)
        self._parent.annotations_changed.connect(
            lambda *args: self.annotations_changed.emit(args)
        )
        self._parent.channel_schemata_changed.connect(
            lambda *args: self._channel_schemata_changed.emit(args)
        )
        self._target_idx = target_idx
        self._rewrite_data()

    def _update_rollback_target(self, target_idx: int) -> None:
        parent_data = self._parent.get_point_data()

        num_points = len(next(iter(parent_data.values()), []))

        if target_idx == -1 or num_points == 0:
            sliced_data = parent_data
        elif 0 <= target_idx < num_points:
            sliced_data = self.slice_data(parent_data, target_idx)
        else:
            raise ValueError(
                f"Cannot rollback to index {target_idx} "
                f"(have only {num_points} points available)"
            )
        self._target_idx = target_idx

        data_rewritten = False
        for name, incoming_values in sliced_data.items():
            # Check if points were appended or rewritten.
            if name in self._sliced_data:
                imax = min(len(incoming_values), len(self._sliced_data[name]))
                if not np.array_equal(
                    incoming_values[:imax], self._sliced_data[name][:imax]
                ):
                    data_rewritten = True

        # print(type(self._sliced_data), type(sliced_data))
        # if self._sliced_data == sliced_data:
        #     return

        self._sliced_data = sliced_data

        if data_rewritten:
            self.points_rewritten.emit(self._sliced_data)
        else:
            self.points_appended.emit(self._sliced_data)

    def _rewrite_data(self, *args) -> None:
        point_data = self._parent.get_point_data()
        num_points = len(next(iter(point_data.values()), []))

        if self._target_idx >= num_points:
            self.set_target_idx(-1)

        self._update_rollback_target(self._target_idx)

    def _append_data(self, *args) -> None:
        point_data = self._parent.get_point_data()
        num_points = len(next(iter(point_data.values()), []))

        if self._target_idx >= num_points:
            self.set_target_idx(-1)

        if self._target_idx == -1:
            self._update_rollback_target(self._target_idx)

    def slice_data(
        self,
        source_data: dict[str, Any],
        target_idx: int,
    ) -> dict[str, Any]:
        """Extract the sliced data from the parent point data.

        :param source_data: The point data from the parent model.
        :param target_idx: The index of the target to which to slice up to.
        :return: The sliced point data.
        """

        return {axis: values[: target_idx + 1] for axis, values in source_data.items()}

    def get_channel_schemata(self) -> dict[str, Any]:
        return self._channel_schemata

    def get_point_data(self) -> dict[str, Any]:
        return self._sliced_data

    def quit(self) -> None:
        self._parent.points_appended.disconnect(self._append_data)
        self._parent.points_rewritten.disconnect(self._rewrite_data)
        self._rollback_target.point_changed.disconnect(self._update_rollback_target)

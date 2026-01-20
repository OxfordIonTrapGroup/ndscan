import logging
from typing import Any

import numpy as np

from ..utils import slice_data_along_axis
from . import Model, Root, ScanModel
from .select_point import SelectPointFromScanModel

logger = logging.getLogger(__name__)


class SliceRoot(Root):
    def __init__(
        self,
        parent: ScanModel,
        selected_point: SelectPointFromScanModel,
        channel_schemata: dict[str, Any],
        axis_idx: int,
    ):
        super().__init__()
        self.axis_idx = axis_idx

        self._parent = parent
        self._selected_point = selected_point
        self._channel_schemata = channel_schemata
        self._model = None

        self._selected_point = None
        self.set_selected_point(selected_point)

    def _update(self, *_args) -> None:
        fixed_point_idx = self._selected_point.get_source_index()

        # No point selected; clear model.
        if fixed_point_idx is None:
            if self._model:
                self._model.quit()
                self.model_changed.emit(None)
            self._model = None
            return

        if self._model is None:
            self._model = SliceForScanModel(
                self._parent, self.axis_idx, self._selected_point
            )
            self.model_changed.emit(self._model)

    def get_model(self) -> Model | None:
        return self._model

    def get_selected_point(self) -> SelectPointFromScanModel:
        return self._selected_point

    def set_selected_point(self, selected_point: SelectPointFromScanModel) -> None:
        self._selected_point = selected_point
        self._selected_point.point_changed.connect(self._update)
        self._update()


class SliceForScanModel(ScanModel):
    """A 1-dimensional slice of an N-dimensional scan.

    Point content changes are forwarded, but the schema is static; changes to the latter
    necessitate a new model instance.
    """

    def __init__(
        self,
        parent: ScanModel,
        axis_idx: int,
        fixed_point: SelectPointFromScanModel,
    ):
        """
        Slice a parent N-dimensional scan model along `axis_idx` through `fixed_point`.

        :param parent: The parent scan model.
        :param axis_idx: The index of the slicing axis
            (i.e. along which the coordinates vary).
        :param fixed_point: A model giving the fixed coordinates for the other axes.
        """

        self._parent = parent
        self._axis_idx = axis_idx
        self._fixed_point = fixed_point
        self._channel_schemata = self._parent.get_channel_schemata()

        axes = [self._parent.axes[self._axis_idx]]
        super().__init__(axes, parent.schema_revision, parent.context)

        self._sliced_data = {}

        self._fixed_point.point_changed.connect(self._update)
        self._parent.points_appended.connect(self._update)
        self._parent.points_rewritten.connect(self._update)
        self._update()

    def _update(self, *_args) -> None:
        parent_data = self._parent.get_point_data()
        fixed_point_idx = self._fixed_point.get_source_index()
        sliced_data = self.slice_data(parent_data, fixed_point_idx)

        data_rewritten = False
        for name, incoming_values in sliced_data.items():
            # Check if points were appended or rewritten.
            if name in self._sliced_data:
                imax = min(len(incoming_values), len(self._sliced_data[name]))
                if not np.array_equal(
                    incoming_values[:imax], self._sliced_data[name][:imax]
                ):
                    data_rewritten = True

        if self._sliced_data == sliced_data:
            return

        self._sliced_data = sliced_data

        if data_rewritten:
            self.points_rewritten.emit(self._sliced_data)
        else:
            self.points_appended.emit(self._sliced_data)

    def slice_data(
        self,
        source_data: dict[str, Any],
        fixed_point_idx: int | None,
    ) -> dict[str, Any]:
        """Extract the sliced data from the parent point data.

        :param source_data: The point data from the parent model.
        :param fixed_point_idx: The index of the fixed point in the parent data.
        :return: The sliced point data.
        """
        if source_data is None or fixed_point_idx is None:
            return {"axis_0": []}

        sliced_data = {}
        slice_indices = slice_data_along_axis(
            source_data, fixed_point_idx, self._axis_idx
        )

        sliced_data["axis_0"] = np.asarray(source_data[f"axis_{self._axis_idx}"])[
            slice_indices
        ].tolist()

        for name, values in source_data.items():
            if not name.startswith("axis_"):
                sliced_data[name] = np.asarray(values)[slice_indices].tolist()

        return sliced_data

    def get_channel_schemata(self) -> dict[str, Any]:
        return self._channel_schemata

    def get_point_data(self) -> dict[str, Any]:
        return self._sliced_data

    def quit(self) -> None:
        self._parent.points_appended.disconnect(self._update)
        self._parent.points_rewritten.disconnect(self._update)
        self._fixed_point.point_changed.disconnect(self._update)


def create_slice_roots(
    model: ScanModel, selected_point: SelectPointFromScanModel
) -> dict[str, SliceRoot]:
    schemata = model.get_channel_schemata()
    if schemata is None:
        return {}

    result = {}
    for i, axis in enumerate(model.axes):
        name = axis.get("param", {}).get("description", "axis_{}".format(i))
        root = SliceRoot(model, selected_point, schemata, i)
        result[name] = root
    return result

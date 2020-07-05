from typing import Any, Dict, Optional
from ...utils import strip_prefix
from . import ScanModel, SinglePointModel
import numpy


class SelectPointFromScanModel(SinglePointModel):
    def __init__(self, source: ScanModel):
        super().__init__(source.schema_revision, source.context)
        self._source = source
        self._source_index = None
        self._point = None

        self._source.points_rewritten.connect(
            lambda: self._set_point(self._source_index, silently_fail=True))

        # TODO: Invalidate point data (reset index?) on channel schema change.
        self._source.channel_schemata_changed.connect(self.channel_schemata_changed)

    def set_source_index(self, idx: Optional[int]) -> None:
        if idx == self._source_index:
            return
        self._set_point(idx, silently_fail=False)

    def get_channel_schemata(self) -> Dict[str, Any]:
        return self._source.get_channel_schemata()

    def get_point(self) -> Optional[Dict[str, Any]]:
        return self._point

    def _set_point(self, idx: Optional[int], silently_fail: bool) -> None:
        self._source_index = idx
        if idx is None:
            point = None
        else:
            points = self._source.get_point_data()
            num_values = len(next(iter(points.values())))
            if idx >= num_values:
                if silently_fail:
                    point = None
                else:
                    raise ValueError("Invalid source index {} for length {}".format(
                        idx, num_values))
            else:
                point = {}
                for key, values in points.items():
                    name = strip_prefix(key, "channel_")
                    if name != key:
                        point[name] = values[idx]
        # The point data can include NumPy arrays, which breaks object comparison (as
        # comparing two arrays gives back a bool array of element-wise results). We thus
        # need to use array_equal() to work around this.
        if numpy.array_equal(point, self._point):
            return
        self._point = point
        self.point_changed.emit(point)

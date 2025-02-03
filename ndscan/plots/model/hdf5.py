import json
import logging
from typing import Any
import h5py
from . import (Context, FixedDataSource, Model, Root, ScanModel, SinglePointModel)
from ...utils import SCHEMA_REVISION_KEY

logger = logging.getLogger(__name__)


class HDF5Root(Root):
    """Scan root fed from an HDF5 results file.

    :param datasets: The HDF5 group below which the dataset keys are found.
    :param prefix: Prefix of the ndscan dataset tree to represent, e.g.
        ``"ndscan."`` for the default location.
    """
    def __init__(self, datasets: h5py.Group, prefix: str, context: Context):
        super().__init__()

        try:
            schema_revision = datasets[prefix + SCHEMA_REVISION_KEY][()]
        except KeyError:
            # Backwards-compatibility with old files without SCHEMA_REVISION_KEY.
            schema_revision = 1

        axes = json.loads(datasets[prefix + "axes"][()])
        dim = len(axes)

        if dim == 0:
            self._model = HDF5SingleShotModel(datasets, prefix, schema_revision,
                                              context)
        else:
            self._model = HDF5ScanModel(axes, datasets, prefix, schema_revision,
                                        context)

    def get_model(self) -> Model | None:
        return self._model


class HDF5SingleShotModel(SinglePointModel):
    def __init__(self, datasets: h5py.Group, prefix: str, schema_revision: int,
                 context: Context):
        super().__init__(schema_revision, context)

        self._channel_schemata = json.loads(datasets[prefix + "channels"][()])

        self._point = {}
        for key in self._channel_schemata:
            self._point[key] = datasets[prefix + "point." + key][()]

    def get_channel_schemata(self) -> dict[str, Any]:
        return self._channel_schemata

    def get_point(self) -> dict[str, Any] | None:
        return self._point


class HDF5ScanModel(ScanModel):
    def __init__(self, axes: list[dict[str, Any]], datasets: h5py.Group, prefix: str,
                 schema_revision: int, context: Context):
        super().__init__(axes, schema_revision, context)

        self._channel_schemata = json.loads(datasets[prefix + "channels"][()])

        self._analysis_result_sources = {}
        ark = prefix + "analysis_results"
        if ark in datasets:
            for name in json.loads(datasets[ark][()]).keys():
                # FIXME: Need different HDF5 dataset operation for arrays?!
                try:
                    self._analysis_result_sources[name] = FixedDataSource(
                        datasets[prefix + "analysis_result." + name][()])
                except KeyError:
                    pass

        self._point_data = {}
        for name in ([f"axis_{i}" for i in range(len(self.axes))] +
                     ["channel_" + c for c in self._channel_schemata.keys()]):
            self._point_data[name] = datasets[prefix + "points." + name][:]

        self._set_online_analyses(json.loads(datasets[prefix + "online_analyses"][()]))
        self._set_annotation_schemata(json.loads(datasets[prefix + "annotations"][()]))

    def get_channel_schemata(self) -> dict[str, Any]:
        return self._channel_schemata

    def get_point_data(self) -> dict[str, Any]:
        return self._point_data

    def get_analysis_result_source(self, name: str) -> FixedDataSource | None:
        if name not in self._analysis_result_sources:
            return None
        return self._analysis_result_sources[name]

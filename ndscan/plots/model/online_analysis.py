import asyncio
from concurrent.futures import ProcessPoolExecutor
from pyqtgraph import SignalProxy
from quamash import QtCore
from typing import Any, Dict
from ...utils import FIT_OBJECTS


class OnlineAnalysis(QtCore.QObject):
    updated = QtCore.pyqtSignal()

    def stop(self):
        pass


class OnlineNamedFitAnalysis(OnlineAnalysis):
    """Implements :class:`ndscan.experiment.default_analysis.OnlineFit`, that is, a fit
    of a well-known function that is executed repeatedly as new data is coming in.

    :param schema: The ``ndscan.online_analyses`` schema to implement.
    :param parent_model: The :class:`~ndscan.plots.model.ScanModel` to draw the data
        from. The schema is notexpected not to change until :meth:`stop` is called.
    """
    _trigger_recompute_fit = QtCore.pyqtSignal()

    def __init__(self, schema: Dict[str, Any], parent_model):
        super().__init__()
        self._schema = schema
        self._model = parent_model

        self._fit_type = self._schema["fit_type"]
        self._fit_obj = FIT_OBJECTS[self._fit_type]
        self._constants = self._schema.get("constants", {})
        self._initial_values = self._schema.get("initial_values", {})

        self._last_fit_params = None
        self._last_fit_errors = None

        self._recompute_fit_limiter = SignalProxy(
            self._trigger_recompute_fit,
            slot=lambda: asyncio.ensure_future(self._recompute_fit()),
            rateLimit=30)
        self._recompute_in_progress = False
        self._fit_executor = ProcessPoolExecutor(max_workers=1)

        self._model.points_rewritten.connect(self._update)
        self._model.points_appended.connect(self._update)

        self._update()

    def stop(self):
        self._model.points_rewritten.disconnect(self._update)
        self._model.points_appended.disconnect(self._update)
        self._fit_executor.shutdown(wait=False)

    def get_data(self):
        if self._last_fit_params is None:
            return {}
        result = self._last_fit_params.copy()
        for key, value in self._last_fit_errors.items():
            error_key = key + "_error"
            if error_key in result:
                raise ValueError(
                    "Fit error key name collides with result: ''".format(error_key))
            result[error_key] = value
        return result

    def _update(self):
        data = self._model.get_point_data()

        self._source_data = {}
        for param_key, source_key in self._schema["data"].items():
            self._source_data[param_key] = data.get(source_key, [])

        # Truncate the source data to a complete set of points.
        num_points = min(len(v) for v in self._source_data.values())
        if num_points < len(self._fit_obj.parameter_names):
            # Not enough points yet for the given number of degrees of freedom.
            return

        for key, value in self._source_data.items():
            self._source_data[key] = value[:num_points]
        self._trigger_recompute_fit.emit()

    async def _recompute_fit(self):
        if self._recompute_in_progress:
            # Run at most one fit computation at a time. To make sure we don't
            # leave a few final data points completely disregarded, just
            # re-emit the signal – even for long fits, repeated checks aren't
            # expensive, as long as the SignalProxy rate is slow enough.
            self._trigger_recompute_fit.emit()
            return

        self._recompute_in_progress = True

        # oitg.fitting currently only supports 1D fits, but this could/should be
        # changed.
        xs = self._source_data["x"]
        ys = self._source_data["y"]
        y_errs = self._source_data.get("y_err", None)

        loop = asyncio.get_event_loop()
        self._last_fit_params, self._last_fit_errors = await loop.run_in_executor(
            self._fit_executor, _run_fit, self._fit_type, xs, ys, y_errs,
            self._constants, self._initial_values)

        self._recompute_in_progress = False
        self.updated.emit()


def _run_fit(fit_type, xs, ys, y_errs, constants, initial_values):
    """Fits the given data with the chosen method.

    This function is intended to be executed on a worker process, hence the
    primitive API.
    """
    try:
        return FIT_OBJECTS[fit_type].fit(x=xs,
                                         y=ys,
                                         y_err=y_errs,
                                         constants=constants,
                                         initialise=initial_values)
    except Exception:
        return None, None

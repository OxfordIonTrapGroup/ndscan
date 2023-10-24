import logging
import numpy as np
import pyqtgraph
from collections import defaultdict
from typing import NamedTuple

from .._qt import QtCore
from .annotation_items import ComputedCurveItem, CurveItem, VLineItem
from .cursor import CrosshairAxisLabel, LabeledCrosshairCursor
from .model import ScanModel
from .model.select_point import SelectPointFromScanModel
from .model.subscan import create_subscan_roots
from .plot_widgets import (SubplotMenuPanesWidget, build_channel_selection_context_menu,
                           add_source_id_label)
from .utils import (extract_linked_datasets, extract_scalar_channels,
                    get_default_hidden_channels, format_param_identity,
                    group_channels_into_axes, group_axes_into_panes,
                    hide_series_from_groups, get_axis_scaling_info, setup_axis_item,
                    FIT_COLORS, SERIES_COLORS, enum_to_numeric)

logger = logging.getLogger(__name__)


class SourcePoint(NamedTuple):
    """For point averaging, keeps track of individual points in the source data (as
    opposed to the points derived from averaging)."""
    y: float
    y_err: float | None
    source_idx: int


def combined_uncertainty(points: list[SourcePoint], num_samples_per_point=1):
    """Combine several points and return the error of the average.

    To combine data points, each created as an average of multiple samples, the
    uncertainty of the individual points and the variance of the ensemble of points
    must be taken into account. A derivation can be found in https://www.yumpu.com/en/
    document/read/37147068/combining-multiple-averaged-data-points-and-their-errors. To
    calculate the standard deviation of the underlying data from individual bins in
    this way, the number of samples per point would be required. However, in this
    context here, this quantity is unavailable.

    Firstly, we assume that all points contain the same number of samples. Secondly, we
    use one sample per point as the default, which produces the "worst case" scenario,
    where we overestimate the true standard deviation if actually,
    ``num_samples_per_point > 1``.
    """
    n = len(points)
    y = [p.y for p in points]
    total_var = np.var(y) / max(1, num_samples_per_point * n - 1)  # max() avoids 0/0
    if points[0].y_err is not None:
        total_var += sum(p.y_err**2 for p in points) / n**2
    return np.sqrt(total_var)


class _XYSeries(QtCore.QObject):
    def __init__(self, view_box, data_name, data_item, error_bar_name, error_bar_item,
                 series_idx, pane_idx):
        super().__init__(view_box)

        self.view_box = view_box
        self.data_item = data_item
        self.data_name = data_name
        self.error_bar_item = error_bar_item
        self.error_bar_name = error_bar_name
        self.num_current_points = 0
        self.series_idx = series_idx
        self.pane_idx = pane_idx

        #: Whether to average points with the same x coordinate.
        self.averaging_enabled = False

        #: Keeps track of source points for each x coordinate for faster updates while
        #: averaging is enabled.
        self.source_points_by_x = defaultdict[float, list[SourcePoint]](list)

    def update(self, x_data, data, averaging_enabled):
        def channel(name):
            return np.array(data.get("channel_" + name, []))

        x_data = np.array(x_data)
        y_data = channel(self.data_name)

        # Determine how many data points are actually complete.
        num_to_show = min(len(x_data), len(y_data))
        y_err = None
        if self.error_bar_name:
            y_err = channel(self.error_bar_name)
            num_to_show = min(num_to_show, len(y_err))

        # If nothing has changed, skip the update.
        if (num_to_show == self.num_current_points
                and averaging_enabled == self.averaging_enabled):
            return

        # Combine points with same coordinates if enabled.
        if averaging_enabled:
            x_data, y_data, y_err, source_idxs = self._average_add_points(
                num_to_show, x_data, y_data, y_err)
        else:
            x_data = x_data[:num_to_show]
            y_data = y_data[:num_to_show]
            if y_err is not None:
                y_err = y_err[:num_to_show]
            source_idxs = np.arange(num_to_show)

        # source_idxs can be queried later via spot.data().
        self.data_item.setData(x_data, y_data, data=source_idxs)

        if y_err is not None:
            self.error_bar_item.setData(x=x_data, y=y_data, height=2 * y_err)

        if self.num_current_points == 0:
            self.view_box.addItem(self.data_item)
            if y_err is not None:
                self.view_box.addItem(self.error_bar_item)
        elif averaging_enabled != self.averaging_enabled:
            if y_err is None:
                self.view_box.removeItem(self.error_bar_item)
            elif not self.error_bar_name:
                self.view_box.addItem(self.error_bar_item)

        self.averaging_enabled = averaging_enabled
        self.num_current_points = num_to_show

    def _average_add_points(self, num_to_show, x_data, y_data, y_err):
        # Append new data to collection.
        start_idx = sum(len(v) for v in self.source_points_by_x.values())
        for i in range(start_idx, num_to_show):
            self.source_points_by_x[x_data[i]].append(
                SourcePoint(y=y_data[i],
                            y_err=None if y_err is None else y_err[i],
                            source_idx=i))

        # Average over values with same coordinate.
        x_data = np.array(list(self.source_points_by_x.keys()))
        # Using the unweighted mean to estimate the mean of the underlying data
        # assuming that 1) the samples which constitute the points are drawn from the
        # same distribution, and 2) the number of samples per point are equal for all
        # points -- see ``combined_uncertainty()``.
        y_data = np.array(
            [np.mean([p.y for p in self.source_points_by_x[x]]) for x in x_data])
        y_err = np.array(
            [combined_uncertainty(self.source_points_by_x[x]) for x in x_data])

        # We can only ascribe a single source index to the data if there wasn't any
        # actual averaging.
        source_idxs = [
            self.source_points_by_x[x][0].source_idx
            if len(self.source_points_by_x[x]) == 1 else None for x in x_data
        ]

        return x_data, y_data, y_err, source_idxs

    def remove_items(self):
        if self.num_current_points == 0:
            return
        self.view_box.removeItem(self.data_item)
        self.view_box.removeItem(self.error_bar_item)
        self.source_points_by_x.clear()
        self.num_current_points = 0


class XY1DPlotWidget(SubplotMenuPanesWidget):
    error = QtCore.pyqtSignal(str)
    ready = QtCore.pyqtSignal()

    def __init__(self, model: ScanModel, get_alternate_plot_names):
        super().__init__(model.context, get_alternate_plot_names)
        self.model = model
        self.model.channel_schemata_changed.connect(self._initialise_series)
        self.model.points_appended.connect(self._update_points)
        self.model.annotations_changed.connect(self._update_annotations)
        self.model.points_rewritten.connect(self._rewrite)

        self.selected_point_model = SelectPointFromScanModel(self.model)

        self.annotation_items = []
        self.series = []
        self.unique_x_data = set()
        self.found_duplicate_x_data = False
        self.averaging_enabled = False

        self.x_schema = self.model.axes[0]
        self.x_param_spec = self.x_schema["param"]["spec"]
        self.x_unit_suffix, self.x_data_to_display_scale = get_axis_scaling_info(
            self.x_param_spec)

        self.crosshairs = []
        self._highlighted_spot = None

        # List of all scalar channel names for the context menu.
        self.data_names = None
        # Set of channel names that are currently hidden.
        self.hidden_channels = None

    def _rewrite(self, points):
        self._initialise_series(self.model.get_channel_schemata())
        self._update_points(points)

    def _initialise_series(self, channels):
        # Remove all currently shown items and any extra axes added.
        self.clear_panes()
        self.clear()
        for s in self.series:
            s.remove_items()
        self.series.clear()
        self.crosshairs.clear()
        self.unique_x_data.clear()
        self.found_duplicate_x_data = False
        self._clear_annotations()

        self.subscan_roots = create_subscan_roots(self.selected_point_model)

        try:
            (self.data_names, error_bar_names) = extract_scalar_channels(channels)
        except ValueError as e:
            self.error.emit(str(e))
            return

        axes = group_channels_into_axes(channels, self.data_names)
        panes_axes = group_axes_into_panes(channels, axes)
        if self.hidden_channels is None:
            self.hidden_channels = get_default_hidden_channels(
                channels, self.data_names)
        panes_axes_shown = hide_series_from_groups(panes_axes, self.hidden_channels)

        for axes_series in panes_axes_shown:
            pane = self.add_pane()
            pane.showGrid(x=True, y=True)
            crosshair_items = []
            for series in axes_series:
                axis, view_box = pane.new_y_axis()
                view_box.scene().sigMouseClicked.connect(self._handle_scene_click)

                info = []
                for (series_idx, name) in series:
                    color = SERIES_COLORS[series_idx % len(SERIES_COLORS)]
                    data_item = pyqtgraph.ScatterPlotItem(pen=None, brush=color, size=6)
                    data_item.sigClicked.connect(self._point_clicked)

                    error_bar_name = error_bar_names.get(name, None)

                    # Always create ErrorBarItem in case averaging is enabled later.
                    error_bar_item = pyqtgraph.ErrorBarItem(pen=color)

                    self.series.append(
                        _XYSeries(view_box, name, data_item, error_bar_name,
                                  error_bar_item, series_idx,
                                  len(self.panes) - 1))

                    channel = channels[name]
                    label = channel["description"]
                    if not label:
                        label = channel["path"].split("/")[-1]
                    info.append(
                        (label, channel["path"], channel["type"], color, channel))

                crosshair_label_args = setup_axis_item(axis, info)
                crosshair_items.extend(
                    [CrosshairAxisLabel(view_box, *a) for a in crosshair_label_args])

            x_crosshair_item = CrosshairAxisLabel(pane.getViewBox(),
                                                  self.x_unit_suffix,
                                                  self.x_data_to_display_scale,
                                                  is_x=True)
            crosshair_items = [x_crosshair_item] + crosshair_items
            crosshair = LabeledCrosshairCursor(self, pane, crosshair_items)
            self.crosshairs.append(crosshair)

        if len(self.panes) > 1:
            self.link_x_axes()

        add_source_id_label(self.panes[-1].getViewBox(), self.model.context)

        setup_axis_item(self.panes[-1].getAxis("bottom"), [
            (self.x_schema["param"]["description"], format_param_identity(
                self.x_schema), self.x_schema["param"]["type"], None, self.x_param_spec)
        ])

        # Make sure we put back annotations (if they haven't changed but the points
        # have been rewritten, there might not be an annotations_changed event).
        self._update_annotations()

        self.ready.emit()

    def _update_points(self, points):
        x_data = points["axis_0"]
        # Compare length to zero instead of using `not x_data` for NumPy array
        # compatibility.
        if len(x_data) == 0:
            return

        # If all points were unique so far, check if we have duplicates now.
        if not self.found_duplicate_x_data:
            for x in x_data[len(self.unique_x_data):]:
                if x in self.unique_x_data:
                    self.found_duplicate_x_data = True
                    break
                else:
                    self.unique_x_data.add(x)

        if self.x_schema["param"]["type"].startswith("enum"):
            x_data = enum_to_numeric(self.x_param_spec["enum_display_map"].keys(),
                                     x_data)
        for s in self.series:
            s.update(x_data, points, self.averaging_enabled)

    def _clear_annotations(self):
        for item in self.annotation_items:
            item.remove()
        self.annotation_items.clear()

    def _update_annotations(self):
        self._clear_annotations()

        def channel_refs_to_series(refs):
            associated_series = []
            seen_panes = set[int]()
            for series in self.series:
                if series.pane_idx in seen_panes:
                    # Limit to one entry per pane.
                    continue
                use_series = False
                if refs is None:
                    # Append an entry in each viewbox.
                    use_series = True
                else:
                    for ref in refs:
                        if "channel_" + series.data_name == ref:
                            use_series = True
                            break
                if use_series:
                    associated_series.append(series)
                    seen_panes.add(series.pane_idx)
            return associated_series

        def make_curve_item(series_idx):
            color = FIT_COLORS[series_idx % len(FIT_COLORS)]
            pen = pyqtgraph.mkPen(color, width=3)
            return pyqtgraph.PlotCurveItem(pen=pen)

        annotations = self.model.get_annotations()
        for a in annotations:
            if a.kind == "location":
                if set(a.coordinates.keys()) == {"axis_0"}:
                    channel_refs = a.parameters.get("associated_channels", None)
                    associated_series = channel_refs_to_series(channel_refs)
                    for series in associated_series:
                        color = FIT_COLORS[series.series_idx % len(FIT_COLORS)]
                        line = VLineItem(
                            a.coordinates["axis_0"],
                            a.data.get("axis_0_error", None),
                            series.view_box,
                            color,
                            self.x_data_to_display_scale,
                            self.x_unit_suffix,
                            show_label=(series is associated_series[0]),
                        )
                        self.annotation_items.append(line)

            elif a.kind == "curve":
                associated_series = None
                for series in self.series:
                    match_coords = {"axis_0", "channel_" + series.data_name}
                    if set(a.coordinates.keys()) == match_coords:
                        associated_series = series
                        break
                if associated_series is not None:
                    item = CurveItem(
                        a.coordinates["axis_0"],
                        a.coordinates["channel_" + associated_series.data_name],
                        associated_series.view_box,
                        make_curve_item(associated_series.series_idx),
                    )
                    self.annotation_items.append(item)

            elif a.kind == "computed_curve":
                function_name = a.parameters.get("function_name", None)
                if ComputedCurveItem.is_function_supported(function_name):
                    channel_refs = a.parameters.get("associated_channels", None)
                    associated_series = channel_refs_to_series(channel_refs)
                    for series in associated_series:
                        x_limits = [
                            self.x_param_spec.get(n, None) for n in ("min", "max")
                        ]
                        item = ComputedCurveItem(
                            function_name,
                            a.data,
                            series.view_box,
                            make_curve_item(series.series_idx),
                            x_limits,
                        )
                        self.annotation_items.append(item)

            else:
                logger.info("Ignoring annotation of kind '%s' with coordinates %s",
                            a.kind, list(a.coordinates.keys()))

    def build_context_menu(self, pane_idx, builder):
        x_schema = self.model.axes[0]

        if self.model.context.is_online_master():
            for d in extract_linked_datasets(x_schema["param"]):
                action = builder.append_action(f"Set '{d}' from crosshair")
                action.triggered.connect(
                    lambda *a, d=d: self._set_dataset_from_crosshair_x(pane_idx, d))
            builder.ensure_separator()

        if self.found_duplicate_x_data:
            action = builder.append_action("Average points with same x")
            action.setCheckable(True)
            action.setChecked(self.averaging_enabled)
            action.triggered.connect(
                lambda *a: self.enable_averaging(not self.averaging_enabled))
            builder.ensure_separator()

        if len(self.data_names) > 1:
            build_channel_selection_context_menu(
                builder, lambda: self._rewrite(self.model.get_point_data()),
                self.data_names, self.hidden_channels)
            builder.ensure_separator()

        super().build_context_menu(pane_idx, builder)

    def enable_averaging(self, enabled: bool):
        self.averaging_enabled = enabled
        self._update_points(self.model.get_point_data())

    def _set_dataset_from_crosshair_x(self, pane_idx, dataset_key):
        if not self.crosshairs:
            logger.warning("Plot not initialised yet, ignoring set dataset request")
            return
        # The x crosshair is always the first item (see `_initialise_series()`).
        self.model.context.set_dataset(
            dataset_key, self.crosshairs[pane_idx].crosshair_items[0].last_value)

    def _highlight_spot(self, spot):
        if self._highlighted_spot is not None:
            self._highlighted_spot.resetPen()
            self._highlighted_spot = None
        if spot is not None:
            spot.setPen("y", width=2)
            self._highlighted_spot = spot

    def _point_clicked(self, scatter_plot_item, spot_items: np.ndarray):
        if len(spot_items) == 0:
            # No points clicked. Nota bene: pyqtgraph does not actually seem to emit
            # events in this case anyway, but this is not well-documented.
            self._background_clicked()
            return

        # Arbitrarily choose the first element in the list if multiple spots
        # overlap; the user can always zoom in if that is undesired.
        spot = spot_items[0]
        source_index = spot.data()
        if source_index is None:
            # This came from a point for which there was averaging.
            # TODO: Show an informative message to the user in some kind of low-overhead
            # way (e.g. a text plot item; a QMessageBox would be distracting/annoying).
            pass
        else:
            self._highlight_spot(spot)
            self.selected_point_model.set_source_index(spot.data())

    def _background_clicked(self):
        self._highlight_spot(None)
        self.selected_point_model.set_source_index(None)

    def _handle_scene_click(self, event):
        if not event.isAccepted():
            # Event not handled yet, so background/… was clicked instead of a point.
            self._background_clicked()

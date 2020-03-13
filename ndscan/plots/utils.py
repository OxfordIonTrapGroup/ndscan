import logging
from typing import Any, Dict, List, Tuple
from ..utils import eval_param_default

logger = logging.getLogger(__name__)

# ColorBrewer-inspired to use for data series (RGBA) and associated fit curves.
SERIES_COLORS = [
    "#d9d9d9bb", "#fdb462bb", "#80b1d3bb", "#fb8072bb", "#bebadabb", "#ffffb3bb"
]
FIT_COLORS = [
    "#ff333399", "#fdb462dd", "#80b1d3dd", "#fb8072dd", "#bebadadd", "#ffffb3dd"
]


def extract_scalar_channels(
        channels: Dict[str, Any]) -> Tuple[List[str], Dict[str, str]]:
    """Extract channels with scalar numerical values from the given channel metadata,
    also mapping error bar channels to their associated value channels.

    :param channels: The ndscan.channels metadata.

    :return: A tuple ``(data_names, error_bar_names)``. The first element is a list of
        strings giving the scalar channel names in priority order (excluding error
        bars), the second a dictionary matching those channels to the associated error
        bars, if any.
    """
    data_names = set(name for name, spec in channels.items()
                     if spec["type"] in ["int", "float"])

    path_to_name = {channels[name]["path"]: name for name in data_names}

    # Build map from "primary" channel names to error bar names.
    error_bar_names = {}
    for name in data_names:
        spec = channels[name]
        display_hints = spec.get("display_hints", {})
        err_path = display_hints.get("error_bar_for", "")
        if not err_path:
            continue
        if err_path not in path_to_name:
            msg = "Error bar target '{}' does not exist".format(err_path)
            if err_path in channels:
                # Previously, this accepted the shortened name instead of the full path;
                # suggest this to help users migrate.
                msg += "; did you mean to specify the full path '{}'?".format(
                    channels[name]["path"])
            logger.warning(msg)
            # Still avoid to display the error bar channel, though (key is arbitrary).
            error_bar_names[err_path] = name
            continue
        err_name = path_to_name[err_path]
        if err_name in error_bar_names:
            raise ValueError(
                "More than one set of error bars specified for channel '{}'".format(
                    err_path))
        error_bar_names[err_name] = name

    data_names -= set(error_bar_names.values())

    # Sort by descending priority and then path (the latter for stable order).
    def get_priority(name):
        return channels[name].get("display_hints", {}).get("priority", 0)

    data_names = list(data_names)
    data_names.sort(key=lambda name: (-get_priority(name), channels[name]["path"]))

    # HACK: Don't show negative-priority channels by default (but leave at least one).
    # Instead of removing them entirely, they should just be hidden at a higher layer
    # (and still be accessible from a context menu).
    while len(data_names) > 1:
        if get_priority(data_names[-1]) >= 0:
            break
        try:
            del error_bar_names[data_names[-1]]
        except KeyError:
            pass
        data_names = data_names[:-1]

    return data_names, error_bar_names


def group_channels_into_axes(channels: Dict[str, Any],
                             data_names: List[str]) -> List[List[str]]:
    """Extract channels with scalar numerical values from the given channel metadata,
    also mapping error bar channels to their associated value channels.

    :param channels: ndscan.channels metadata.
    :param data_names: The channels to group. Sets the order of results.

    :return: A list of lists giving the channel names along each axis.
    """

    # The display hint is given in terms of paths, so we need to translate to names. We
    # cache the results in a dict to only emit the does-not-exist warning once.
    path_to_name = {channels[name]["path"]: name for name in data_names}

    def get_share_name(name):
        path = channels[name].get("display_hints", {}).get("share_axis_with", None)
        if path is None:
            return name
        if path not in path_to_name:
            logger.warning("share_axis_with target path '%s' does not exist", path)
            return name
        return path_to_name[path]

    # Group data names into axes. We don't know which order we will get the channels in,
    # so just check both directions. This implementation is quadratic, but many other
    # things will break before this becomes a concern.
    axes = []
    share_names = {}
    for index, name in enumerate(data_names):
        share_name = get_share_name(name)
        share_names[name] = share_name

        target_axis = None

        # Find links of the current name to any already existing axes.
        if share_name != name:
            for axis in axes:
                for _, existing_name in axis:
                    if existing_name == share_name:
                        target_axis = axis
                        break
                else:
                    continue
                break
        if target_axis is None:
            target_axis = []
            axes.append(target_axis)

        target_axis.append((index, name))

        # Now resolve any already existing axes with items pointing to the
        # current name by merging them into the current axis.
        new_axes = []
        for axis in axes:
            if axis == target_axis:
                # Can't merge target into itself.
                new_axes.append(axis)
                continue
            links_to_current = False
            for _, existing_name in axis:
                if share_names[existing_name] == name:
                    links_to_current = True
                    break
            if links_to_current:
                target_axis.extend(axis)
            else:
                new_axes.append(axis)
        axes = new_axes

    # Sort the channels on each axes by original order, and then the groups themselves
    # lexicographically too.
    for axis in axes:
        axis.sort()
    axes.sort(key=lambda a: a[0])

    return [[name for (_, name) in axis] for axis in axes]


def extract_linked_datasets(param_schema: Dict[str, Any]) -> List[str]:
    """Extract datasets mentioned in the default value of the given parameter schema.

    :return: A list of dataset keys mentioned.
    """
    datasets = []
    try:
        # Intercept dataset() to build up list of accessed keys.
        def log_datasets(dataset, default):
            datasets.append(dataset)
            return default

        eval_param_default(param_schema["default"], log_datasets)
    except Exception:
        # Ignore default parsing errors here; the user will get warnings from the
        # experiment dock and on the core device anyway.
        pass
    return datasets


def format_param_identity(schema: Dict[str, Any]) -> str:
    """Extract a string representation of the parameter identity from the given schema,
    for use in human-readable labels.
    """
    path = schema["path"]
    if not path:
        path = "/"
    shortened_fqn = schema["param"]["fqn"].split(".")[-1]
    return shortened_fqn + "@" + path


def setup_axis_item(axis_item, axes: List[Tuple[str, str, str, Dict[str, Any]]]):
    """Set up an axis item with the appropriate labels/scaling for the given axis
    metadata.

    :param axis_item: The :class:`pyqtgraph.AxisItem` to set up.
    :param axes: A list of tuples ``(description, identity_string, color, spec)``,
        giving for each logical axes to be displayed on the target axis item the name
        and source fragment identity to be displayed, the series color to render it in,
        and the backing parameter/result channel metadata.
    :return: A tuple ``(unit_suffix, data_to_display_scale)`` of the unit suffix to
        display when referring to coordinates on this axis, and the scale factor to
        apply to compute the data from display coordinates due to the applied units.
    """
    def label_html(description, identity_string, color, spec):
        result = ""
        if color is not None:
            if isinstance(color, str) and len(color) == 9 and color[0] == "#":
                # KLUDGE: Reorder RGBA to ARGB.
                color = "#" + color[7:] + color[1:7]
            result += "<span style='color: \"{}\"'>".format(color)
        unit = spec.get("unit", "")
        if unit:
            unit = "/ " + unit + " "
        result += "<b>{} {}</b>".format(description, unit)
        if identity_string:
            pt = axis_item.label.font().pointSizeF() * 0.8
            result += "<i style='font-size: {}pt'>({})</i>".format(pt, identity_string)
        if color is not None:
            result += "</span>"
        return result

    axis_item.setLabel("<br>".join(label_html(*a) for a in axes))

    if len(axes) != 1:
        return "", 1.0

    _, _, _, spec = axes[0]
    unit_suffix = ""
    unit = spec.get("unit", "")
    if unit:
        unit_suffix = " " + unit

    data_to_display_scale = 1 / spec["scale"]
    axis_item.setScale(data_to_display_scale)
    axis_item.autoSIPrefix = False
    return unit_suffix, data_to_display_scale

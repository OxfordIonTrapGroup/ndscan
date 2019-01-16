from ndscan.utils import eval_param_default

# Colours to use for data series (RGBA) and associated fit curves.
SERIES_COLORS = [
    "#d9d9d999", "#fdb46299", "#80b1d399", "#fb807299", "#bebeada99", "#ffffb399"
]
FIT_COLORS = [
    "#ff333399", "#fdb462dd", "#80b1d3dd", "#fb8072dd", "#bebeadadd", "#ffffb3dd"
]


def extract_scalar_channels(channels):
    data_names = set(
        name for name, spec in channels.items() if spec["type"] in ["int", "float"])

    # Build map from "primary" channel names to error bar names.
    error_bar_names = {}
    for name in data_names:
        spec = channels[name]
        display_hints = spec.get("display_hints", {})
        eb = display_hints.get("error_bar_for", "")
        if not eb:
            continue
        if eb in error_bar_names:
            raise ValueError(
                "More than one set of error bars specified for channel '{}'".format(eb))
        error_bar_names[eb] = name

    data_names -= set(error_bar_names.values())

    # Sort by descending priority and then path (the latter for stable order).
    def priority_key(name):
        return (-channels[name].get("display_hints", {}).get("priority", 0),
                channels[name]["path"])

    data_names = list(data_names)
    data_names.sort(key=priority_key)

    return data_names, error_bar_names


def extract_linked_datasets(param_schema):
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


def setup_axis_item(axis_item, description, identity_string, spec):
    unit_suffix = ""
    unit = spec.get("unit", "")
    if unit:
        unit_suffix = " " + unit
        unit = "/ " + unit + " "

    label = "<b>{} {}</b>".format(description, unit)
    if identity_string:
        label += "<i>({})</i>".format(identity_string)
    axis_item.setLabel(label)

    data_to_display_scale = 1 / spec["scale"]
    axis_item.setScale(data_to_display_scale)
    axis_item.autoSIPrefix = False

    return unit_suffix, data_to_display_scale

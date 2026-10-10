import unittest

from ndscan.experiment import *
from ndscan.experiment.annotations import AnnotationContext


def describe(annotation):
    context = AnnotationContext(
        get_axis_index=lambda handle: 0,
        name_channel=lambda channel: channel.path,
        analysis_result_is_exported=lambda channel: True,
    )
    return annotation.describe(context)


class AssociatedChannelsCase(unittest.TestCase):
    def test_resolve_channels(self):
        channel = FloatChannel("foo")
        spec = describe(
            annotations.axis_location(
                "axis_0", 1.0, associated_channels=[channel, "channel_bar"]
            )
        )
        self.assertEqual(
            spec["parameters"], {"associated_channels": ["channel_foo", "channel_bar"]}
        )

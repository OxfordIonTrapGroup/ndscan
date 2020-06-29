"""ARTIQ applet that plots the results of a single ndscan experiment.

Typically, applets aren't created manually, but used via ``ndscan.experiment`` (CCB).
"""

from artiq.applets.simple import SimpleApplet
import asyncio
import logging
import pyqtgraph
from sipyco.pc_rpc import AsyncioClient
from sipyco.sync_struct import Subscriber
from typing import Any, Dict, Iterable

from .plots.container_widgets import RootWidget
from .plots.model import Context
from .plots.model.subscriber import SubscriberRoot

logger = logging.getLogger(__name__)


class _MainWidget(RootWidget):
    def __init__(self, args):
        self.args = args

        # TODO: Consider exposing Context in Root.
        context = Context(self.set_dataset)
        super().__init__(SubscriberRoot(args.prefix, context), context)

        # Try ensuring a sensible window size on startup (i.e. large enough to show a
        # plot in.
        # FIXME: This doesn't seem to work when used with ARTIQ applet embedding. See if
        # call_later() works around that, or whether this needs to be fixed in ARTIQ.
        self.resize(600, 600)
        self.setWindowTitle("ndscan plot")

    def data_changed(self, data: Dict[str, Any], mods: Iterable[Dict[str, Any]]):
        self.root.data_changed(data, mods)

    def set_dataset(self, key, value):
        asyncio.ensure_future(self._set_dataset_impl(key, value))

    async def _set_dataset_impl(self, key, value):
        logger.info("Setting '%s' to %s", key, value)
        try:
            remote = AsyncioClient()
            await remote.connect_rpc(self.args.server, self.args.port_control,
                                     "master_dataset_db")
            try:
                await remote.set(key, value, persist=True)
            finally:
                remote.close_rpc()
        except Exception:
            logger.error("Failed to set dataset '%s'", key, exc_info=True)


class NdscanApplet(SimpleApplet):
    def __init__(self):
        # Use a small update delay by default to avoid lagging out the UI by
        # continuous redraws for plots with a large number of points. (20 ms
        # is a pretty arbitrary choice for a latency not perceptible by the
        # user in a normal use case).
        super().__init__(_MainWidget, default_update_delay=20e-3)

        self.argparser.add_argument("--port-control",
                                    default=3251,
                                    type=int,
                                    help="TCP port for master control commands")
        self.argparser.add_argument("--prefix",
                                    default="ndscan.",
                                    type=str,
                                    help="Root of the ndscan dataset tree")
        self.argparser.add_argument("--rid", help="RID of the experiment to plot")

    def subscribe(self):
        # We want to subscribe only to the experiment-local datasets for our RID
        # (but always, even if using IPC – this can be optimised later).
        self.subscriber = Subscriber("datasets_rid_{}".format(self.args.rid),
                                     self.sub_init, self.sub_mod)
        self.loop.run_until_complete(
            self.subscriber.connect(self.args.server, self.args.port))

        # Make sure we still respond to non-dataset messages like `terminate` in
        # embed mode.
        if self.embed is not None:

            def ignore(*args):
                pass

            self.ipc.subscribe([], ignore, ignore)

    def unsubscribe(self):
        self.loop.run_until_complete(self.subscriber.close())

    def filter_mod(self, *args):
        return True


def main():
    pyqtgraph.setConfigOptions(antialias=True)
    NdscanApplet().run()


if __name__ == "__main__":
    main()

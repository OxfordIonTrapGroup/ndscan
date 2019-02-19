from artiq.applets.simple import SimpleApplet
from artiq.protocols.pc_rpc import AsyncioClient
from artiq.protocols.sync_struct import Subscriber
import asyncio
import logging
import pyqtgraph
from quamash import QtWidgets

from .plots.container import PlotContainerWidget
from .plots.model import Context
from .plots.model_subscriber import SubscriberRoot

logger = logging.getLogger(__name__)


class _MainWidget(QtWidgets.QWidget):
    def __init__(self, args):
        super().__init__()
        self.args = args

        self.setWindowTitle("ndscan plot")
        self.resize(600, 600)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        self.widget_stack = QtWidgets.QStackedWidget()
        self.message_label = QtWidgets.QLabel(
            "Waiting for ndscan metadata for rid {}…".format(self.args.rid))
        self.widget_stack.addWidget(self.message_label)
        self.layout.addWidget(self.widget_stack)

        self.context = Context(self.set_dataset)
        self.context.title_changed.connect(self._set_window_title)
        self.root = SubscriberRoot(self.context)
        self.root.model_changed.connect(self._create_plot)

        self.plot_container = None

    def data_changed(self, data, mods):
        self.root.data_changed(data, mods)

    def _create_plot(self):
        self.plot_container = PlotContainerWidget(self.root.get_model())
        self.widget_stack.addWidget(self.plot_container)
        self.widget_stack.setCurrentIndex(
            self.widget_stack.indexOf(self.plot_container))

    def _set_window_title(self, title):
        self.setWindowTitle("{} – ndscan".format(title))

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

        self.argparser.add_argument(
            "--port-control",
            default=3251,
            type=int,
            help="TCP port for master control commands")
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

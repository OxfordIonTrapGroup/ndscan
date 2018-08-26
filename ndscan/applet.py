from artiq.applets.simple import SimpleApplet
from artiq.protocols.pc_rpc import AsyncioClient
from artiq.protocols.sync_struct import Subscriber
import asyncio
import json
import logging
import pyqtgraph
from quamash import QtWidgets, QtCore

from .plots.rolling_1d import Rolling1DPlotWidget
from .plots.xy_1d import XY1DPlotWidget

logger = logging.getLogger(__name__)


class _MainWidget(QtWidgets.QWidget):
    def __init__(self, args):
        super().__init__()
        self.args = args

        self.setWindowTitle("ndscan plot")
        self.resize(800, 500)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        self.widget_stack = QtWidgets.QStackedWidget()
        self.message_label = QtWidgets.QLabel(
            "Waiting for ndscan metadata for rid {}…".format(self.args.rid))
        self.widget_stack.addWidget(self.message_label)
        self.layout.addWidget(self.widget_stack)

        self.title_set = False
        self.plot_initialised = False

    def data_changed(self, data, mods):
        def d(name):
            return data.get("ndscan." + name, (False, None))[1]

        if not self.title_set:
            fqn = d("fragment_fqn")
            if not fqn:
                return
            self.setWindowTitle("{} – ndscan".format(fqn))
            self.title_set = True

        if not self.plot_initialised:
            axes_json = d("axes")
            if not axes_json:
                return
            axes = json.loads(axes_json)
            if len(axes) == 0:
                self.plot = Rolling1DPlotWidget()
            elif len(axes) == 1:
                self.plot = XY1DPlotWidget(axes[0], self.set_dataset)
            else:
                self.message_label.setText(
                    "{}-dimensional scans are not yet supported".format(len(axes)))
                self._show(self.message_label)
                return

            self.plot.error.connect(self._show_error)
            self.widget_stack.addWidget(self.plot)
            self._show(self.plot)

            self.plot_initialised = True

        self.plot.data_changed(data, mods)

    def _show(self, widget):
        self.widget_stack.setCurrentIndex(self.widget_stack.indexOf(widget))

    def _show_error(self, message):
        self.message_label.setText("Error: " + message)
        self._show(self.message_label)

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
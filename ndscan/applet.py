import json
import logging
import pyqtgraph

from artiq.applets.simple import SimpleApplet
from artiq.protocols.sync_struct import Subscriber
from quamash import QtWidgets, QtCore

logger = logging.getLogger(__name__)


class _XYPlotWidget(pyqtgraph.PlotWidget):
    def __init__(self):
        super().__init__()


    def data_changed(self, data, mods):
        pass


class _MainWidget(QtWidgets.QWidget):
    def __init__(self, args):
        super().__init__()
        self.args = args

        self.setWindowTitle("ndscan plot")

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
            return data.get(name, (False, None))[1]

        if not self.title_set:
            fqn = d("ndscan.fragment_fqn")
            if fqn:
                self.setWindowTitle("{} – ndscan".format(fqn))
                self.title_set = True

        if not self.plot_initialised:
            axes_json = d("ndscan.axes")
            if axes_json:
                axes = json.loads(axes_json[1])
                if len(axes) == 0:
                    # Show rolling plot.
                    pass
                elif len(axes) == 1:
                    # Show 1D plot.
                    self.plot = _XYPlotWidget()
                    self.widget_stack.addWidget(self.plot)
                    self._show(self.plot)
                else:
                    self.message_label.setText(
                        "{}-dimensional scans are not yet supported".format(len(axes)))
                    self._show(self.message_label)
                self.plot_initialised = True

        if self.plot_initialised:
            self.plot.data_changed(data, mods)

    def _show(self, widget):
        self.widget_stack.setCurrentIndex(self.widget_stack.indexOf(widget))


class NdscanApplet(SimpleApplet):
    def __init__(self):
        super().__init__(_MainWidget)
        self.argparser.add_argument("--rid", help="RID of the experiment to plot")

    def subscribe(self):
        # We want to subscribe only to the experiment-local datasets for our RID
        # (but always, even if using IPC – this can be optimised later).
        self.subscriber = Subscriber("datasets_rid_{}".format(self.args.rid),
                                     self.sub_init, self.sub_mod)
        self.loop.run_until_complete(self.subscriber.connect(
            self.args.server, self.args.port))

    def filter_mod(self, *args):
        return True


def main():
    applet = NdscanApplet()
    applet.run()

if __name__ == "__main__":
    main()

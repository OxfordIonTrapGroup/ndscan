"""Cleans up ndscan results datasets some time after experiments terminate.

ndscan_dataset_janitor maintains a connection to the artiq_master process and watches
the status of experiments in the run schedule. Once an experiment is terminated in some
fashion (enters the "deleting" status), a timer delay is started, after which all the
associated ndscan datasets (default: `ndscan.rid_xxxxx`) are removed. Any datasets of
this form for which no experiment exists anymore when the janitor process is started are
considered stale and immediately deleted.

This avoids continuous, unbounded growth of the master's dataset database, which is also
broadcast to all UI clients (such as artiq_dashboard). Typically, ndscan_dataset_janitor
should always be started in the background (together with artiq_master) when ndscan is
used.
"""

import argparse
import asyncio
import logging
import time
from typing import Optional
from sipyco import common_args, pc_rpc, sync_struct

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)

    mg = parser.add_argument_group("ARTIQ master connection details")
    mg.add_argument("--server",
                    default="::1",
                    help="hostname or IP to connect to (default: '%(default)s')")
    mg.add_argument("--port-notify",
                    default=3250,
                    type=int,
                    help="master notify port (dataset subscriptions)")
    mg.add_argument("--port-control",
                    default=3251,
                    type=int,
                    help="master control port (dataset modification RPCs)")

    dg = parser.add_argument_group("Dataset cleanup settings")
    dg.add_argument("--key-prefix",
                    default="ndscan.rid_",
                    help="prefix of the dataset tree to clean up")
    dg.add_argument("--timeout",
                    default=600,
                    type=float,
                    help="amount of time after experiment termination after which " +
                    "datasets are removed (in seconds, default: %(default)s)")

    common_args.verbosity_args(parser)

    return parser.parse_args()


class _NullSyncStruct:
    """sipyco.sync_struct target that ignores all changes to avoid memory usage in cases
    where only mods are important.
    """
    def append(self, x):
        pass

    def insert(self, i, x):
        pass

    def pop(self, i=-1):
        pass

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __getitem__(self, key):
        return self


async def run(args):
    #: Current view of all dataset keys existing on the master.
    dataset_keys = set[str]()

    #: Ordered map of pending cleanup tasks, from RID to the time.monotonic() instant at
    #: which to delete the datasets. As the timeout is the same for all RIDs, it will
    #: naturally be a FIFO queue in terms of deletion order.
    deletions = dict[int, float]()

    #: Notifies the main task (that executes the deletions) that a new task was added,
    #: or something went wrong with one of the connections.
    wake_loop = asyncio.Event()

    dataset_db: Optional[pc_rpc.AsyncioClient] = None
    dataset_sub: Optional[sync_struct.Subscriber] = None
    schedule_sub: Optional[sync_struct.Subscriber] = None

    def all_connected():
        """Return whether all master connections appear to be intact."""
        return dataset_db and dataset_sub and schedule_sub

    async def connect_sub(name: str, update_cb, disconnect_cb, track_contents: bool):
        """Connect to the given named artiq_master publisher.

        :param track_contents: If ``False``, the actual contents of the published struct
            are discarded, which e.g. avoids having to keep all the datasets in memory
            just to act on the sync_struct mods.
        """
        initial_data = asyncio.Future()

        def init(data):
            if not initial_data.done():
                initial_data.set_result(data)
            return data if track_contents else _NullSyncStruct()

        sub = sync_struct.Subscriber(name, init,
                                     lambda mod: update_cb(initial_data.result(), mod),
                                     disconnect_cb)
        while True:
            try:
                await sub.connect(args.server, args.port_notify)
                break
            except ConnectionRefusedError as e:
                logger.error(f"Connection refused for {name} publisher, retrying: %s",
                             e)
                await asyncio.sleep(5)
        logger.info(f"Connected to {name} publisher.")
        return sub, await initial_data

    # This outer loop establishes all master connections, synchronises the initial state
    # (schedules deletions for any already-orphaned experiments), and then processes
    # cleanup requests as their respective timeouts elapse. In normal use, where the
    # janitor process would run on the same host as artiq_master, there shouldn't be a
    # reason for the connections to fail (unless artiq_master is terminated). We
    # nevertheless handle this for robustness, as the process should be "set-and-forget"
    # for the user. In that case, we restart the outer loop to ensure we start from a
    # coherent view of the dataset/schedule state.
    while True:
        # Establish all connections.
        if dataset_db is None:
            dataset_db = pc_rpc.AsyncioClient()
            while True:
                try:
                    await dataset_db.connect_rpc(args.server, args.port_control,
                                                 "master_dataset_db")
                    break
                except ConnectionRefusedError as e:
                    logger.error(
                        "Connection refused for dataset_db RPC service, retrying: %s",
                        e)
                    await asyncio.sleep(5)
            logger.info("Connected to dataset_db RPC service.")

        if dataset_sub is None:

            def datasets_disconnected():
                nonlocal dataset_sub
                dataset_sub = None
                wake_loop.set()

            def datasets_updated(_data, mod):
                is_del = mod["action"] == sync_struct.ModAction.delitem.value
                is_set = mod["action"] == sync_struct.ModAction.setitem.value
                if not is_set or is_del:
                    return
                if mod["path"]:
                    logger.warning(
                        "Non-empty path in {set,del}item sync_struct mod not " +
                        "expected for datasets: %s", mod)
                    return
                if is_set:
                    dataset_keys.add(mod["key"])
                else:
                    dataset_keys.remove(mod["key"])

            dataset_sub, initial_datasets = await connect_sub("datasets",
                                                              datasets_updated,
                                                              datasets_disconnected,
                                                              track_contents=False)
            dataset_keys.clear()
            dataset_keys |= initial_datasets.keys()

        if schedule_sub is None:

            def schedule_disconnected():
                nonlocal schedule_sub
                schedule_sub = None
                wake_loop.set()

            def schedule_updated(data, _mod):
                for rid, item in data.items():
                    if item["status"] == "deleting":
                        logger.debug(f"RID {rid} being deleted, scheduling cleanup.")
                        deletions[rid] = time.monotonic() + args.timeout
                        wake_loop.set()

            schedule_sub, initial_schedule = await connect_sub("schedule",
                                                               schedule_updated,
                                                               schedule_disconnected,
                                                               track_contents=True)
            schedule_updated(initial_schedule, None)

        # Take into account current state after reconnecting, where some experiments
        # might have disappeared without us noticing that they were deleted.
        preexisting_data_rids = set()
        for key in dataset_keys:
            if key.startswith(args.key_prefix):
                rest = key[len(args.key_prefix):]
                rid = rest[:rest.index(".")]
                preexisting_data_rids.add(rid)
        for rid in preexisting_data_rids:
            try:
                rid = int(rid)
            except ValueError:
                # Old ndscan/…? Shouldn't usually happen.
                logger.warning(
                    "Key fragment '%s' in pre-existing datasets under '%s' " +
                    "does not look like a rid; ignoring.", rid, args.key_prefix)
                continue
            if rid not in deletions:
                # Execute cleanup immediately so restarting the janitor process provides
                # a quick way for the user to get rid of all stale results (e.g. if a
                # huge amount of data mistakenly accumulated).
                logger.info(
                    "Found datasets for RID %s which is no longer known, will " +
                    "immediately clean up", rid)
                deletions[rid] = time.monotonic()
                wake_loop.set()

        # Execute pending deletion requests.
        while all_connected():
            while deletions and all_connected():
                rid, timestamp = deletions.popitem()
                if (delay := timestamp - time.monotonic()) > 0:
                    await asyncio.sleep(delay)
                logger.info(f"ndscan RID {rid} timed out, cleaning up datasets.")
                prefix = f"{args.key_prefix}{rid}."
                to_remove = [k for k in dataset_keys if k.startswith(prefix)]
                logger.debug("Deleting datasets: %s", to_remove)
                for key in to_remove:
                    try:
                        await dataset_db.delete(key)
                    except Exception:
                        logger.exception(
                            f"Failed to delete dataset '{key}', reconnecting.")
                        dataset_db = None
                        break
            if all_connected():
                wake_loop.clear()
                await wake_loop.wait()


def main():
    args = parse_args()
    common_args.init_logger_from_args(args)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.warning("Termination requested, exiting…")


if __name__ == "__main__":
    main()

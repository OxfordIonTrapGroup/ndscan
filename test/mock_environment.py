"""
Helpers for unit-testing HasEnvironment code.
"""

import copy
import unittest
import unittest.mock

from artiq.language.core import TerminationRequested
from artiq.language.environment import ProcessArgumentManager
from artiq.master.worker_db import DatasetManager, DeviceManager
from sipyco.sync_struct import process_mod


class MockDatasetDB:
    def __init__(self):
        self.data = dict()

    def get(self, key):
        return self.data[key][1]

    def update(self, mod):
        # Copy mod before applying to avoid sharing references to objects
        # between this and the DatasetManager, which would lead to mods being
        # applied twice.
        process_mod(self.data, copy.deepcopy(mod))

    def delete(self, key):
        del self.data[key]


class MockExamineDatasetMgr:
    def __init__(self, db):
        self.db = db

    def get(self, key, archive=False):
        return self.db.get(key)


class MockScheduler:
    def __init__(self):
        self.rid = 0
        self.num_check_pause_calls = 0
        self.num_check_pause_calls_until_termination = 0

    def _should_terminate(self) -> bool:
        if self.num_check_pause_calls_until_termination == 0:
            # Limit disabled
            return False
        return (self.num_check_pause_calls >=
                self.num_check_pause_calls_until_termination)

    def check_pause(self) -> bool:
        self.num_check_pause_calls += 1
        return self._should_terminate()

    def pause(self):
        if self._should_terminate():
            raise TerminationRequested


class MockDeviceDB:
    def __init__(self):
        self.devices = {"core": {"type": "dummy"}}

    def get(self, key):
        return self.devices[key]

    def get_device_db(self):
        return self.devices


class HasEnvironmentCase(unittest.TestCase):
    def setUp(self):
        self.dataset_db = MockDatasetDB()
        self.device_db = MockDeviceDB()
        self.ccb = unittest.mock.Mock()
        self.core = unittest.mock.Mock()
        self.scheduler = MockScheduler()
        self.device_mgr = DeviceManager(self.device_db,
                                        virtual_devices={
                                            "ccb": self.ccb,
                                            "core": self.core,
                                            "scheduler": self.scheduler
                                        })

    def create(self, klass, *args, env_args=None, like_examine=False, **kwargs):
        dataset_mgr_cls = MockExamineDatasetMgr if like_examine else DatasetManager
        arg_mgr = ProcessArgumentManager(env_args or {})
        return klass((self.device_mgr, dataset_mgr_cls(self.dataset_db), arg_mgr, None),
                     *args, **kwargs)


class ExpFragmentCase(HasEnvironmentCase):
    def create(self, klass, *args, **kwargs):
        fragment = super().create(klass, [], *args, **kwargs)
        fragment.init_params()
        return fragment

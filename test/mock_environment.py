"""
Helpers for unit-testing HasEnvironment code.
"""

import copy
import unittest
import unittest.mock

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


class MockScheduler:
    def __init__(self):
        self.rid = 0

    def check_pause(self):
        return False

    def pause(self):
        pass


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
        self.dataset_mgr = DatasetManager(self.dataset_db)
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

    def create(self, klass, *args, env_args=None, **kwargs):
        return klass(
            (self.device_mgr, self.dataset_mgr, ProcessArgumentManager(
                env_args or {}), None), *args, **kwargs)


class ExpFragmentCase(HasEnvironmentCase):
    def create(self, klass, *args, **kwargs):
        fragment = super().create(klass, [], *args, **kwargs)
        fragment.init_params()
        return fragment

import os

device_db = {
    "core": {
        "type": "local",
        "module": "artiq.coredevice.core",
        "class": "CoreEmulator",
        "arguments": {
            "libartiq_emulator_path": os.getenv("LIBARTIQ_EMULATOR"),
            "ref_period": 1e-9
        }
    },
}

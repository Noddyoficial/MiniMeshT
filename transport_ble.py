"""
# Select a BLE radio on the PC for for use by bluetoothctl (apparently it can only use one at a time).
sudo btmgmt info
bluetoothctl select 11:22:33:44:55:66

# Readies the device for pairing.
bluetoothctl connect 77:88:99:AA:BB:CC

# Actually pairs. Will prompt for pin.
bluetoothctl pair 77:88:99:AA:BB:CC

# Sets the device to re-pair automatically when it is turned on, which eliminates the need to pair all over again.
bluetoothctl trust 77:88:99:AA:BB:CC

# Check whether paired and connected.
bluetoothctl devices
bluetoothctl info 77:88:99:AA:BB:CC

# Disconnect bluez from the device. Sometimes bleak needs this to own the connection.
bluetoothctl disconnect 77:88:99:AA:BB:CC
"""


import asyncio
from bleak import BleakClient
import logging
logger = logging.getLogger(__name__)

# Meshtastic BLE UUIDs
TO_RADIO_UUID = "f75c76d2-129e-4dad-a1dd-7866124401e7"
FROM_RADIO_UUID = "2c55e69e-4993-11ed-b878-0242ac120002"
FROM_NUM_UUID = "ed9da18c-a800-4f66-a670-aa7547e34453"


class BLETransport:
    def __init__(self, address, adapter=None):
        self.address = address
        self.adapter = adapter
        self.client = None
        self._in_q = asyncio.Queue()
        self._drain_lock = asyncio.Lock()
        self._error = None

    async def _drain(self):
        if self.client is None:
            return
        async with self._drain_lock:
            try:
                while True:
                    data = await self.client.read_gatt_char(FROM_RADIO_UUID)
                    if not data:
                        break
                    await self._in_q.put(data)
            except Exception as e:
                self._error = e
                logger.error("ble drain error: %s", e)
                try:
                    self._in_q.put_nowait(None)
                except Exception:
                    pass

    async def start(self):
        # Bleak expects the address as the first positional arg
        # Meshtastic devices typically use random address type on BLE.
        # Reset state to avoid stale sentinels/data from prior sessions
        self._in_q = asyncio.Queue()
        self._error = None
        client = BleakClient(self.address, adapter=self.adapter, address_type="random")
        await client.connect()
        self.client = client
        logger.debug("BLETransport.start: connected to %s (adapter=%s)", self.address, self.adapter)

        def from_num_handler(_sender, _data):
            asyncio.create_task(self._drain())

        await client.start_notify(FROM_NUM_UUID, from_num_handler)

    async def send(self, payload):
        if self.client is None:
            return
        try:
            await self.client.write_gatt_char(TO_RADIO_UUID, payload, response=True)
            asyncio.create_task(self._drain())
        except Exception as e:
            self._error = e
            logger.error("ble send error: %s", e)
            try:
                self._in_q.put_nowait(None)
            except Exception:
                pass

    async def recv(self):
        item = await self._in_q.get()
        if item is None:
            raise ConnectionError(self._error or "ble transport error")
        return item

    async def close(self):
        if self.client is None:
            return
        try:
            try:
                await self.client.stop_notify(FROM_NUM_UUID)
            except Exception:
                pass
            await self.client.disconnect()
        finally:
            self.client = None

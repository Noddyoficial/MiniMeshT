import asyncio
import serial_asyncio
import logging
logger = logging.getLogger(__name__)

MAGIC0 = 0x94
MAGIC1 = 0xC3
READ_CHUNK = 4096


def encode_frame(payload):
    n = len(payload)
    header = bytes([MAGIC0, MAGIC1, (n >> 8) & 0xFF, n & 0xFF])
    return header + payload


class SerialTransport:
    def __init__(self, port, baudrate=115200):
        self.port = port
        self.baudrate = baudrate
        self.reader = None
        self.writer = None
        self._in_q = asyncio.Queue()
        self._buf = bytearray()
        self._reader_task = None
        self._error = None

    async def start(self):
        # Reset state to avoid stale sentinels/data from prior sessions
        self._in_q = asyncio.Queue()
        self._buf = bytearray()
        self._error = None
        self.reader, self.writer = await serial_asyncio.open_serial_connection(
            url=self.port, baudrate=self.baudrate
        )
        self._reader_task = asyncio.create_task(self._reader_loop(), name="serial-reader")
        logger.debug("SerialTransport.start: opened %s @ %s", self.port, self.baudrate)

    async def _reader_loop(self):
        assert self.reader is not None
        r = self.reader
        try:
            while True:
                data = await r.read(READ_CHUNK)
                if not data:
                    await asyncio.sleep(0)
                    continue
                self._buf.extend(data)
                while True:
                    start = -1
                    for i in range(len(self._buf) - 1):
                        if self._buf[i] == MAGIC0 and self._buf[i + 1] == MAGIC1:
                            start = i
                            break
                    if start == -1:
                        if len(self._buf) > 1:
                            self._buf[:] = self._buf[-1:]
                        break
                    if start > 0:
                        del self._buf[:start]
                    if len(self._buf) < 4:
                        break
                    length = (self._buf[2] << 8) | self._buf[3]
                    total = 4 + length
                    if len(self._buf) < total:
                        break
                    payload = bytes(self._buf[4:total])
                    await self._in_q.put(payload)
                    del self._buf[:total]
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Record error and notify receivers by placing a sentinel
            self._error = e
            logger.error("serial read error: %s", e)
            try:
                self._in_q.put_nowait(None)
            except Exception:
                pass

    async def send(self, payload):
        if self.writer is None:
            return
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError("payload must be bytes")
        try:
            self.writer.write(encode_frame(payload))
            await self.writer.drain()
        except Exception as e:
            # Treat as an error condition; surface to receiver path
            self._error = e
            logger.error("serial write error: %s", e)
            try:
                self._in_q.put_nowait(None)
            except Exception:
                pass

    async def recv(self):
        item = await self._in_q.get()
        if item is None:
            raise ConnectionError(self._error or "serial transport error")
        return item

    async def close(self):
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except Exception:
                pass
            self._reader_task = None
        if self.writer is not None:
            try:
                self.writer.close()
            except Exception:
                pass
            self.writer = None
        self.reader = None

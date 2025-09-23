import asyncio
import logging

logger = logging.getLogger(__name__)

MAGIC0 = 0x94
MAGIC1 = 0xC3
READ_CHUNK = 4096


def encode_frame(payload):
    n = len(payload)
    header = bytes([MAGIC0, MAGIC1, (n >> 8) & 0xFF, n & 0xFF])
    return header + payload


class TCPTransport:
    def __init__(self, host, port=4403):
        self.host = host
        self.port = int(port)
        self.reader = None
        self.writer = None
        self._recv_q = asyncio.Queue()
        self._buf = bytearray()
        self._reader_task = None
        self._error = None
        self._closing = False

    async def start(self):
        self._closing = False
        self._error = None
        self._buf = bytearray()
        self._recv_q = asyncio.Queue()
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
        except Exception as e:
            self._error = e
            raise
        self.reader = reader
        self.writer = writer
        self._reader_task = asyncio.create_task(self._reader_loop(), name="tcp-reader")
        logger.debug("TCPTransport.start: connected to %s:%s", self.host, self.port)

    async def _reader_loop(self):
        assert self.reader is not None
        r = self.reader
        try:
            while True:
                data = await r.read(READ_CHUNK)
                if not data:
                    if self._closing:
                        return
                    self._error = ConnectionError("tcp connection closed")
                    logger.error("tcp read error: connection closed")
                    try:
                        self._recv_q.put_nowait(None)
                    except Exception:
                        pass
                    return
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
                    await self._recv_q.put(payload)
                    del self._buf[:total]
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self._closing:
                return
            self._error = e
            logger.error("tcp read error: %s", e)
            try:
                self._recv_q.put_nowait(None)
            except Exception:
                pass

    async def send(self, payload):
        if self.writer is None:
            return
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError("payload must be bytes")
        try:
            frame = encode_frame(bytes(payload))
            self.writer.write(frame)
            await self.writer.drain()
        except Exception as e:
            if self._closing:
                return
            self._error = e
            logger.error("tcp write error: %s", e)
            try:
                self._recv_q.put_nowait(None)
            except Exception:
                pass

    async def recv(self):
        item = await self._recv_q.get()
        if item is None:
            raise ConnectionError(self._error or "tcp transport error")
        return item

    async def close(self):
        self._closing = True
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
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
        self.reader = None


import asyncio

import pytest

import transport_tcp
from transport_tcp import TCPTransport, encode_frame, MAGIC0, MAGIC1


class FakeReader:
    def __init__(self, queue):
        self._queue = queue

    async def read(self, _size):
        data = await self._queue.get()
        if data is None:
            return b""
        return data


class FakeWriter:
    def __init__(self, queue):
        self._queue = queue
        self._closed = False

    def write(self, data):
        if self._closed:
            return
        self._queue.put_nowait(bytes(data))

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._queue.put_nowait(None)

    async def wait_closed(self):
        await asyncio.sleep(0)


class FakeTCPDevice:
    def __init__(self):
        self.received = []
        self.port = 4403
        self._incoming = None
        self._outgoing = None
        self._consumer_task = None

    async def start(self):
        self.received = []
        self._incoming = asyncio.Queue()
        self._outgoing = asyncio.Queue()
        self._consumer_task = asyncio.create_task(self._consume_outgoing())

    async def stop(self):
        if self._consumer_task is not None:
            self._outgoing.put_nowait(None)
            try:
                await self._consumer_task
            except Exception:
                pass
            self._consumer_task = None
        self._incoming = None
        self._outgoing = None

    async def _consume_outgoing(self):
        buffer = bytearray()
        while True:
            chunk = await self._outgoing.get()
            if chunk is None:
                break
            buffer.extend(chunk)
            while True:
                if len(buffer) < 4:
                    break
                if buffer[0] != MAGIC0 or buffer[1] != MAGIC1:
                    del buffer[0]
                    continue
                length = (buffer[2] << 8) | buffer[3]
                total = 4 + length
                if len(buffer) < total:
                    break
                payload = bytes(buffer[4:total])
                self.received.append(payload)
                del buffer[:total]

    async def enqueue(self, payload):
        if self._incoming is None:
            raise RuntimeError("FakeTCPDevice not started")
        await self._incoming.put(encode_frame(bytes(payload)))

    async def close_client(self):
        if self._incoming is None:
            return
        await self._incoming.put(None)

    async def fake_open_connection(self, host, port):
        if self._incoming is None or self._outgoing is None:
            raise RuntimeError("FakeTCPDevice not started")
        reader = FakeReader(self._incoming)
        writer = FakeWriter(self._outgoing)
        return reader, writer


async def _run_with_fake_transport(test_coro):
    server = FakeTCPDevice()
    await server.start()
    original_open_connection = transport_tcp.asyncio.open_connection
    transport_tcp.asyncio.open_connection = server.fake_open_connection
    try:
        await test_coro(server)
    finally:
        transport_tcp.asyncio.open_connection = original_open_connection
        await server.stop()


def test_tcp_transport_receives_and_sends():
    async def _run(server):
        #
        # Arrange
        #
        transport = TCPTransport("127.0.0.1", server.port)
        try:
            await transport.start()
            await asyncio.wait_for(server.enqueue(b"one"), timeout=1.0)
            await asyncio.wait_for(server.enqueue(b"two"), timeout=1.0)

            #
            # Act
            #
            data1 = await asyncio.wait_for(transport.recv(), timeout=1.0)
            data2 = await asyncio.wait_for(transport.recv(), timeout=1.0)
            await transport.send(b"payload")

            #
            # Assert
            #
            assert data1 == b"one"
            assert data2 == b"two"
            assert server.received[-1] == b"payload"
        finally:
            await transport.close()

    asyncio.run(_run_with_fake_transport(_run))


def test_tcp_transport_closed_connection_surfaces():
    async def _run(server):
        #
        # Arrange
        #
        transport = TCPTransport("127.0.0.1", server.port)
        try:
            await transport.start()
            await server.close_client()

            #
            # Act
            #
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(transport.recv(), timeout=1.0)

            #
            # Assert
            #
            assert transport._error is not None
        finally:
            await transport.close()

    asyncio.run(_run_with_fake_transport(_run))


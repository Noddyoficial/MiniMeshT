import asyncio
import base64
import json
import os
import time

from mesht_device import MeshtDevice, FROMRADIO_SCHEMA, TORADIO_SCHEMA, NAMES_TO_PORTNUMS, USER_SCHEMA
import pb
from mesht_db import MeshtDb


class FakeTransport:
    def __init__(self):
        self._recv_q = asyncio.Queue()
        self.started = False
        self.closed = False
        self.sent = []

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True

    async def send(self, data):
        self.sent.append(data)
        tr = pb.decode(data, TORADIO_SCHEMA)
        assert isinstance(tr, dict)

        want_config_id = tr.get("want_config_id")
        if want_config_id:
            packets = [
                {"channel": {"index": 0, "settings": {"name": "General"}}},
                {"my_info": {"my_node_num": 1}},
                {"node_info": {"num": 1, "user": {"long_name": "NodeOne", "short_name": "n1"}}},
                {"config_complete_id": want_config_id},
            ]
            for fr in packets:
                await self._recv_q.put(pb.encode(fr, FROMRADIO_SCHEMA))

    async def recv(self):
        return await self._recv_q.get()


def test_radio_db_persists_ingested_text(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        await db.start()

        # Inject one text FromRadio
        fr = {"packet": {"from": 1, "channel": 0, "rx_time": int(time.time()), "decoded": {"portnum": 1, "payload": b"hi"}}}
        await ft._recv_q.put(pb.encode(fr, FROMRADIO_SCHEMA))

        # Drain DB until we see that text frame
        async def _drain_until_text():
            while True:
                frm = await db.next_fromradio()
                pkt = (frm or {}).get("packet") or {}
                dec = pkt.get("decoded") or {}
                if dec.get("portnum") == 1 and (pkt.get("channel") or 0) == 0:
                    break
        await asyncio.wait_for(_drain_until_text(), timeout=1.0)

        #
        # Assert
        #
        # Messages persisted (per-channel file)
        msgs_path = os.path.join(str(tmp_path), "messages.0.jsonl")
        assert os.path.exists(msgs_path)
        with open(msgs_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        assert any('"type":"FromRadio"' in ln for ln in lines)

        await db.close()

    asyncio.run(_run())


def test_meshtdb_updates_rx_snr_from_packets(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        await db.start()

        async def _drain_startup():
            while True:
                fr = await asyncio.wait_for(db.next_fromradio(), timeout=1.0)
                if isinstance(fr, dict) and fr.get("config_complete_id"):
                    return

        await _drain_startup()

        node_num = 0xCAFEBABE
        snr = -7.25
        frame = {
            "packet": {
                "from": node_num,
                "to": 0,
                "rx_snr": snr,
                "rx_rssi": -113,
                "decoded": {
                    "portnum": NAMES_TO_PORTNUMS["TEXT_MESSAGE_APP"],
                    "payload": b"hello",
                },
            }
        }
        await ft._recv_q.put(pb.encode(frame, FROMRADIO_SCHEMA))

        #
        # Act
        #
        await asyncio.wait_for(db.next_fromradio(), timeout=1.0)

        #
        # Assert
        #
        node_hex = f"{node_num & 0xFFFFFFFF:08x}"
        node = db.node_info.get(node_hex)
        assert node is not None
        assert node.rx_snr == snr

        await db.close()

    asyncio.run(_run())


def test_meshtdb_updates_rx_rssi_from_packets(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        await db.start()

        async def _drain_startup():
            while True:
                fr = await asyncio.wait_for(db.next_fromradio(), timeout=1.0)
                if isinstance(fr, dict) and fr.get("config_complete_id"):
                    return

        await _drain_startup()

        node_num = 0xD00DFEED
        rssi = -101
        frame = {
            "packet": {
                "from": node_num,
                "to": 0,
                "rx_rssi": rssi,
                "decoded": {
                    "portnum": NAMES_TO_PORTNUMS["TEXT_MESSAGE_APP"],
                    "payload": b"ping",
                },
            }
        }
        await ft._recv_q.put(pb.encode(frame, FROMRADIO_SCHEMA))

        #
        # Act
        #
        await asyncio.wait_for(db.next_fromradio(), timeout=1.0)

        #
        # Assert
        #
        node_hex = f"{node_num & 0xFFFFFFFF:08x}"
        node = db.node_info.get(node_hex)
        assert node is not None
        assert node.rx_rssi == rssi

        await db.close()

    asyncio.run(_run())


def test_meshtdb_startup_channel_seen(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        #
        # Act and assert: after start(), we should observe a channel frame via next_fromradio
        #
        await db.start()
        async def _drain_until_channel():
            while True:
                fr = await db.next_fromradio()
                if isinstance(fr, dict) and fr.get("channel"):
                    return
        await asyncio.wait_for(_drain_until_channel(), timeout=1.0)
        await db.close()

    asyncio.run(_run())


def test_meshtdb_persists_startup_nodeinfo_log(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        #
        # Act
        #
        await db.start()
        # Drain startup frames so nodeinfo file is written
        async def _drain_startup():
            while True:
                fr = await asyncio.wait_for(db.next_fromradio(), timeout=1.0)
                if isinstance(fr, dict) and fr.get("config_complete_id"):
                    break
        await _drain_startup()

        #
        # Assert
        #
        node_path = os.path.join(str(tmp_path), "nodeinfo.jsonl")
        with open(node_path, "r", encoding="utf-8") as f:
            entries = [json.loads(ln) for ln in f if ln.strip()]
        assert entries and all("raw_packet" in e for e in entries)
        # Each entry has timestamps and friendly fields
        assert all(isinstance(e.get("ts"), int) for e in entries)
        assert all(isinstance(e.get("tsh"), str) and e.get("tsh") for e in entries)
        assert any((e.get("ID") and e.get("short_name") == "n1" and e.get("long_name") == "NodeOne") for e in entries)

        await db.close()

    asyncio.run(_run())


def test_radio_db_get_messages_filters_and_orders(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))
        await db.start()

        now = int(time.time())
        msgs = [
            {"packet": {"from": 2, "channel": 0, "rx_time": now - 10, "decoded": {"portnum": 1, "payload": b"m1"}}},
            {"packet": {"from": 3, "channel": 1, "rx_time": now - 8, "decoded": {"portnum": 1, "payload": b"m2"}}},
            {"packet": {"from": 2, "channel": 0, "rx_time": now - 5, "decoded": {"portnum": 1, "payload": b"m3"}}},
        ]
        for fr in msgs:
            await ft._recv_q.put(pb.encode(fr, FROMRADIO_SCHEMA))

        # Drain frames until all three text messages are ingested
        async def _drain_texts():
            seen = 0
            while seen < len(msgs):
                fr = await db.next_fromradio()
                pkt = (fr or {}).get("packet") or {}
                dec = pkt.get("decoded") or {}
                if dec.get("portnum") == 1:
                    seen += 1
        await asyncio.wait_for(_drain_texts(), timeout=1.0)

        #
        # Act
        #
        last_two = db.get_messages(channel=0)

        #
        # Assert
        #
        assert len(last_two) == 2
        texts = [ m.get("text") for m in last_two ]
        assert texts == ["m1", "m3"]

        await db.close()

    asyncio.run(_run())


def test_radio_db_send_text_logs(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        await db.start()
        # Drain startup frames so our node/user metadata is known
        async def _drain_startup():
            while True:
                fr = await asyncio.wait_for(db.next_fromradio(), timeout=1.0)
                if isinstance(fr, dict) and fr.get("config_complete_id"):
                    break
        await _drain_startup()
        # channel 0 exists due to FakeTransport start handshake
        await db.send_text("hello", 0)

        #
        # Assert
        #
        with open(os.path.join(str(tmp_path), "messages.0.jsonl"), "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        assert any('"type":"ToRadio"' in ln for ln in lines)
        # The ToRadio entry should use our node's name when known
        import json
        toradio_lines = [json.loads(ln) for ln in lines if '"type":"ToRadio"' in ln]
        assert toradio_lines, "expected at least one ToRadio entry"
        last = toradio_lines[-1]
        assert last.get("sender_long_name") == "NodeOne"
        assert last.get("sender_short_name") == "n1"
        # Human readable timestamp should be present
        assert isinstance(last.get("tsh"), str) and last.get("tsh")
        # 'from' should be hex-formatted string now
        assert isinstance(last.get("from"), str)
        assert len(last.get("from")) == 8

        await db.close()

    asyncio.run(_run())


def test_nodeinfo_compaction_keeps_first_changes_newest(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        await db.start()
        # Drain startup frames so the initial node_info from handshake is ingested
        async def _drain_startup():
            while True:
                fr = await asyncio.wait_for(db.next_fromradio(), timeout=1.0)
                if isinstance(fr, dict) and fr.get("config_complete_id"):
                    break
        await _drain_startup()

        # Build a sequence of node_info updates for the same node (ID 00000001)
        frames = [
            {"node_info": {"num": 1, "user": {"long_name": "NodeOne", "short_name": "n1", "public_key": b"a"}}},
            {"node_info": {"num": 1, "user": {"long_name": "NodeOne", "short_name": "n1", "public_key": b"a"}}},
            {"node_info": {"num": 1, "user": {"long_name": "NodeOne", "short_name": "n1x", "public_key": b"a"}}},
            {"node_info": {"num": 1, "user": {"long_name": "NodeOneX", "short_name": "n1x", "public_key": b"a"}}},
            {"node_info": {"num": 1, "user": {"long_name": "NodeOneX", "short_name": "n1x", "public_key": b"b"}}},
            {"node_info": {"num": 1, "user": {"long_name": "NodeOneX", "short_name": "n1x", "public_key": b"b"}}},
        ]
        for fr in frames:
            await ft._recv_q.put(pb.encode(fr, FROMRADIO_SCHEMA))

        # Drain until we've ingested all node_info frames
        async def _drain(n):
            seen = 0
            while seen < n:
                fr = await db.next_fromradio()
                if isinstance(fr, dict) and fr.get("node_info"):
                    seen += 1
        await asyncio.wait_for(_drain(len(frames)), timeout=1.0)

        #
        # Assert
        #
        node_path = os.path.join(str(tmp_path), "nodeinfo.jsonl")
        with open(node_path, "r", encoding="utf-8") as f:
            entries = [json.loads(ln) for ln in f if ln.strip()]

        # Filter for our node and map to (short,long,public_key_base64)
        entries = [e for e in entries if e.get("ID") == "00000001"]
        def _tup(e):
            return (
                e.get("short_name") or "",
                e.get("long_name") or "",
                e.get("public_key") or "",
            )

        got = [ _tup(e) for e in entries ]
        expected = [
            ("n1", "NodeOne", ""),                 # first seen during startup handshake
            ("n1", "NodeOne", "YQ=="),              # change: public_key a
            ("n1x", "NodeOne", "YQ=="),            # change: short_name
            ("n1x", "NodeOneX", "YQ=="),           # change: long_name
            ("n1x", "NodeOneX", "Yg=="),           # newest (and change) kept
        ]
        assert got == expected

        await db.close()

    asyncio.run(_run())


def test_meshtdb_ingests_nodeinfo_packets(tmp_path):
    async def _run():
        #
        # Arrange
        #
        ft = FakeTransport()
        dev = MeshtDevice(ft)
        db = MeshtDb(dev, str(tmp_path))

        await db.start()

        async def _drain_startup():
            while True:
                fr = await asyncio.wait_for(db.next_fromradio(), timeout=1.0)
                if isinstance(fr, dict) and fr.get("config_complete_id"):
                    return
        await _drain_startup()

        node_num = 0x12345678
        now = int(time.time())
        public_key = b"\x01" * 32
        payload = pb.encode({
            "id": "!unit-test",
            "long_name": "Unit Test",
            "short_name": "ut",
            "public_key": public_key,
        }, USER_SCHEMA)
        frame = {
            "packet": {
                "from": node_num,
                "to": 0,
                "rx_time": now,
                "rx_snr": 9.5,
                "rx_rssi": -107,
                "decoded": {
                    "portnum": NAMES_TO_PORTNUMS["NODEINFO_APP"],
                    "payload": payload,
                },
            }
        }
        await ft._recv_q.put(pb.encode(frame, FROMRADIO_SCHEMA))

        #
        # Act
        #
        await asyncio.wait_for(db.next_fromradio(), timeout=1.0)

        #
        # Assert
        #
        node_hex = f"{node_num & 0xFFFFFFFF:08x}"
        node = db.node_info.get(node_hex)
        assert node is not None
        assert node.short_name == "ut"
        assert node.long_name == "Unit Test"
        assert node.node_id == node_hex
        assert node.user_id == "!unit-test"
        pk_b64 = base64.b64encode(public_key).decode("ascii")
        assert node.public_key == pk_b64
        assert node.last_heard == now

        node_path = os.path.join(str(tmp_path), "nodeinfo.jsonl")
        with open(node_path, "r", encoding="utf-8") as f:
            entries = [json.loads(ln) for ln in f if ln.strip()]
        matching = [e for e in entries if e.get("ID") == node_hex]
        assert matching
        assert matching[-1].get("public_key") == pk_b64
        assert matching[-1].get("user_id") == "!unit-test"
        assert matching[-1].get("last_heard") == now
        assert matching[-1].get("rx_snr") == 9.5
        assert matching[-1].get("rx_rssi") == -107

        await db.close()

        # Assert reload uses stored fields rather than raw packets
        dev2 = MeshtDevice(FakeTransport())
        db2 = MeshtDb(dev2, str(tmp_path))
        cached = db2.node_info.get(node_hex)
        assert cached is not None
        assert cached.short_name == "ut"
        assert cached.long_name == "Unit Test"
        assert cached.node_id == node_hex
        assert cached.user_id == "!unit-test"
        assert cached.public_key == pk_b64
        assert cached.last_heard is None
        assert cached.rx_snr is None
        assert cached.rx_rssi is None

    asyncio.run(_run())

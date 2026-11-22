#!/usr/bin/env python3
"""Arrow IPC Stream sender — emits valid streams and deliberately bad messages.

Two transports:

  file   write a self-contained .arrow stream to disk (for the web upload path)
  ws     connect to the inspector's WebSocket and push a stream frame by frame

Scenarios (--scenario):

  good            normal nested stream (null / [] / "" all present)
  dict-lifecycle  dictionary replacement + delta across batches
  bad-offsets     non-monotonic list offsets (pyarrow accepts silently; the
                  inspector's physical validator must flag it)
  undefined-dict  record batch whose dictionary message was removed
  truncated       stream cut in the middle of a batch body
  framing         garbage metadata length prefix

Usage:
  sender.py file --scenario good --out sample.arrow
  sender.py ws  --scenario good --url ws://127.0.0.1:8000/ws/stream
"""
from __future__ import annotations

import argparse
import asyncio
import json
import struct
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

sys.path.insert(0, "/workspace/arrow-inspector/backend")
from app.flatbuf import (  # noqa: E402
    parse_message_metadata,
    HEADER_RECORD_BATCH,
    HEADER_DICTIONARY_BATCH,
)


def _stream_bytes(batches, schema, options=None):
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema, options=options)
    for b in batches:
        writer.write_batch(b)
    writer.close()
    return sink.getvalue().to_pybytes()


def scenario_good():
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("label", pa.string()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("metrics", pa.list_(pa.int64())),
        pa.field(
            "payload",
            pa.struct([
                pa.field("kind", pa.string()),
                pa.field("weight", pa.float64()),
                pa.field("active", pa.bool_()),
            ]),
        ),
    ])
    b1 = pa.record_batch([
        pa.array([1, None, 3, 4], pa.int32()),
        pa.array(["alpha", "", None, "delta"], pa.string()),
        pa.array([["x", "y"], [], None, ["z"]], pa.list_(pa.string())),
        pa.array([[10, 20], [], None, [30, 40, 50]], pa.list_(pa.int64())),
        pa.array([
            {"kind": "a", "weight": 1.5, "active": True},
            None,
            {"kind": None, "weight": 2.5, "active": False},
            {"kind": "d", "weight": None, "active": True},
        ]),
    ], schema=schema)
    b2 = pa.record_batch([
        pa.array([5, 6], pa.int32()),
        pa.array(["echo", "∑ unicode"], pa.string()),
        pa.array([["nested", "deep"], []], pa.list_(pa.string())),
        pa.array([None, [1]], pa.list_(pa.int64())),
        pa.array([
            {"kind": "e", "weight": 0.0, "active": False},
            {"kind": "f", "weight": 9.9, "active": None},
        ]),
    ], schema=schema)
    return schema, [b1, b2]


def scenario_dict_lifecycle():
    d = pa.dictionary(pa.int8(), pa.string())
    schema = pa.schema([pa.field("category", d), pa.field("n", pa.int32())])
    b1 = pa.record_batch([
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1, 0], pa.int8()), pa.array(["red", "blue"])),
        pa.array([1, 2, 3], pa.int32()),
    ], schema=schema)
    # replacement dictionary with different values at the same indices
    b2 = pa.record_batch([
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1, 1], pa.int8()), pa.array(["green", "yellow"])),
        pa.array([4, 5, 6], pa.int32()),
    ], schema=schema)
    # growing dictionary lets the writer emit a delta under delta options
    b3 = pa.record_batch([
        pa.DictionaryArray.from_arrays(
            pa.array([0, 2], pa.int8()), pa.array(["green", "yellow", "violet"])),
        pa.array([7, 8], pa.int32()),
    ], schema=schema)
    opts = ipc.IpcWriteOptions(emit_dictionary_deltas=True)
    return schema, [b1, b2, b3], opts


def _frame(metadata: bytes, body: bytes) -> bytes:
    return (b"\xff\xff\xff\xff" + struct.pack("<i", len(metadata))
            + metadata + body)


def _iter_framed(raw: bytes):
    """Yield (start, header_pos, meta_len, header, body_start, end)."""
    pos = 0
    while pos < len(raw):
        start = pos
        marker = struct.unpack_from("<i", raw, pos)[0]
        if marker == -1:
            meta_len = struct.unpack_from("<i", raw, pos + 4)[0]
            hpos = pos + 8
        else:
            meta_len = marker
            hpos = pos + 4
        if meta_len == 0:
            break
        h = parse_message_metadata(bytes(raw[hpos:hpos + meta_len]))
        body_start = (hpos + meta_len + 7) & ~7
        end = (body_start + h.body_length + 7) & ~7
        yield start, hpos, meta_len, h, body_start, end
        pos = end


def scenario_bad_offsets():
    schema = pa.schema([pa.field("tags", pa.list_(pa.int32()))])
    batch = pa.record_batch(
        [pa.array([[1, 2], [3], [4, 5, 6]], pa.list_(pa.int32()))],
        schema=schema,
    )
    raw = bytearray(_stream_bytes([batch], schema))
    for _, _, _, h, body_start, _ in _iter_framed(raw):
        if h.header_type == HEADER_RECORD_BATCH:
            off_desc = h.record_batch.buffers[1]  # validity, offsets, values
            # break monotonicity: offset[2] smaller than offset[1]
            struct.pack_into("<i", raw, body_start + off_desc.offset + 8, 1)
            break
    return bytes(raw)


def scenario_undefined_dict():
    d = pa.dictionary(pa.int8(), pa.string())
    schema = pa.schema([pa.field("category", d)])
    batch = pa.record_batch([
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], pa.int8()), pa.array(["a", "b"])),
    ], schema=schema)
    raw = _stream_bytes([batch], schema)
    # split into messages and drop the dictionary batch
    chunks = []
    for start, _, _, h, _, end in _iter_framed(raw):
        if h.header_type != HEADER_DICTIONARY_BATCH:
            chunks.append(raw[start:end])
    return b"".join(chunks) + b"\xff\xff\xff\xff\x00\x00\x00\x00"


def scenario_truncated():
    schema, batches = scenario_good()
    raw = _stream_bytes(batches, schema)
    # keep the first full batch, cut well inside the second batch's body
    return raw[: len(raw) - 40]


def scenario_framing():
    schema, batches = scenario_good()
    raw = bytearray(_stream_bytes(batches, schema))
    # corrupt the metadata length of the LAST record batch into an implausible
    # size, so the earlier complete batch must still be retained
    framed = list(_iter_framed(raw))
    rb_messages = [m for m in framed if m[3].header_type == HEADER_RECORD_BATCH]
    target = rb_messages[-1]
    _, hpos, _, _, _, _ = target
    # metadata length sits 4 bytes before the header position
    struct.pack_into("<i", raw, hpos - 4, 0x40000000)
    return bytes(raw)


def build_payload(scenario: str) -> bytes:
    if scenario == "good":
        schema, batches = scenario_good()
        return _stream_bytes(batches, schema)
    if scenario == "dict-lifecycle":
        schema, batches, opts = scenario_dict_lifecycle()
        return _stream_bytes(batches, schema, options=opts)
    if scenario == "bad-offsets":
        return scenario_bad_offsets()
    if scenario == "undefined-dict":
        return scenario_undefined_dict()
    if scenario == "truncated":
        return scenario_truncated()
    if scenario == "framing":
        return scenario_framing()
    raise SystemExit(f"unknown scenario: {scenario}")


async def send_ws(url: str, payload: bytes, chunk_size: int):
    import websockets
    async with websockets.connect(url, max_size=None) as ws:
        # push in small chunks to exercise the incremental framing path
        for i in range(0, len(payload), chunk_size):
            await ws.send(payload[i:i + chunk_size])
            await asyncio.sleep(0.03)
        await ws.send("EOS")
        # progress events may still be in flight; keep reading until the
        # terminal final event (or an error) arrives.
        final = None
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                text = msg.decode() if isinstance(msg, bytes) else msg
                import json as _json
                event = _json.loads(text)
                if event.get("event") in ("final", "error"):
                    final = event
                    break
        except asyncio.TimeoutError:
            print("(no final acknowledgement within timeout)")
        if final is not None:
            print(json.dumps({
                k: final[k] for k in
                ("event", "termination", "termination_detail",
                 "messages_received", "bytes_received")
                if k in final
            }, ensure_ascii=False))
            print("batches:", [
                (b["ordinal"], b["status"], b["num_rows"])
                for b in final.get("batches", [])
            ])


def main():
    ap = argparse.ArgumentParser(description="Arrow IPC stream sender")
    ap.add_argument("transport", choices=["file", "ws"])
    ap.add_argument("--scenario", required=True,
                    choices=["good", "dict-lifecycle", "bad-offsets",
                             "undefined-dict", "truncated", "framing"])
    ap.add_argument("--out", default="sample.arrow")
    ap.add_argument("--url", default="ws://127.0.0.1:8000/ws/stream")
    ap.add_argument("--chunk-size", type=int, default=64)
    args = ap.parse_args()

    payload = build_payload(args.scenario)
    if args.transport == "file":
        with open(args.out, "wb") as fh:
            fh.write(payload)
        print(f"wrote {len(payload)} bytes ({args.scenario}) -> {args.out}")
    else:
        asyncio.run(send_ws(args.url, payload, args.chunk_size))
        print(f"sent {len(payload)} bytes ({args.scenario}) over {args.url}")


if __name__ == "__main__":
    main()

"""End-to-end smoke tests for the decoder core (no web stack needed).

Run: .venv/bin/python -m backend.selftest   (from the arrow-inspector dir)
"""
from __future__ import annotations

import json
import struct
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

from app.decoder import decode_stream
from app.framing import TruncatedStreamError, iter_messages
import io


def build_stream(batches, schema, dict_arrays=None):
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    for b in batches:
        writer.write_batch(b)
    writer.close()
    return sink.getvalue().to_pybytes()


def case_basic_nested():
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field(
            "info",
            pa.struct([
                pa.field("a", pa.int64()),
                pa.field("b", pa.bool_()),
            ]),
        ),
    ])
    batch = pa.record_batch([
        pa.array([1, None, 3], pa.int32()),
        pa.array(["alpha", "", None], pa.string()),
        pa.array([["x", "y"], [], None], pa.list_(pa.string())),
        pa.array([
            {"a": 10, "b": True},
            None,
            {"a": None, "b": False},
        ]),
    ], schema=schema)
    data = build_stream([batch], schema)
    report = decode_stream(data)
    assert report.termination == "eos", report.termination_detail
    assert len(report.batches) == 1
    b = report.batches[0]
    assert b.status == "ok", b.validation_issues
    rows = b.logical
    assert rows[0] == {"id": 1, "name": "alpha", "tags": ["x", "y"],
                       "info": {"a": 10, "b": True}}
    assert rows[1]["name"] == "" and rows[1]["tags"] == []
    assert rows[1]["id"] is None and rows[2]["name"] is None
    assert rows[2]["info"] == {"a": None, "b": False}
    print("ok: basic nested — null / [] / '' distinct")


def case_dictionary_lifecycle():
    d = pa.dictionary(pa.int8(), pa.string())
    schema = pa.schema([pa.field("cat", d)])
    b1 = pa.record_batch(
        [pa.DictionaryArray.from_arrays(pa.array([0, 1], pa.int8()),
                                        pa.array(["a", "b"]))],
        schema=schema,
    )
    b2 = pa.record_batch(
        [pa.DictionaryArray.from_arrays(pa.array([0, 1], pa.int8()),
                                        pa.array(["p", "q"]))],
        schema=schema,
    )
    data = build_stream([b1, b2], schema)
    report = decode_stream(data)
    assert len(report.batches) == 2
    assert report.batches[0].logical[0] == {"cat": "a"}
    assert report.batches[1].logical[0] == {"cat": "p"}
    # batch 1 must keep its original snapshot even after the replacement dict
    assert report.batches[0].logical[0]["cat"] == "a"
    assert len(report.dictionary_log) == 2
    assert report.dictionary_log[0]["values_preview"] == ["a", "b"]
    assert report.dictionary_log[1]["values_preview"] == ["p", "q"]
    print("ok: dictionary replacement does not rewrite earlier batches")


def case_undefined_dictionary():
    d = pa.dictionary(pa.int8(), pa.string())
    schema = pa.schema([pa.field("cat", d)])
    b = pa.record_batch(
        [pa.DictionaryArray.from_arrays(pa.array([0, 1], pa.int8()),
                                        pa.array(["a", "b"]))],
        schema=schema,
    )
    data = build_stream([b], schema)
    # parse messages and drop the dictionary message
    raw = bytearray(data)
    msgs = list(iter_messages(io.BytesIO(data).read))
    schema_msg, dict_msg, batch_msg = msgs[0], msgs[1], msgs[2]
    rebuilt = (
        b"\xff\xff\xff\xff" + struct.pack("<i", len(schema_msg.metadata))
        + schema_msg.metadata
        + b"\xff\xff\xff\xff" + struct.pack("<i", len(batch_msg.metadata))
        + batch_msg.metadata + batch_msg.body
        + b"\xff\xff\xff\xff\x00\x00\x00\x00"
    )
    report = decode_stream(rebuilt)
    assert len(report.batches) == 1
    assert report.batches[0].status == "undecodable"
    assert report.batches[0].error  # a concrete pyarrow failure is recorded
    print("ok: undefined dictionary reported, batch marked undecodable")


def case_illegal_offsets():
    schema = pa.schema([pa.field("tags", pa.list_(pa.int32()))])
    batch = pa.record_batch(
        [pa.array([[1, 2], [3], [4, 5, 6]], pa.list_(pa.int32()))],
        schema=schema,
    )
    data = bytearray(build_stream([batch], schema))
    msgs = list(iter_messages(io.BytesIO(bytes(data)).read))
    rb = msgs[1]
    from app.flatbuf import parse_message_metadata, HEADER_RECORD_BATCH
    h = parse_message_metadata(rb.metadata)
    off_desc = h.record_batch.buffers[1]  # validity, offsets, ...
    body_start = rb.stream_offset + 8 + len(rb.metadata)
    # make offsets non-monotonic
    abs_off = body_start + off_desc.offset + 8
    struct.pack_into("<i", data, abs_off, 1)
    report = decode_stream(bytes(data))
    b = report.batches[0]
    assert b.status == "invalid", b.status
    assert any("non-monotonic" in p for p in b.validation_issues), b.validation_issues
    print("ok: illegal offsets flagged as invalid with details")


def case_truncated_stream():
    schema = pa.schema([pa.field("id", pa.int32())])
    b1 = pa.record_batch([pa.array([1, 2], pa.int32())], schema=schema)
    b2 = pa.record_batch([pa.array([3, 4], pa.int32())], schema=schema)
    data = build_stream([b1, b2], schema)
    # cut inside the second batch body
    cut = len(data) - 12
    report = decode_stream(data[:cut])
    assert report.termination == "truncated", report.termination
    assert len(report.batches) >= 1
    assert report.batches[0].status == "ok"
    assert report.termination_detail and "truncated" in report.termination_detail
    print("ok: truncated stream keeps complete batches, explains the cut")


def case_structured_export_shape():
    schema = pa.schema([pa.field("id", pa.int32())])
    batch = pa.record_batch([pa.array([1, None], pa.int32())], schema=schema)
    report = decode_stream(build_stream([batch], schema))
    payload = json.loads(json.dumps({
        "schema": report.schema,
        "batches": [
            {
                "ordinal": b.ordinal,
                "status": b.status,
                "logical": b.logical,
                "physical": b.physical,
            }
            for b in report.batches
        ],
    }, default=str))
    node = payload["batches"][0]["physical"][0]
    assert node["buffers"][0]["role"] == "validity"
    assert node["validity_bits"] == [1, 0]
    print("ok: structured export carries buffers and validity bits")


if __name__ == "__main__":
    case_basic_nested()
    case_dictionary_lifecycle()
    case_undefined_dictionary()
    case_illegal_offsets()
    case_truncated_stream()
    case_structured_export_shape()
    print("\nAll selftests passed.")

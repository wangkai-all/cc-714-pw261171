"""Stream-level decoder: message orchestration, dictionary snapshots, output.

Design constraints driven by the task specification:

* Every record batch is decoded independently against a *snapshot* of the
  dictionary messages seen so far. A later dictionary replacement can never
  retroactively change an earlier batch.
* A batch that cannot be explained (undefined dictionary, illegal offsets,
  truncated body) is recorded as a failure; previously completed batches stay
  available and the stream is never reported as cleanly finished.
* Empty list ``[]``, empty string ``""`` and ``null`` are kept distinct by
  carrying PyArrow logical values verbatim instead of flattening to CSV.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.ipc as ipc

from . import flatbuf
from .framing import RawMessage, TruncatedStreamError, iter_messages
from .physical import NodeView, walk_array
from .validate import validate_batch

EOS_MARKER = b"\xff\xff\xff\xff\x00\x00\x00\x00"


@dataclass
class DictSnapshot:
    dictionary_id: int
    sequence: int          # ordinal of the dictionary message carrying it
    kind: str              # 'replacement' | 'delta'
    is_delta: bool
    message_offset: int
    values: list           # logical dictionary values
    index_type: str
    value_type: str


@dataclass
class BatchResult:
    ordinal: int
    message_offset: int
    num_rows: int
    status: str                        # 'ok' | 'invalid' | 'undecodable'
    logical: list[dict] | None = None
    physical: list[dict] | None = None
    validation_issues: list[str] = field(default_factory=list)
    error: str | None = None
    dictionary_refs: dict = field(default_factory=dict)


@dataclass
class StreamReport:
    schema: dict
    batches: list[BatchResult]
    dictionary_log: list[dict]
    termination: str
    termination_detail: str | None
    messages_received: int
    bytes_received: int


def _type_to_dict(t: pa.DataType, dict_id: int | None = None) -> dict:
    info = {"name": str(t)}
    if pa.types.is_dictionary(t):
        info["dictionary_id"] = dict_id
        info["index_type"] = str(t.index_type)
        info["value_type"] = str(t.value_type)
        info["ordered"] = t.ordered
        info["fields"] = [_type_to_dict(t.value_type)]
    elif pa.types.is_struct(t):
        info["fields"] = [
            {"name": f.name, "nullable": f.nullable,
             "type": _type_to_dict(f.type, _dict_id_of(f))}
            for f in t
        ]
    elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        info["fields"] = [
            {"name": t.value_field.name,
             "type": _type_to_dict(t.value_type, _dict_id_of(t.value_field))}
        ]
        if pa.types.is_fixed_size_list(t):
            info["list_size"] = t.list_size
    elif pa.types.is_map(t):
        info["fields"] = [
            {"name": "entries",
             "type": {
                 "name": str(t.item_type),
                 "fields": [
                     {"name": "key", "type": {"name": str(t.key_type)}},
                     {"name": "value", "type": {"name": str(t.item_type)}},
                 ],
             }}
        ]
    return info


def _dict_id_of(field: pa.Field) -> int | None:
    """IPC dictionary id is not on the in-memory type; callers pass it via
    the flatbuf schema map. This helper returns None for non-encoded fields
    and is only used inside recursive structural rendering."""
    return None


def _schema_to_dict(schema: pa.Schema, dict_ids: dict[str, int]) -> dict:
    return {
        "fields": [
            {
                "name": f.name,
                "nullable": f.nullable,
                "type": _type_to_dict(f.type, dict_ids.get(f.name)),
                "metadata": {
                    k.decode(): v.decode() for k, v in (f.metadata or {}).items()
                },
            }
            for f in schema
        ],
        "metadata": {
            k.decode(): v.decode() for k, v in (schema.metadata or {}).items()
        },
    }


def _node_to_dict(node: NodeView) -> dict:
    return {
        "name": node.name,
        "arrow_type": node.arrow_type,
        "length": node.length,
        "null_count": node.null_count,
        "path": node.path,
        "validity_bits": node.validity_bits,
        "offsets": node.offsets,
        "extra": node.extra,
        "buffers": [
            {
                "role": b.role,
                "offset": b.offset,
                "length": b.length,
                "hex_preview": b.hex_preview,
                "base64": b.base64,
            }
            for b in node.buffers
        ],
        "children": [_node_to_dict(c) for c in node.children],
    }


def _frame_message(msg: RawMessage) -> bytes:
    """Re-serialize a raw message back into framed IPC bytes."""
    prefix = b"\xff\xff\xff\xff" + struct.pack("<i", len(msg.metadata))
    return prefix + msg.metadata + msg.body


def _as_message(msg: RawMessage):
    """Re-parse a raw framed message as a pyarrow Message object."""
    return ipc.read_message(pa.BufferReader(_frame_message(msg)))


def _open_substream(messages: list[RawMessage]):
    framed = b"".join(_frame_message(m) for m in messages) + EOS_MARKER
    return ipc.open_stream(pa.BufferReader(framed))


def decode_stream(data: bytes) -> StreamReport:
    """Decode a complete (or partially received) Arrow IPC byte stream."""
    import io

    raw_messages: list[RawMessage] = []
    termination = "eos"
    termination_detail = None
    try:
        for msg in iter_messages(io.BytesIO(data).read):
            raw_messages.append(msg)
    except TruncatedStreamError as exc:
        termination = "truncated"
        termination_detail = exc.detail
    except Exception as exc:
        termination = "framing_error"
        termination_detail = f"{type(exc).__name__}: {exc}"

    bytes_received = 0
    if raw_messages:
        last = raw_messages[-1]
        bytes_received = last.stream_offset + len(last.metadata) + len(last.body)
    return _decode_messages(
        raw_messages, termination, termination_detail, bytes_received
    )


def _decode_messages(
    raw_messages: list[RawMessage],
    termination: str,
    termination_detail: str | None,
    bytes_received: int,
) -> StreamReport:
    schema_msg = None
    schema = None
    schema_fields: list[flatbuf.FieldSchema] = []
    for msg in raw_messages:
        header = flatbuf.parse_message_metadata(msg.metadata)
        if header.header_type == flatbuf.HEADER_SCHEMA:
            schema_msg = msg
            parsed_schema = flatbuf.parse_schema_metadata(msg.metadata)
            schema_fields = parsed_schema.fields
            schema = ipc.read_schema(_as_message(msg))
            break

    if schema_msg is None:
        return StreamReport(
            schema={"fields": []},
            batches=[],
            dictionary_log=[],
            termination=termination or "framing_error",
            termination_detail=termination_detail
            or "no schema message found before the stream ended",
            messages_received=len(raw_messages),
            bytes_received=bytes_received,
        )

    # Top-level field name -> IPC dictionary id (nested dict fields are rare
    # but handled per-array via the flatbuf field tree in physical rendering).
    dict_ids: dict[str, int] = {}
    dict_id_to_field_index: dict[int, int] = {}
    for i, fs in enumerate(schema_fields):
        if fs.dictionary is not None:
            dict_ids[fs.name] = fs.dictionary.dictionary_id
            dict_id_to_field_index[fs.dictionary.dictionary_id] = i

    dictionary_log: list[dict] = []
    dict_seq = 0
    pending_dict_msgs: list[RawMessage] = []
    latest_snapshots: dict[int, DictSnapshot] = {}
    batches: list[BatchResult] = []
    batch_ordinal = 0

    for msg in raw_messages:
        header = flatbuf.parse_message_metadata(msg.metadata)

        if header.header_type == flatbuf.HEADER_DICTIONARY_BATCH:
            dict_seq += 1
            dbh = header.dictionary_batch
            pending_dict_msgs.append(msg)
            field_index = dict_id_to_field_index.get(dbh.dictionary_id)
            values, index_type, value_type = _decode_dictionary_values(
                schema_msg, pending_dict_msgs, field_index
            )
            snap = DictSnapshot(
                dictionary_id=dbh.dictionary_id,
                sequence=dict_seq,
                kind="delta" if dbh.is_delta else "replacement",
                is_delta=dbh.is_delta,
                message_offset=msg.stream_offset,
                values=values,
                index_type=index_type,
                value_type=value_type,
            )
            latest_snapshots[dbh.dictionary_id] = snap
            dictionary_log.append({
                "dictionary_id": dbh.dictionary_id,
                "sequence": dict_seq,
                "kind": snap.kind,
                "is_delta": dbh.is_delta,
                "message_offset": msg.stream_offset,
                "num_values": len(values),
                "values_preview": values[:50],
                "index_type": index_type,
                "value_type": value_type,
            })
            continue

        if header.header_type != flatbuf.HEADER_RECORD_BATCH:
            continue

        batch_ordinal += 1
        result = BatchResult(
            ordinal=batch_ordinal,
            message_offset=msg.stream_offset,
            num_rows=header.record_batch.length,
            status="ok",
        )

        # 1) Physical validation against the flatbuffer layout first.
        issues = validate_batch(
            schema_fields,
            header.record_batch.nodes,
            header.record_batch.buffers,
            msg.body,
        )
        result.validation_issues = issues

        # 2) Logical decode with an isolated substream holding the current
        #    dictionary snapshot.
        try:
            record_batch = _decode_one_batch(schema_msg, pending_dict_msgs, msg)
        except Exception as exc:
            result.status = "undecodable"
            result.error = f"{type(exc).__name__}: {exc}"
            batches.append(result)
            continue

        # 3) Logical values — null / [] / "" remain distinct.
        cols = record_batch.to_pydict()
        result.logical = [
            {name: cols[name][i] for name in record_batch.schema.names}
            for i in range(record_batch.num_rows)
        ]

        # 4) Physical tree with the dictionary snapshots in effect.
        physical = []
        refs = {}
        for i, f in enumerate(record_batch.schema):
            arr = record_batch.column(i)
            node = walk_array(arr, f.name, f.name, latest_snapshots,
                              dict_id=dict_ids.get(f.name))
            physical.append(_node_to_dict(node))
            if pa.types.is_dictionary(f.type) and f.name in dict_ids:
                refs[f.name] = dict_ids[f.name]
        result.physical = physical
        result.dictionary_refs = refs
        if issues:
            result.status = "invalid"
        batches.append(result)

    return StreamReport(
        schema=_schema_to_dict(schema, dict_ids),
        batches=batches,
        dictionary_log=dictionary_log,
        termination=termination,
        termination_detail=termination_detail,
        messages_received=len(raw_messages),
        bytes_received=bytes_received,
    )


def _decode_one_batch(
    schema_msg: RawMessage, dict_msgs: list[RawMessage], batch_msg: RawMessage
) -> pa.RecordBatch:
    """Decode one record batch against an exact dictionary-message snapshot."""
    reader = _open_substream([schema_msg, *dict_msgs, batch_msg])
    return reader.read_next_batch()


def _empty_batch_for(schema: pa.Schema) -> pa.RecordBatch:
    arrays = [pa.array([], type=f.type) for f in schema]
    return pa.record_batch(arrays, schema=schema)


def _decode_dictionary_values(
    schema_msg: RawMessage, dict_msgs: list[RawMessage], field_index: int | None
):
    """Materialize the logical values of one dictionary from its messages.

    An empty record batch synthesized from the schema forces pyarrow to
    resolve every dictionary id; the dictionary array attached to that batch
    then exposes the resolved values without needing any data rows.
    """
    schema = ipc.read_schema(_as_message(schema_msg))
    if field_index is None:
        return [], "?", "?"

    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.write_batch(_empty_batch_for(schema))
    writer.close()
    rb_message_bytes = _extract_first_record_batch_message(
        sink.getvalue().to_pybytes()
    )
    rb_raw = _raw_from_framed(rb_message_bytes)

    reader = _open_substream([schema_msg, *dict_msgs, rb_raw])
    batch = reader.read_next_batch()
    arr = batch.column(field_index)
    return (
        arr.dictionary.to_pylist(),
        str(arr.type.index_type),
        str(arr.type.value_type),
    )


def _extract_first_record_batch_message(framed: bytes) -> bytes:
    pos = 0
    data = framed
    while pos < len(data):
        marker = struct.unpack_from("<i", data, pos)[0]
        start = pos
        if marker == -1:
            meta_len = struct.unpack_from("<i", data, pos + 4)[0]
            header_pos = pos + 8
        else:
            meta_len = marker
            header_pos = pos + 4
        if meta_len == 0:
            break
        meta = data[header_pos : header_pos + meta_len]
        h = flatbuf.parse_message_metadata(meta)
        body_end = header_pos + meta_len + h.body_length
        if h.header_type == flatbuf.HEADER_RECORD_BATCH:
            # include any 8-byte alignment padding after the body
            next_pos = (body_end + 7) & ~7
            return data[start:next_pos]
        pos = (body_end + 7) & ~7
    raise RuntimeError("no record batch message found in synthesized stream")


def _raw_from_framed(framed_message: bytes) -> RawMessage:
    marker = struct.unpack_from("<i", framed_message, 0)[0]
    if marker == -1:
        meta_len = struct.unpack_from("<i", framed_message, 4)[0]
        header_pos = 8
    else:
        meta_len = marker
        header_pos = 4
    metadata = framed_message[header_pos : header_pos + meta_len]
    body = framed_message[header_pos + meta_len :]
    return RawMessage(metadata=metadata, body=body, stream_offset=-1)

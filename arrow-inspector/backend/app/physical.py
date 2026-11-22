"""Physical buffer model extracted from decoded PyArrow arrays.

The walk mirrors the pre-order FieldNode/buffer layout of the Arrow columnar
format, so every node carries the exact buffers its logical values come from:
validity bits, offsets, fixed-width data, and dictionary indices/values.
"""
from __future__ import annotations

import base64
import struct
from dataclasses import dataclass, field

import pyarrow as pa

HEX_PREVIEW_LIMIT = 48  # bytes of raw buffer content shipped to the UI


@dataclass
class BufferView:
    role: str  # 'validity' | 'offsets' | 'data' | 'type_ids' | ...
    offset: int  # offset inside the message body
    length: int
    hex_preview: str
    base64: str | None = None  # full content only for small buffers


@dataclass
class NodeView:
    name: str
    arrow_type: str
    length: int
    null_count: int
    path: str
    buffers: list[BufferView] = field(default_factory=list)
    children: list["NodeView"] = field(default_factory=list)
    # convenience payloads for the UI's "where does this value come from" panel
    validity_bits: list[int] | None = None
    offsets: list[int] | None = None
    extra: dict = field(default_factory=dict)


def _buffer_view(role: str, buf: pa.Buffer | None, body_offset_map=None) -> BufferView:
    if buf is None or buf.size == 0:
        return BufferView(role=role, offset=-1, length=0, hex_preview="")
    raw = buf.to_pybytes()
    preview = raw[:HEX_PREVIEW_LIMIT]
    return BufferView(
        role=role,
        offset=-1,  # filled in by the message-level mapping when available
        length=buf.size,
        hex_preview=preview.hex(" "),
        base64=base64.b64encode(raw).decode("ascii") if len(raw) <= 4096 else None,
    )


def _validity_bits(arr: pa.Array) -> list[int] | None:
    buf = arr.buffers()[0]
    if buf is None:
        return None
    n = len(arr)
    raw = buf.to_pybytes()
    return [(raw[i // 8] >> (i % 8)) & 1 for i in range(n)]


def _offset_values(buf: pa.Buffer | None, width: int = 4) -> list[int]:
    """Decode an int32/int64 offset buffer into a plain Python list."""
    if buf is None or buf.size == 0:
        return []
    raw = buf.to_pybytes()
    if width == 8:
        count = len(raw) // 8
        return list(struct.unpack(f"<{count}q", raw))
    count = len(raw) // 4
    return list(struct.unpack(f"<{count}i", raw))


_PRIMITIVE_FORMATS = {
    pa.lib.Type_BOOL: ("?", 1),
    pa.lib.Type_INT8: ("b", 1),
    pa.lib.Type_UINT8: ("B", 1),
    pa.lib.Type_INT16: ("h", 2),
    pa.lib.Type_UINT16: ("H", 2),
    pa.lib.Type_INT32: ("i", 4),
    pa.lib.Type_UINT32: ("I", 4),
    pa.lib.Type_INT64: ("q", 8),
    pa.lib.Type_UINT64: ("Q", 8),
    pa.lib.Type_HALF_FLOAT: ("e", 2),
    pa.lib.Type_FLOAT: ("f", 4),
    pa.lib.Type_DOUBLE: ("d", 8),
    pa.lib.Type_DATE32: ("i", 4),
    pa.lib.Type_DATE64: ("q", 8),
    pa.lib.Type_TIME32: ("i", 4),
    pa.lib.Type_TIME64: ("q", 8),
    pa.lib.Type_TIMESTAMP: ("q", 8),
    pa.lib.Type_DURATION: ("q", 8),
    pa.lib.Type_INTERVAL_MONTH_DAY_NANO: ("16s", 16),
}


def _primitive_values(arr: pa.Array):
    t = arr.type
    kind = t.id
    if kind == pa.lib.Type_BOOL:
        raw = arr.buffers()[1].to_pybytes()
        return [((raw[i // 8] >> (i % 8)) & 1) == 1 for i in range(len(arr))]
    if kind in (pa.lib.Type_DECIMAL128, pa.lib.Type_DECIMAL256):
        return [str(v) if v is not None else None for v in arr.to_pylist()]
    fmt, width = _PRIMITIVE_FORMATS.get(kind, (None, 0))
    if fmt is None:
        return arr.to_pylist()
    buf = arr.buffers()[1]
    raw = buf.to_pybytes() if buf is not None else b""
    count = len(raw) // width
    return list(struct.unpack(f"<{count}{fmt}", raw[: count * width]))


def walk_array(
    arr: pa.Array,
    name: str,
    path: str,
    dict_snapshot: dict[int, "DictSnapshot"] | None = None,
    dict_id: int | None = None,
) -> NodeView:
    """Build the physical node tree for one top-level array."""
    t = arr.type
    node = NodeView(
        name=name,
        arrow_type=str(t),
        length=len(arr),
        null_count=arr.null_count,
        path=path,
    )
    node.validity_bits = _validity_bits(arr)

    kind = t.id

    if kind == pa.lib.Type_DICTIONARY:
        bv = _buffer_view("validity", arr.buffers()[0])
        node.buffers.append(bv)
        idx_view = _buffer_view("indices", arr.indices.buffers()[1])
        node.buffers.append(idx_view)
        node.extra["index_type"] = str(t.index_type)
        node.extra["dictionary_id"] = dict_id
        node.extra["ordered"] = t.ordered
        snap = dict_snapshot.get(dict_id) if (dict_snapshot and dict_id is not None) else None
        node.extra["dictionary_message_sequence"] = snap.sequence if snap else None
        node.extra["indices"] = arr.indices.to_pylist()
        # dictionary values as their own physical subtree
        dict_node = walk_array(arr.dictionary, f"{name}$dictionary", f"{path}::$dict")
        dict_node.extra["dictionary_id"] = dict_id
        if snap:
            dict_node.extra["message_kind"] = snap.kind
            dict_node.extra["message_sequence"] = snap.sequence
        node.children.append(dict_node)
        return node

    if kind in (pa.lib.Type_LIST, pa.lib.Type_LARGE_LIST, pa.lib.Type_MAP):
        node.buffers.append(_buffer_view("validity", arr.buffers()[0]))
        node.buffers.append(_buffer_view("offsets", arr.buffers()[1]))
        width = 8 if kind == pa.lib.Type_LARGE_LIST else 4
        node.offsets = _offset_values(arr.buffers()[1], width)
        child_name = "items" if kind != pa.lib.Type_MAP else "entries"
        child = arr.values
        node.children.append(
            walk_array(child, child_name, f"{path}[]{child_name}", dict_snapshot)
        )
        if kind == pa.lib.Type_MAP:
            # expose keys/items grouping of the struct entries
            entries = node.children[0]
            entries.name = "entries(struct)"
        return node

    if kind == pa.lib.Type_FIXED_SIZE_LIST:
        node.buffers.append(_buffer_view("validity", arr.buffers()[0]))
        node.extra["list_size"] = t.list_size
        node.children.append(
            walk_array(arr.values, "items", f"{path}[]items", dict_snapshot)
        )
        return node

    if kind == pa.lib.Type_STRUCT:
        node.buffers.append(_buffer_view("validity", arr.buffers()[0]))
        for i, field in enumerate(t):
            node.children.append(
                walk_array(
                    arr.field(i), field.name, f"{path}.{field.name}", dict_snapshot
                )
            )
        return node

    if kind in (pa.lib.Type_STRING, pa.lib.Type_LARGE_STRING,
                pa.lib.Type_BINARY, pa.lib.Type_LARGE_BINARY):
        node.buffers.append(_buffer_view("validity", arr.buffers()[0]))
        node.buffers.append(_buffer_view("offsets", arr.buffers()[1]))
        width = 8 if kind in (pa.lib.Type_LARGE_STRING, pa.lib.Type_LARGE_BINARY) else 4
        node.offsets = _offset_values(arr.buffers()[1], width)
        node.buffers.append(_buffer_view("values", arr.buffers()[2]))
        node.extra["logical_values"] = _string_like_values(arr, kind)
        return node

    if kind == pa.lib.Type_FIXED_SIZE_BINARY:
        node.buffers.append(_buffer_view("validity", arr.buffers()[0]))
        node.buffers.append(_buffer_view("data", arr.buffers()[1]))
        node.extra["byte_width"] = t.byte_width
        return node

    if kind == pa.lib.Type_SPARSE_UNION:
        # Unions have no validity buffer: slots are [None, type_ids, children...]
        node.buffers.append(_buffer_view("type_ids", arr.buffers()[1]))
        node.extra["type_codes"] = t.type_codes
        for i, field in enumerate(t):
            node.children.append(
                walk_array(arr.field(i), field.name, f"{path}.{field.name}",
                           dict_snapshot)
            )
        return node

    if kind == pa.lib.Type_DENSE_UNION:
        # [None, type_ids, offsets, ...child buffers]
        node.buffers.append(_buffer_view("type_ids", arr.buffers()[1]))
        node.buffers.append(_buffer_view("offsets", arr.buffers()[2]))
        node.offsets = _offset_values(arr.buffers()[2], 4)
        node.extra["type_codes"] = t.type_codes
        for i, field in enumerate(t):
            node.children.append(
                walk_array(arr.field(i), field.name, f"{path}.{field.name}",
                           dict_snapshot)
            )
        return node

    if kind == pa.lib.Type_NA:
        node.extra["all_null"] = True
        return node

    # primitive numeric / temporal
    node.buffers.append(_buffer_view("validity", arr.buffers()[0]))
    data_buf = arr.buffers()[1]
    node.buffers.append(_buffer_view("data", data_buf))
    node.extra["physical_values"] = _primitive_values(arr)
    return node


def _string_like_values(arr: pa.Array, kind) -> list[str | None]:
    out = []
    for v in arr.to_pylist():
        if v is None:
            out.append(None)
        elif kind in (pa.lib.Type_BINARY, pa.lib.Type_LARGE_BINARY):
            out.append(base64.b64encode(v).decode("ascii"))
        else:
            out.append(v)
    return out

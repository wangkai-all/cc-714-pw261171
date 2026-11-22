"""Minimal FlatBuffers reader for the Arrow IPC metadata we need.

PyArrow 26 no longer exposes Message header accessors or a Python-side
DictionaryMemo, so the metadata of IPC messages is parsed here directly
against the Arrow FlatBuffers schema (format/Message.fbs, Schema.fbs).

Only the fields required by the inspector are decoded; everything else is
skipped using raw vtable lookups.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

# --- Arrow MessageHeader union tags (Message.fbs MessageHeader) ---
# NONE=0, Schema=1, DictionaryBatch=2, RecordBatch=3, Tensor=4, SparseTensor=5
HEADER_SCHEMA = 1
HEADER_DICTIONARY_BATCH = 2
HEADER_RECORD_BATCH = 3
HEADER_TENSOR = 4
HEADER_SPARSE_TENSOR = 5

# --- Arrow Type union tags (Schema.fbs Type), verified against pyarrow 26 ---
TYPE_NA = 1
TYPE_INT = 2
TYPE_FLOAT = 3
TYPE_BINARY = 4
TYPE_UTF8 = 5
TYPE_BOOL = 6
TYPE_DECIMAL = 7
TYPE_DATE = 8
TYPE_TIME = 9
TYPE_TIMESTAMP = 10
TYPE_INTERVAL = 11
TYPE_LIST = 12
TYPE_STRUCT = 13
TYPE_UNION = 14
TYPE_FIXED_SIZE_BINARY = 15
TYPE_FIXED_SIZE_LIST = 16
TYPE_MAP = 17
TYPE_DURATION = 18
TYPE_LARGE_BINARY = 19
TYPE_LARGE_UTF8 = 20
TYPE_LARGE_LIST = 21


class FBTable:
    """A FlatBuffers table positioned inside a bytes/memoryview buffer."""

    __slots__ = ("buf", "pos")

    def __init__(self, buf, pos):
        self.buf = buf
        self.pos = pos

    def _slot_offset(self, slot: int) -> int:
        """In-table byte offset of a vtable field slot, or 0 when absent.

        Vtable layout: uint16 vtable size, uint16 table size, then one
        uint16 offset per declared field (field 0, 1, 2, ...).
        """
        soff = struct.unpack_from("<i", self.buf, self.pos)[0]
        vtable = self.pos - soff
        vtable_len = struct.unpack_from("<H", self.buf, vtable)[0]
        index = 4 + 2 * slot
        if index + 2 > vtable_len:
            return 0
        return struct.unpack_from("<H", self.buf, vtable + index)[0]

    def get(self, slot: int, fmt: str, default=0):
        off = self._slot_offset(slot)
        if off == 0:
            return default
        return struct.unpack_from(fmt, self.buf, self.pos + off)[0]

    def table(self, slot: int) -> "FBTable | None":
        off = self._slot_offset(slot)
        if off == 0:
            return None
        # off is an in-table byte offset; the uoffset stored there is relative
        # to the field's own position.
        field_pos = self.pos + off
        rel = struct.unpack_from("<I", self.buf, field_pos)[0]
        return FBTable(self.buf, field_pos + rel)

    def string(self, slot: int) -> bytes | None:
        off = self._slot_offset(slot)
        if off == 0:
            return None
        field_pos = self.pos + off
        p = field_pos + struct.unpack_from("<I", self.buf, field_pos)[0]
        length = struct.unpack_from("<I", self.buf, p)[0]
        return bytes(self.buf[p + 4 : p + 4 + length])

    def vector_len(self, slot: int) -> int:
        off = self._slot_offset(slot)
        if off == 0:
            return 0
        field_pos = self.pos + off
        p = field_pos + struct.unpack_from("<I", self.buf, field_pos)[0]
        return struct.unpack_from("<I", self.buf, p)[0]

    def vector_of_tables(self, slot: int) -> list["FBTable"]:
        off = self._slot_offset(slot)
        if off == 0:
            return []
        field_pos = self.pos + off
        p = field_pos + struct.unpack_from("<I", self.buf, field_pos)[0]
        count = struct.unpack_from("<I", self.buf, p)[0]
        start = p + 4
        out = []
        for i in range(count):
            elem = start + 4 * i
            rel = struct.unpack_from("<I", self.buf, elem)[0]
            out.append(FBTable(self.buf, elem + rel))
        return out

    def vector_of_structs(self, slot: int, struct_size: int) -> list[bytes]:
        """Return raw struct byte spans (caller unpacks)."""
        off = self._slot_offset(slot)
        if off == 0:
            return []
        field_pos = self.pos + off
        p = field_pos + struct.unpack_from("<I", self.buf, field_pos)[0]
        count = struct.unpack_from("<I", self.buf, p)[0]
        start = p + 4
        return [
            bytes(self.buf[start + struct_size * i : start + struct_size * (i + 1)])
            for i in range(count)
        ]


@dataclass
class FieldNode:
    length: int
    null_count: int


@dataclass
class BufferDesc:
    offset: int  # relative to message body
    length: int


@dataclass
class RecordBatchHeader:
    length: int
    nodes: list[FieldNode]
    buffers: list[BufferDesc]


@dataclass
class DictionaryBatchHeader:
    dictionary_id: int
    is_delta: bool
    record_batch: RecordBatchHeader


@dataclass
class MessageHeader:
    header_type: int
    body_length: int
    record_batch: RecordBatchHeader | None = None
    dictionary_batch: DictionaryBatchHeader | None = None


def _parse_record_batch(t: FBTable) -> RecordBatchHeader:
    length = t.get(0, "<q")
    node_spans = t.vector_of_structs(1, 16)  # FieldNode: long length, long null_count
    nodes = [FieldNode(*struct.unpack("<qq", span)) for span in node_spans]
    buffer_spans = t.vector_of_structs(2, 16)  # Buffer: long offset, long length
    buffers = [BufferDesc(*struct.unpack("<qq", span)) for span in buffer_spans]
    return RecordBatchHeader(length=length, nodes=nodes, buffers=buffers)


def parse_message_metadata(metadata: bytes) -> MessageHeader:
    """Parse the flatbuffer payload of an IPC message (without framing).

    Message table slots (Arrow Message.fbs):
      0 version (u16), 1 header_type (u8 union tag), 2 header (table),
      3 bodyLength (i64), 4 custom_metadata.
    """
    buf = memoryview(metadata)
    root = struct.unpack_from("<I", buf, 0)[0]
    msg = FBTable(buf, root)
    header_type = msg.get(1, "<B")
    body_length = msg.get(3, "<q")
    out = MessageHeader(header_type=header_type, body_length=body_length)
    inner = msg.table(2)
    if inner is None:
        return out
    if header_type == HEADER_RECORD_BATCH:
        out.record_batch = _parse_record_batch(inner)
    elif header_type == HEADER_DICTIONARY_BATCH:
        dict_id = inner.get(0, "<q")
        is_delta = bool(inner.get(2, "<b"))
        rb_table = inner.table(1)
        out.dictionary_batch = DictionaryBatchHeader(
            dictionary_id=dict_id,
            is_delta=is_delta,
            record_batch=_parse_record_batch(rb_table),
        )
    return out


@dataclass
class DictionaryEncoding:
    dictionary_id: int
    ordered: bool
    index_type_kind: str  # 'signed' | 'unsigned'
    index_bit_width: int


@dataclass
class FieldSchema:
    name: str
    nullable: bool
    type_tag: int
    dictionary: DictionaryEncoding | None
    children: list["FieldSchema"]
    # numeric / decimal metadata
    bit_width: int | None = None
    is_signed: bool | None = None
    precision: int | None = None
    scale: int | None = None
    list_size: int | None = None
    unit: int | None = None
    time_bit_width: int | None = None
    type_codes: list[int] | None = None
    mode: int | None = None  # union mode: 0 sparse, 1 dense
    # map entries are struct children with a Map marker in custom metadata;
    # the MAP type tag on the parent is enough for us.


def _parse_field(ft: FBTable) -> FieldSchema:
    name = ft.string(0).decode("utf-8", errors="replace")
    nullable = bool(ft.get(1, "<b"))
    type_tag = ft.get(2, "<B")
    type_table = ft.table(3)
    children = [_parse_field(c) for c in ft.vector_of_tables(5)]

    dictionary = None
    dict_table = ft.table(4)
    if dict_table is not None:
        dict_id = dict_table.get(0, "<q")
        ordered = bool(dict_table.get(2, "<b"))
        index_table = dict_table.table(1)
        bit_width = index_table.get(0, "<i") if index_table else 32
        is_signed = bool(index_table.get(1, "<b")) if index_table else True
        dictionary = DictionaryEncoding(
            dictionary_id=dict_id,
            ordered=ordered,
            index_type_kind="signed" if is_signed else "unsigned",
            index_bit_width=bit_width,
        )

    fs = FieldSchema(
        name=name,
        nullable=nullable,
        type_tag=type_tag,
        dictionary=dictionary,
        children=children,
    )
    if type_table is not None:
        if type_tag == TYPE_INT:
            # Int: slot0 bitWidth (i32), slot1 is_signed (bool at unaligned off)
            fs.bit_width = type_table.get(0, "<i")
            fs.is_signed = bool(type_table.get(1, "<b"))
        elif type_tag == TYPE_FLOAT:
            # FloatingPoint: slot0 precision (short): 0 half, 1 single, 2 double
            fs.precision = type_table.get(0, "<h")
        elif type_tag == TYPE_DECIMAL:
            # Decimal: slot0 precision (i32), slot1 scale (i32), slot2 bitWidth
            fs.precision = type_table.get(0, "<i")
            fs.scale = type_table.get(1, "<i")
            fs.bit_width = type_table.get(2, "<i", 128)
        elif type_tag == TYPE_TIME:
            # Time: slot0 unit (short), slot1 bitWidth (i32)
            fs.unit = type_table.get(0, "<h")
            fs.time_bit_width = type_table.get(1, "<i")
        elif type_tag == TYPE_TIMESTAMP:
            # Timestamp: slot0 unit (short); timezone string at slot1
            fs.unit = type_table.get(0, "<h")
        elif type_tag == TYPE_DURATION:
            fs.unit = type_table.get(0, "<h")
        elif type_tag == TYPE_FIXED_SIZE_LIST:
            # FixedSizeList: slot0 listSize (i32)
            fs.list_size = type_table.get(0, "<i")
        elif type_tag == TYPE_FIXED_SIZE_BINARY:
            # FixedSizeBinary: slot0 byteWidth (i32) — reused list_size field
            fs.list_size = type_table.get(0, "<i")
        elif type_tag == TYPE_DATE:
            # Date: slot0 unit (short): 0 day, 1 millisecond
            fs.unit = type_table.get(0, "<h")
        elif type_tag == TYPE_UNION:
            mode = type_table.get(0, "<h")
            fs.mode = mode
            codes_spans = type_table.vector_of_structs(1, 4)
            fs.type_codes = [struct.unpack("<i", s)[0] for s in codes_spans]
    return fs


@dataclass
class SchemaHeader:
    endianness: int  # 0 little, 1 big
    fields: list[FieldSchema]


def parse_schema_metadata(metadata: bytes) -> SchemaHeader:
    buf = memoryview(metadata)
    root = struct.unpack_from("<I", buf, 0)[0]
    msg = FBTable(buf, root)
    # Schema message: header table is Message slot 2.
    schema_table = msg.table(2)
    endianness = schema_table.get(0, "<H")
    fields = [_parse_field(ft) for ft in schema_table.vector_of_tables(1)]
    return SchemaHeader(endianness=endianness, fields=fields)

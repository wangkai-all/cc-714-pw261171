"""Physical validation of RecordBatch buffers against the declared schema.

PyArrow's IPC reader accepts some malformed input silently (for example
non-monotonic list offsets produce garbage instead of an error), so the
inspector validates the buffer layout itself before trusting any decoded
logical value.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field

from .flatbuf import (
    FieldNode,
    BufferDesc,
    FieldSchema,
    TYPE_BOOL,
    TYPE_BINARY,
    TYPE_DATE,
    TYPE_DECIMAL,
    TYPE_DURATION,
    TYPE_FIXED_SIZE_BINARY,
    TYPE_FIXED_SIZE_LIST,
    TYPE_FLOAT,
    TYPE_INT,
    TYPE_INTERVAL,
    TYPE_LARGE_BINARY,
    TYPE_LARGE_LIST,
    TYPE_LARGE_UTF8,
    TYPE_LIST,
    TYPE_MAP,
    TYPE_NA,
    TYPE_UTF8,
    TYPE_TIME,
    TYPE_TIMESTAMP,
    TYPE_STRUCT,
    TYPE_UNION,
)


class ValidationError(ValueError):
    """A physical buffer violates the Arrow columnar format."""


@dataclass
class Cursor:
    """Pre-order walk state over the parallel node/buffer arrays."""

    nodes: list[FieldNode]
    buffers: list[BufferDesc]
    node_idx: int = 0
    buffer_idx: int = 0
    problems: list[str] = field(default_factory=list)

    def next_node(self) -> FieldNode:
        if self.node_idx >= len(self.nodes):
            raise ValidationError(
                f"record batch declares too few field nodes "
                f"(needed node {self.node_idx} of {len(self.nodes)})"
            )
        node = self.nodes[self.node_idx]
        self.node_idx += 1
        return node

    def next_buffer(self) -> BufferDesc:
        if self.buffer_idx >= len(self.buffers):
            raise ValidationError(
                f"record batch declares too few buffers "
                f"(needed buffer {self.buffer_idx} of {len(self.buffers)})"
            )
        buf = self.buffers[self.buffer_idx]
        self.buffer_idx += 1
        return buf


# Offset byte width per variable-width list/string/binary type.
_OFFSET_WIDTH = {
    TYPE_LIST: 4,
    TYPE_MAP: 4,
    TYPE_UTF8: 4,
    TYPE_BINARY: 4,
    TYPE_LARGE_LIST: 8,
    TYPE_LARGE_UTF8: 8,
    TYPE_LARGE_BINARY: 8,
}


def _check_validity(cur: Cursor, node: FieldNode, body_size: int, path: str):
    desc = cur.next_buffer()
    if node.null_count == 0:
        # null_count == 0 permits an omitted (zero-length) validity buffer.
        return
    expected_bytes = (node.length + 7) // 8
    if desc.length == 0:
        cur.problems.append(
            f"{path}: node declares {node.null_count} null(s) but validity "
            f"buffer is absent"
        )
        return
    if desc.offset + desc.length > body_size:
        cur.problems.append(
            f"{path}: validity buffer [{desc.offset}, +{desc.length}) exceeds "
            f"body size {body_size}"
        )
        return
    if desc.length < expected_bytes:
        cur.problems.append(
            f"{path}: validity buffer {desc.length} byte(s) too small for "
            f"{node.length} elements (need {expected_bytes})"
        )
    if node.null_count < 0:
        cur.problems.append(f"{path}: negative null count {node.null_count}")


def _read_offsets(body: bytes, desc: BufferDesc, width: int) -> list[int]:
    if width == 4:
        count = desc.length // 4
        return list(struct.unpack(f"<{count}i",
                                  body[desc.offset:desc.offset + desc.length]))
    count = desc.length // 8
    return list(struct.unpack(f"<{count}q",
                              body[desc.offset:desc.offset + desc.length]))


def _check_offsets(cur: Cursor, node: FieldNode, body: bytes, body_size: int,
                   width: int, path: str) -> list[int]:
    desc = cur.next_buffer()
    expected = (node.length + 1) * width
    if desc.offset + desc.length > body_size:
        cur.problems.append(
            f"{path}: offset buffer [{desc.offset}, +{desc.length}) exceeds "
            f"body size {body_size}"
        )
        return [0] * (node.length + 1)
    if desc.length < expected:
        cur.problems.append(
            f"{path}: offset buffer {desc.length} byte(s) too small "
            f"(need {expected} for {node.length} elements)"
        )
        return [0] * (node.length + 1)
    offsets = _read_offsets(body, desc, width)
    if offsets and offsets[0] != 0:
        cur.problems.append(
            f"{path}: first offset must be 0, got {offsets[0]}"
        )
    for i in range(1, len(offsets)):
        if offsets[i] < offsets[i - 1]:
            cur.problems.append(
                f"{path}: non-monotonic offsets at index {i}: "
                f"{offsets[i - 1]} -> {offsets[i]}"
            )
            break
    return offsets


def _check_fixed_width(cur: Cursor, node: FieldNode, body_size: int,
                       byte_width: int, path: str):
    desc = cur.next_buffer()
    if desc.offset + desc.length > body_size:
        cur.problems.append(
            f"{path}: data buffer [{desc.offset}, +{desc.length}) exceeds "
            f"body size {body_size}"
        )
        return
    expected = node.length * byte_width
    if desc.length < expected:
        cur.problems.append(
            f"{path}: data buffer {desc.length} byte(s) too small "
            f"(need {expected} for {node.length} elements of "
            f"{byte_width} byte(s))"
        )


def _walk(cur: Cursor, fs: FieldSchema, body: bytes, body_size: int,
          path: str) -> int:
    """Walk one field node; return its logical length."""
    node = cur.next_node()
    if node.length < 0:
        cur.problems.append(f"{path}: negative array length {node.length}")
    tag = fs.type_tag

    # Unions have no validity buffer (nulls are tracked per child); the null
    # type has no buffers at all.
    if tag != TYPE_UNION and tag != TYPE_NA:
        _check_validity(cur, node, body_size, path)

    if fs.dictionary is not None:
        # Dictionary-encoded field: this node holds only validity + indices.
        # The dictionary values arrive in separate DictionaryBatch messages
        # and are validated there, not as part of this record batch.
        width = fs.dictionary.index_bit_width // 8
        _check_fixed_width(cur, node, body_size, width, f"{path}[indices]")
        return node.length

    if tag == TYPE_NA:
        return node.length

    if tag == TYPE_STRUCT:
        for child in fs.children:
            _walk(cur, child, body, body_size, f"{path}.{child.name}")
        return node.length

    if tag == TYPE_FIXED_SIZE_LIST:
        size = fs.list_size or 0
        child = fs.children[0]
        child_node_idx = cur.node_idx
        _walk(cur, child, body, body_size, f"{path}.{child.name}")
        child_node = cur.nodes[child_node_idx]
        expected = node.length * size
        if child_node.length != expected:
            cur.problems.append(
                f"{path}: fixed-size list child length {child_node.length} "
                f"!= {node.length} * list_size {size} = {expected}"
            )
        return node.length

    if tag in (TYPE_LIST, TYPE_LARGE_LIST, TYPE_MAP):
        width = _OFFSET_WIDTH[tag]
        offsets = _check_offsets(cur, node, body, body_size, width, path)
        child = fs.children[0]
        child_node_idx = cur.node_idx
        _walk(cur, child, body, body_size, f"{path}.{child.name}")
        child_node = cur.nodes[child_node_idx]
        if offsets and offsets[-1] > child_node.length:
            cur.problems.append(
                f"{path}: final offset {offsets[-1]} runs past child array "
                f"length {child_node.length}"
            )
        return node.length

    if tag in (TYPE_UTF8, TYPE_BINARY, TYPE_LARGE_UTF8, TYPE_LARGE_BINARY):
        width = _OFFSET_WIDTH[tag]
        offsets = _check_offsets(cur, node, body, body_size, width, path)
        data_desc = cur.next_buffer()
        if data_desc.offset + data_desc.length > body_size:
            cur.problems.append(
                f"{path}: value buffer [{data_desc.offset}, "
                f"+{data_desc.length}) exceeds body size {body_size}"
            )
        elif offsets and offsets[-1] > data_desc.length:
            cur.problems.append(
                f"{path}: final offset {offsets[-1]} runs past value buffer "
                f"length {data_desc.length}"
            )
        return node.length

    if tag == TYPE_FIXED_SIZE_BINARY:
        _check_fixed_width(cur, node, body_size, fs.list_size or 0, path)
        return node.length

    if tag == TYPE_BOOL:
        desc = cur.next_buffer()
        expected = (node.length + 7) // 8
        if desc.offset + desc.length > body_size:
            cur.problems.append(f"{path}: bool buffer exceeds body")
        elif desc.length < expected:
            cur.problems.append(
                f"{path}: bool buffer {desc.length} byte(s) too small for "
                f"{node.length} elements"
            )
        return node.length

    if tag == TYPE_INT:
        _check_fixed_width(cur, node, body_size, (fs.bit_width or 0) // 8, path)
        return node.length

    if tag == TYPE_FLOAT:
        # precision: 0 half(2), 1 single(4), 2 double(8)
        width = {0: 2, 1: 4, 2: 8}.get(fs.precision, 4)
        _check_fixed_width(cur, node, body_size, width, path)
        return node.length

    if tag == TYPE_DECIMAL:
        width = {128: 16, 256: 32}.get(fs.bit_width or 128, 16)
        _check_fixed_width(cur, node, body_size, width, path)
        return node.length

    if tag == TYPE_DATE:
        # unit 0 = day (i32), unit 1 = millisecond (i64)
        width = 8 if fs.unit == 1 else 4
        _check_fixed_width(cur, node, body_size, width, path)
        return node.length

    if tag == TYPE_TIME:
        width = 8 if fs.time_bit_width == 64 else 4
        _check_fixed_width(cur, node, body_size, width, path)
        return node.length

    if tag == TYPE_TIMESTAMP or tag == TYPE_DURATION:
        _check_fixed_width(cur, node, body_size, 8, path)
        return node.length

    if tag == TYPE_INTERVAL:
        _check_fixed_width(cur, node, body_size, 16, path)
        return node.length

    if tag == TYPE_UNION:
        # Unions have no validity buffer. Sparse: [type_ids]; dense:
        # [type_ids, offsets]. Child arrays follow in pre-order.
        type_desc = cur.next_buffer()
        if type_desc.length < node.length:
            cur.problems.append(
                f"{path}: union type-id buffer too small for {node.length} rows"
            )
        if fs.mode == 1:  # dense union carries one int32 offset *per row*
            desc = cur.next_buffer()
            expected = node.length * 4
            if desc.offset + desc.length > body_size:
                cur.problems.append(
                    f"{path}: union offset buffer [{desc.offset}, "
                    f"+{desc.length}) exceeds body size {body_size}"
                )
            elif desc.length < expected:
                cur.problems.append(
                    f"{path}: union offset buffer {desc.length} byte(s) too "
                    f"small (need {expected} for {node.length} elements)"
                )
            else:
                offsets = _read_offsets(body, desc, 4)
                for i in range(1, len(offsets)):
                    if offsets[i] < offsets[i - 1]:
                        cur.problems.append(
                            f"{path}: non-monotonic union offsets at index {i}: "
                            f"{offsets[i - 1]} -> {offsets[i]}"
                        )
                        break
        for child in fs.children:
            _walk(cur, child, body, body_size, f"{path}.{child.name}")
        return node.length

    cur.problems.append(f"{path}: unvalidated physical type tag {tag}")
    return node.length


def validate_batch(schema_fields: list[FieldSchema], nodes: list[FieldNode],
                   buffers: list[BufferDesc], body: bytes) -> list[str]:
    """Return human-readable validation problems (empty list == valid)."""
    cur = Cursor(nodes=nodes, buffers=buffers)
    body_size = len(body)
    try:
        for fs in schema_fields:
            _walk(cur, fs, body, body_size, fs.name)
    except ValidationError as exc:
        cur.problems.append(str(exc))
        return cur.problems

    if cur.node_idx != len(nodes):
        cur.problems.append(
            f"{len(nodes) - cur.node_idx} trailing field node(s) not covered "
            f"by the schema"
        )
    if cur.buffer_idx != len(buffers):
        cur.problems.append(
            f"{len(buffers) - cur.buffer_idx} trailing buffer(s) not covered "
            f"by the schema"
        )
    return cur.problems

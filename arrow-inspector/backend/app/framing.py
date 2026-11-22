"""Incremental Arrow IPC stream framing.

Reads length-prefixed messages from any binary stream. Unlike
``pyarrow.ipc.open_stream`` this layer:

* never assumes the stream is complete, so a truncated connection is reported
  precisely instead of failing the whole decode;
* exposes every raw message (schema, dictionary batch, record batch) so the
  inspector can keep per-batch dictionary snapshots and validate buffers
  before PyArrow gets a chance to silently accept illegal offsets.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

CONTINUATION = b"\xff\xff\xff\xff"
EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


class TruncatedStreamError(EOFError):
    """The stream ended in the middle of a message.

    ``consumed_messages`` complete messages were already delivered; the
    partially read message is described in ``detail``.
    """

    def __init__(self, detail: str, consumed_messages: int):
        super().__init__(detail)
        self.detail = detail
        self.consumed_messages = consumed_messages


class FramingError(ValueError):
    """The byte stream does not conform to the Arrow IPC framing rules."""


@dataclass
class RawMessage:
    metadata: bytes
    body: bytes
    # absolute position of the message prefix in the source stream, for
    # diagnostics and buffer hex dumps
    stream_offset: int


def _read_exact(read, n: int, what: str, consumed: int, stream_pos: int) -> bytes:
    """Read exactly n bytes; a short read means the stream was truncated."""
    if n == 0:
        return b""
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = read(remaining)
        if not chunk:
            got = n - remaining
            raise TruncatedStreamError(
                f"stream truncated while reading {what}: expected {n} bytes, "
                f"only {got} arrived (byte {stream_pos + got})",
                consumed_messages=consumed,
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def iter_messages(read, max_message_size: int = 512 * 1024 * 1024):
    """Yield :class:`RawMessage` objects from a blocking binary ``read`` callable.

    Supports both the modern continuation marker framing and the legacy
    prefix-less framing (metadata length as the first int32). Terminates
    cleanly on the end-of-stream marker, raises
    :class:`TruncatedStreamError` on a cut-off message.
    """
    consumed = 0
    stream_pos = 0
    while True:
        prefix = read(4)
        if prefix == b"":
            return  # clean close between messages
        if len(prefix) < 4:
            raise TruncatedStreamError(
                "stream truncated inside message prefix", consumed_messages=consumed
            )
        msg_start = stream_pos
        first = struct.unpack("<i", prefix)[0]
        if first == -1:  # 0xFFFFFFFF continuation marker
            size_bytes = _read_exact(
                read, 4, "metadata length", consumed, stream_pos
            )
            metadata_length = struct.unpack("<i", size_bytes)[0]
            stream_pos += 8
        else:
            # Legacy framing: the first int32 is the metadata length directly.
            metadata_length = first
            stream_pos += 4

        if metadata_length == 0:
            # End-of-stream marker (continuation + zero length).
            return

        if metadata_length < 0 or metadata_length > max_message_size:
            raise FramingError(
                f"implausible metadata length {metadata_length} at byte {msg_start}"
            )

        metadata = _read_exact(
            read, metadata_length, "message metadata", consumed, stream_pos
        )
        metadata_end = stream_pos + metadata_length
        # Body length comes from the flatbuffer metadata; parse it here so the
        # framing layer stays independent of pyarrow header availability.
        from . import flatbuf  # local import avoids a cycle at module import

        header = flatbuf.parse_message_metadata(metadata)
        body_length = header.body_length
        # The body starts at the next 8-byte boundary relative to the message
        # prefix position, per the encapsulated IPC layout.
        body_start = (metadata_end + 7) & ~7
        meta_pad = body_start - metadata_end
        if meta_pad:
            _read_exact(read, meta_pad, "metadata padding", consumed, stream_pos)
        body = b""
        if body_length:
            body = _read_exact(
                read, body_length, "message body", consumed, stream_pos
            )
        body_end = body_start + body_length
        next_start = (body_end + 7) & ~7
        body_pad = next_start - body_end
        if body_pad:
            _read_exact(read, body_pad, "body padding", consumed, stream_pos)
        stream_pos = next_start
        yield RawMessage(metadata=metadata, body=body, stream_offset=msg_start)
        consumed += 1

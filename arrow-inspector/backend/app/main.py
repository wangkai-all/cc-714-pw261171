"""FastAPI application: upload, live sender connection, structured export."""
from __future__ import annotations

import io
import json
from dataclasses import asdict

from fastapi import FastAPI, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .decoder import StreamReport, decode_stream
from .framing import TruncatedStreamError, iter_messages

app = FastAPI(title="Arrow IPC Stream Inspector", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _report_to_dict(report: StreamReport) -> dict:
    return {
        "schema": report.schema,
        "termination": report.termination,
        "termination_detail": report.termination_detail,
        "messages_received": report.messages_received,
        "bytes_received": report.bytes_received,
        "dictionary_log": report.dictionary_log,
        "batches": [asdict(b) for b in report.batches],
    }


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/inspect/upload")
async def inspect_upload(file: UploadFile):
    """Decode an uploaded .arrow / .arrows IPC stream file."""
    raw = await file.read()
    report = decode_stream(raw)
    return JSONResponse(_report_to_dict(report))


@app.post("/api/inspect/bytes")
async def inspect_bytes(payload: dict):
    """Decode a raw stream provided as a base64 body (used by the sender demo)."""
    import base64

    raw = base64.b64decode(payload.get("data", ""))
    report = decode_stream(raw)
    return JSONResponse(_report_to_dict(report))


@app.websocket("/ws/stream")
async def ws_stream(ws: WebSocket):
    """Receive a live IPC stream from a local sender, decoding incrementally.

    The sender pushes binary frames; the server accumulates them and re-decodes
    after each frame so the UI sees batches arrive one by one. A truncated
    connection keeps every fully received batch and reports the cut explicitly.
    """
    await ws.accept()
    buffer = bytearray()

    def emit_final():
        if not buffer:
            return None
        return {"event": "final", **_report_to_dict(decode_stream(bytes(buffer)))}

    try:
        while True:
            message = await ws.receive()
            if message["type"] == "websocket.disconnect":
                # Abrupt close without EOS: decode what fully arrived and
                # report the cut before letting the socket go.
                final = emit_final()
                if final is not None:
                    await ws.send_json(final)
                break
            chunk = message.get("bytes")
            if chunk is None:
                text = message.get("text")
                if text == "EOS":
                    final = emit_final()
                    if final is not None:
                        await ws.send_json(final)
                    break
                continue
            buffer.extend(chunk)
            report = decode_stream(bytes(buffer))
            payload = _report_to_dict(report)
            # While the connection is still open, a cut at the current tail is
            # normal backpressure, not a real truncation. Only complete messages
            # have been decoded anyway.
            if payload["termination"] == "truncated":
                payload["termination"] = "receiving"
                payload["termination_detail"] = None
            await ws.send_json({
                "event": "progress",
                "bytes_received": len(buffer),
                **payload,
            })
    except WebSocketDisconnect:
        final = emit_final()
        if final is not None:
            try:
                await ws.send_json(final)
            except Exception:
                pass
    except Exception as exc:  # never crash the socket silently
        report = decode_stream(bytes(buffer)) if buffer else None
        await ws.send_json({
            "event": "error",
            "error": f"{type(exc).__name__}: {exc}",
            **(_report_to_dict(report) if report else {}),
        })
    finally:
        try:
            await ws.close()
        except Exception:
            pass


@app.post("/api/export/json")
async def export_json(payload: dict):
    """Re-emit a structured export preserving list/struct nesting."""
    # The frontend posts the already-decoded report; we normalize it here so
    # there is one canonical export shape (no CSV flattening).
    canonical = {
        "format": "arrow-ipc-inspector/1",
        "termination": payload.get("termination"),
        "schema": payload.get("schema"),
        "dictionary_log": payload.get("dictionary_log", []),
        "batches": [
            {
                "ordinal": b.get("ordinal"),
                "status": b.get("status"),
                "num_rows": b.get("num_rows"),
                "logical": b.get("logical"),
                "validation_issues": b.get("validation_issues", []),
                "error": b.get("error"),
                "dictionary_refs": b.get("dictionary_refs", {}),
            }
            for b in payload.get("batches", [])
        ],
    }
    return JSONResponse(
        canonical,
        headers={"Content-Disposition": 'attachment; filename="arrow-export.json"'},
    )


# Frontend static assets (mounted last so /api routes take precedence).
app.mount(
    "/",
    StaticFiles(directory="/workspace/arrow-inspector/frontend/dist", html=True),
    name="frontend",
)

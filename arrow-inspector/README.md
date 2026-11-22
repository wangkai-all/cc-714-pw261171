# Arrow IPC Stream Inspector

A web-based inspector for Arrow IPC **streaming** format. Upload a `.arrow`
stream file or connect to a live local sender, then inspect each RecordBatch
at three levels simultaneously:

1. **Logical values** — decoded rows, where `null`, the empty list `[]`, and
   the empty string `""` are always rendered as distinct values (never
   flattened or conflated).
2. **Schema / field types** — full nested type tree including dictionary
   encodings and their IPC dictionary ids.
3. **Physical buffers** — the exact validity bits, offset arrays, fixed-width
   data, and dictionary indices that produced each logical value, with hex
   previews. Selecting any nested value expands the buffers behind it.

## Guarantees

- **Per-batch dictionary snapshots.** Every RecordBatch is decoded against an
  isolated substream containing only the dictionary messages received up to
  that point. A later dictionary replacement or delta can never be
  retroactively applied to an earlier batch. The dictionary log records every
  replacement/delta with its stream offset.
- **No fabricated completion.** On an undefined dictionary, illegal offsets,
  framing corruption, or stream truncation, every fully received batch is
  retained and exposed. The batch that cannot be interpreted is marked
  `invalid` (physical layout violation) or `undecodable` (PyArrow cannot
  decode it), and the termination field names the exact byte position instead
  of pretending the stream ended cleanly.
- **Independent physical validation.** PyArrow silently accepts some malformed
  input (e.g. non-monotonic list offsets). The validator walks the
  FieldNode/buffer layout from the FlatBuffers metadata itself, so corrupted
  buffers are surfaced with a specific reason even when a naive reader would
  emit garbage.
- **Structured export, not CSV.** JSON export (`arrow-ipc-inspector/1`) keeps
  list and struct hierarchy intact; nulls stay `null`, empty lists stay `[]`.

## Layout

```
arrow-inspector/
├── backend/
│   ├── app/
│   │   ├── flatbuf.py    # hand-rolled FlatBuffers metadata reader
│   │   ├── framing.py    # incremental IPC message framing / truncation
│   │   ├── validate.py   # physical buffer-layout validator
│   │   ├── physical.py   # per-array validity/offset/buffer tree walker
│   │   ├── decoder.py    # orchestration + per-batch dict snapshot isolation
│   │   └── main.py       # FastAPI: upload, bytes, WebSocket, export, SPA
│   └── selftest.py       # six end-to-end correctness tests
├── frontend/             # Vue 3 + Vite (recursive ValueCell / PhysicalNode)
├── sender/sender.py      # generates healthy streams and deliberately bad ones
└── samples/              # pre-generated one of each scenario
```

## Running

### 1. Backend

```bash
cd backend
python -m venv .venv                      # already created at repo-root .venv
.venv/bin/pip install pyarrow fastapi uvicorn websockets python-multipart flatbuffers
.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

The server serves the built frontend at `http://127.0.0.1:8000/` once the
frontend has been built.

### 2. Frontend

```bash
cd frontend
npm install --cache ../.npm-cache
npm run build          # emits dist/, which FastAPI serves at /
# or, for development with hot reload and /api,/ws proxying:
npm run dev
```

### 3. Sender

The sender can write a scenario to a file or push it over WebSocket.

```bash
# file output
.venv/bin/python sender/sender.py file --scenario good --out samples/good.arrow

# live stream (UI must be connected to ws://127.0.0.1:8000/ws/stream)
.venv/bin/python sender/sender.py ws --scenario good --chunk-size 64
```

Scenarios:

| scenario          | what it demonstrates |
|-------------------|----------------------|
| `good`            | nested lists/structs with `null` / `[]` / `""` side by side |
| `dict-lifecycle`  | dictionary replacement + delta; per-batch snapshot isolation |
| `bad-offsets`     | corrupted non-monotonic list offsets → batch `invalid` |
| `undefined-dict`  | batch references a dictionary id never sent → `undecodable` |
| `truncated`       | connection cut mid-message → prior batches retained, cut explained |
| `framing`         | implausible metadata length → framing error, earlier batch retained |

## API

- `POST /api/inspect/upload` — multipart file upload of an IPC stream.
- `POST /api/inspect/bytes` — `{"data": "<base64 stream>"}`.
- `WS  /ws/stream` — push binary frames; server re-decodes after each frame
  and emits `progress` events (an open tail is reported as `receiving`, never
  as truncation). Send text `EOS`, or disconnect; the terminal `final` event
  carries the true termination status.
- `POST /api/export/json` — canonical structured export of a decoded report.
- `GET  /api/health` — liveness probe.

## Tests

```bash
cd backend
.venv/bin/python selftest.py
```

Covers null/`[]`/`""` distinction, dictionary replacement isolation,
undefined dictionaries, illegal offsets, truncation retention, and export
shape. All six tests pass.

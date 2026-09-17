# cairnmark-python

The official Python client for [CairnMark](https://github.com/mettjs/cairnmark) —
a self-hostable file service over S3-compatible storage with a queryable
Postgres metadata layer.

Sync **and** async clients, streaming uploads/downloads, client-side checksum
verification, typed errors, safe retries, lazy pagination. One dependency
(`httpx`). Python ≥ 3.10, fully typed (`py.typed`). Requires a CairnMark
server with extraction jobs — the first server release after v1.2.0, which
carries the `extraction_jobs` migration; every method except the archive ones
also works against a server ≥ v1.1.0.

```sh
pip install cairnmark   # not yet on PyPI — until then: pip install <path to this repo>
```

## Quickstart

```python
from cairnmark import CairnMark

with CairnMark("http://localhost:8080") as cm:
    # Upload with tags; idempotency_key="auto" makes retries duplicate-safe.
    f = cm.upload(
        b"hello from Python",
        filename="hello.txt",
        content_type="text/plain",
        metadata={"env": "demo"},
        idempotency_key="auto",
    )
    print("uploaded:", f.id)

    # Download — follows the presign redirect and verifies the SHA-256.
    cm.download_to_file(f.id, "hello-copy.txt")

    # Search by tag, lazily across pages.
    for file in cm.iter_files(tags={"env": "demo"}):
        print("found:", file.id, file.filename)
```

Async is the same surface with `await` (and `aiter_bytes`/`aread`/`aclose` on
downloads):

```python
from cairnmark import AsyncCairnMark

async with AsyncCairnMark("http://localhost:8080") as cm:
    f = await cm.upload(b"hello", filename="hello.txt")
    async with await cm.download(f.id, verify=True) as dl:
        data = await dl.aread()
    async for file in cm.iter_files(tags={"env": "demo"}):
        ...
```

## Methods

| Method | Does |
|---|---|
| `upload(content, *, filename, content_type, size, metadata, idempotency_key)` | Stream bytes / a file-like / an iterator up. `idempotency_key="auto"` generates a key. |
| `upload_file(path, ...)` | Upload from disk; name and size inferred. |
| `download(id, *, verify, offset, length)` | Open a content stream (context manager). Default follows the presign redirect; `offset`/`length` for a 206 range; `verify` for checksum checking. |
| `download_to_file(id, path)` | Download to disk, checksum-verified; removes the file on failure. |
| `presign_url(id)` | Mint the presigned object-store URL without following it. |
| `get_metadata(id)` | Fetch the file record (a frozen `File` dataclass). |
| `update_metadata(id, tags, mode="merge"\|"replace")` | Patch tags. |
| `delete(id)` | Soft-delete. |
| `list(...)` / `iter_files(...)` | One page / lazy iteration over every match. `entries=` scopes files extracted from archives. |
| `archive_entries(id)` | List a stored zip's entries by index, each selectable or carrying a skip reason. Writes nothing. |
| `extract(id, *, entries, timeout)` | Store a zip's entries (all, or `entries` by index) as files and wait for it; returns a bounded summary. |
| `extract_async(id, *, entries)` | Submit the same extraction and return the pending `Job` at once. |
| `job(job_id)` / `wait_for_job(job_id, *, timeout)` | Read a job's state / poll it to a terminal state. |
| `cancel_job(job_id)` | Ask a job to stop after the entry it is on; idempotent. |
| `health()` / `ready()` | Liveness / readiness probes (raise on failure). |

## Archives

A zip uploads like any other file. Extraction is a second, explicit call
against the stored object; each extracted entry becomes an ordinary file,
tagged with the archive it came from.

```python
from cairnmark import TAG_ARCHIVE_ID, TAG_ARCHIVE_PATH

# 1. Upload the zip and see what is inside — nothing is written yet.
arch = cm.upload_file("docs.zip", idempotency_key="auto")
for e in cm.archive_entries(arch.id):
    print(e.index, e.name, e.size, e.selectable, e.reason)

# 2. Extract one entry by index (omit entries= to extract everything selectable).
summary = cm.extract(arch.id, entries=[0])
print("extracted", summary.extracted, "skipped", summary.skipped_by_reason)

# 3. Find the extracted file by its tags, then download it like any other.
for f in cm.iter_files(tags={TAG_ARCHIVE_ID: arch.id, TAG_ARCHIVE_PATH: "reports/q3.pdf"}):
    cm.download_to_file(f.id, "q3.pdf")
```

Entries are addressed by **index** because names need not be unique inside a
zip. Extraction is resumable: a re-run skips entries already stored
(`already_extracted`) and never resurrects one you deleted
(`previously_deleted`).

**The server runs an extraction as a job.** `extract` hides that: it submits,
polls, and returns the summary, blocking for the whole run. `timeout=` bounds
that wait in seconds (`None`, the default, waits as long as it takes) and
raises `WaitTimeoutError` — a `TimeoutError` — without cancelling the job. If
another extraction of the same archive is in flight (`409`), it waits for that
job to finish and resubmits its own. A job that fails raises
`ExtractionFailedError`; one that is cancelled raises
`ExtractionCancelledError` with the partial summary — distinct, so "I
cancelled this" and "it broke" are told apart.

The job surface is there when you want it — to return early, show progress,
or cancel:

```python
job = cm.extract_async(arch.id)          # 202, at once
# ... later, or elsewhere, with just the id:
job = cm.job(job.id)                     # .status, .progress.done / .progress.total
job = cm.cancel_job(job.id)              # stops after the current entry; keeps what is stored
job = cm.wait_for_job(job.id)            # polls (1s, doubling to 10s) until terminal; inspect .status
```

A finished job stays readable for the server's `CAIRNMARK_JOB_RETENTION`
(default 24h), after which `job` raises `NotFoundError`; the extracted files
themselves are permanent. Errors a synchronous call could give — not a zip, a
bad selection, over a cap — still come back from the submission itself.

`list` mixes ordinary files, archives and extracted entries;
`entries="exclude"` / `"only"` separates them, and `tags={TAG_ARCHIVE: "true"}`
lists the archives.

## Configuration

`CairnMark(base_url, ...)` / `AsyncCairnMark(base_url, ...)` keyword options:

| Option | Default | Does |
|---|---|---|
| `headers` | `None` | Default headers on every request; the hook for gateway credentials. |
| `timeout` | `30.0` | httpx per-operation timeout (connect / single socket read) — a `float` or an `httpx.Timeout` for fine-grained control. Doesn't cut off large streams, and bounds each request of an extraction rather than the wait for it. |
| `retries` | `2` | Retries after a network error or 5xx (so up to `retries + 1` attempts), and how many `409`s `extract` waits out before giving up. `0` disables. |
| `poll_interval` | `1.0` | The first wait, in seconds, between polls of an extraction job; each wait doubles up to `10.0`. |
| `user_agent` | `cairnmark-python/<version>` | Override the `User-Agent`. |
| `transport` | `None` | Custom `httpx.BaseTransport` (`AsyncBaseTransport` for async) — proxies, mocking, UDS. |

## Errors

Every non-2xx response raises a subclass of `APIError` (which carries
`.status`, `.message`, `.retry_after` on 409, and `.job_id` on a 409
extraction conflict — the job holding the archive), itself a `CairnMarkError`:

`InvalidRequestError` (400) · `NotFoundError` (404, a file or a job) ·
`IdempotencyConflictError` (409) · `IdempotencyGoneError` (410) ·
`TooLargeError` (413) · `NotArchiveError` (415, an archive method on a file
that is not a zip) · `RangeNotSatisfiableError` (416) · `ServerError` (5xx)
— plus `ChecksumMismatchError` from verified download streams, and from
`extract`: `ExtractionFailedError` / `ExtractionCancelledError` (both
`JobError`, carrying the terminal `.job`) and `WaitTimeoutError` (also a
`TimeoutError`).

An argument the SDK rejects before any request — a bad `entries` scope, a bad
`mode`, `verify` on a range download — raises `InvalidParameterError`, which
subclasses both `CairnMarkError` and `ValueError`, so either one catches it.

## Semantics worth knowing

- **Retries.** Network errors and 5xx are retried with jittered backoff
  (default 2 retries; `retries=` to change). Uploads retry **only** when they
  carry an idempotency key *and* the body can be rewound (bytes or a seekable
  file) — otherwise a retry could duplicate the file. A 409 (same key still in
  flight) waits out the server's `Retry-After`.
- **Checksum verification.** The presigned object-store response carries no
  checksum header, so `verify=True` hashes the stream client-side against the
  stored SHA-256 (fetched from metadata) and raises `ChecksumMismatchError` as
  the last chunk is consumed. Range downloads can't be verified.
- **Timeouts.** `timeout=` is httpx's per-operation timeout (connect, single
  socket read), so large streams aren't cut off mid-transfer. The wait for an
  extraction job is bounded only by `extract(..., timeout=)` /
  `wait_for_job(..., timeout=)`.
- **Extraction jobs.** `extract` is `extract_async` + `wait_for_job`. It
  waits out another job on the same archive and resubmits; the SDK never
  returns someone else's summary as yours. A job's result is readable for the
  server's retention window only; the extracted files are permanent.
- **Auth.** The server has none; put it behind your gateway and inject
  credentials with `headers=` (or a custom `transport=`).

## Development

```sh
uv sync
uv run pytest                    # unit tests (respx-mocked, no server needed)
uv run ruff check src tests && uv run pyright src

# integration: full round-trip against a live server
(cd ../CairnMark && docker compose up -d --build)
uv run pytest -m integration     # honors CAIRNMARK_BASE_URL, default localhost:8080
```

## License

[MIT](LICENSE) © 2026 Michael Ramirez

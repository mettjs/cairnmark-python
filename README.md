# cairnmark-python

The official Python client for [CairnMark](https://github.com/mettjs/cairnmark) —
a self-hostable file service over S3-compatible storage with a queryable
Postgres metadata layer.

Sync **and** async clients, streaming uploads/downloads, client-side checksum
verification, typed errors, safe retries, lazy pagination. One dependency
(`httpx`). Python ≥ 3.10, fully typed (`py.typed`). Requires a CairnMark
server ≥ v1.1.0 (cursor pagination and 409 `Retry-After`).

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
| `list(...)` / `iter_files(...)` | One page / lazy iteration over every match. |
| `health()` / `ready()` | Liveness / readiness probes (raise on failure). |

## Configuration

`CairnMark(base_url, ...)` / `AsyncCairnMark(base_url, ...)` keyword options:

| Option | Default | Does |
|---|---|---|
| `headers` | `None` | Default headers on every request; the hook for gateway credentials. |
| `timeout` | `30.0` | httpx per-operation timeout (connect / single socket read) — a `float` or an `httpx.Timeout` for fine-grained control. Doesn't cut off large streams. |
| `retries` | `2` | Retries after a network error or 5xx (so up to `retries + 1` attempts). `0` disables. |
| `user_agent` | `cairnmark-python/<version>` | Override the `User-Agent`. |
| `transport` | `None` | Custom `httpx.BaseTransport` (`AsyncBaseTransport` for async) — proxies, mocking, UDS. |

## Errors

Every non-2xx response raises a subclass of `APIError` (which carries
`.status`, `.message`, and `.retry_after` on 409), itself a `CairnMarkError`:

`InvalidRequestError` (400) · `NotFoundError` (404) ·
`IdempotencyConflictError` (409) · `IdempotencyGoneError` (410) ·
`TooLargeError` (413) · `RangeNotSatisfiableError` (416) · `ServerError` (5xx)
— plus `ChecksumMismatchError` from verified download streams.

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
  socket read), so large streams aren't cut off mid-transfer.
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

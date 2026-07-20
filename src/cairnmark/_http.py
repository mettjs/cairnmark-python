"""Shared request/response plumbing for the sync and async clients.

Everything here is pure or synchronous helper logic; the two clients own the
actual I/O and retry loops so each can sleep its own way.
"""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from typing import Any, AsyncIterator, Callable, Iterator

import httpx

from .errors import APIError, ChecksumMismatchError, error_class_for

DEFAULT_RETRIES = 2
DEFAULT_TIMEOUT = 30.0
#: Cap on how long a retry waits on a server-supplied Retry-After.
MAX_RETRY_AFTER = 30.0


def backoff_delay(attempt: int) -> float:
    """Exponential, jittered delay in seconds before retry ``attempt`` (0-based)."""
    d = min(0.25 * (2**attempt), 4.0)
    return d / 2 + random.uniform(0, d / 2)


def retry_delay(err: APIError | None, attempt: int) -> float:
    """Backoff, except a server-supplied Retry-After (bounded) wins."""
    if err is not None and err.retry_after:
        return min(err.retry_after, MAX_RETRY_AFTER)
    return backoff_delay(attempt)


def error_from_response(response: httpx.Response) -> APIError:
    """Map a non-2xx response (body already read) to a typed APIError."""
    message = ""
    try:
        body = response.json()
        if isinstance(body, dict):
            message = str(body.get("error") or "")
    except ValueError:
        pass
    if not message:
        message = response.text.strip() or f"HTTP {response.status_code}"
    retry_after = None
    ra = response.headers.get("retry-after", "")
    if ra.isdigit() and int(ra) > 0:
        retry_after = float(ra)
    return error_class_for(response.status_code)(response.status_code, message, retry_after)


def unexpected_status(response: httpx.Response, want: int) -> APIError:
    return APIError(response.status_code, f"unexpected status {response.status_code} (want {want})")


def new_idempotency_key() -> str:
    """32 hex chars, unique per call — comfortably under the server's 255 cap."""
    return uuid.uuid4().hex


def resolve_idempotency_key(key: str | None) -> str | None:
    """The sentinel value "auto" becomes a freshly generated key."""
    return new_idempotency_key() if key == "auto" else key


def upload_params_headers(
    filename: str | None,
    content_type: str | None,
    metadata: dict[str, Any] | None,
    idempotency_key: str | None,
    size: int | None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Query params and headers for POST /files."""
    params: dict[str, str] = {}
    if filename:
        params["filename"] = filename
    headers: dict[str, str] = {}
    if content_type:
        headers["Content-Type"] = content_type
    if metadata:
        # The X-Metadata JSON header carries typed/nested tag values intact.
        headers["X-Metadata"] = json.dumps(metadata, separators=(",", ":"))
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    if size is not None and size > 0:
        headers["Content-Length"] = str(size)
    return params, headers


def list_params(
    content_type: str | None,
    tags: dict[str, str] | None,
    limit: int | None,
    cursor: str | None,
) -> dict[str, str]:
    """Query params for GET /files."""
    params: dict[str, str] = {}
    if content_type:
        params["content_type"] = content_type
    for key, value in (tags or {}).items():
        params[f"tag.{key}"] = value
    if limit:
        params["limit"] = str(limit)
    if cursor:
        params["cursor"] = cursor
    return params


def range_header(offset: int, length: int | None) -> str:
    """A bytes= header for offset/length (length None means "to the end")."""
    if length is not None and length > 0:
        return f"bytes={offset}-{offset + length - 1}"
    return f"bytes={offset}-"


def rewinder(content: Any) -> Callable[[], None] | None:
    """A callable that resets ``content`` for a resend, or None if it can't be.

    Retries must resend the body; bytes are trivially reusable, seekable
    file-likes are rewound to their current position, and anything else
    (a one-shot iterator) disables retries rather than resending garbage.
    """
    if isinstance(content, (bytes, bytearray, memoryview)):
        return lambda: None
    seek = getattr(content, "seek", None)
    tell = getattr(content, "tell", None)
    seekable = getattr(content, "seekable", None)
    if callable(seek) and callable(tell) and (seekable is None or seekable()):
        pos = content.tell()
        return lambda: content.seek(pos)
    return None


def verify_bytes(chunks: Iterator[bytes], expected: str) -> Iterator[bytes]:
    """Pass chunks through, raising ChecksumMismatchError after the last one.

    The presigned object-store response carries no checksum header, so this
    client-side hash is the only integrity signal on an offloaded download.
    """
    digest = hashlib.sha256()
    for chunk in chunks:
        digest.update(chunk)
        yield chunk
    got = digest.hexdigest()
    if got != expected.lower():
        raise ChecksumMismatchError(got, expected)


async def averify_bytes(chunks: AsyncIterator[bytes], expected: str) -> AsyncIterator[bytes]:
    """Async twin of verify_bytes."""
    digest = hashlib.sha256()
    async for chunk in chunks:
        digest.update(chunk)
        yield chunk
    got = digest.hexdigest()
    if got != expected.lower():
        raise ChecksumMismatchError(got, expected)

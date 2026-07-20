"""The asynchronous CairnMark client — the async twin of ``_client.py``.

The two are kept in lockstep by hand; the surface is small enough that a
codegen step would cost more than it saves. Change one, change both.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from typing import Any, AsyncIterator, Callable, Iterator, cast
from urllib.parse import quote

import httpx

from ._http import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    averify_bytes,
    backoff_delay,
    error_from_response,
    list_params,
    range_header,
    resolve_idempotency_key,
    retry_delay,
    rewinder,
    unexpected_status,
    upload_params_headers,
)
from .errors import CairnMarkError, ServerError
from .models import File, ListPage
from .version import __version__


class AsyncDownload:
    """An open async download stream plus the file's metadata record.

    Use as an async context manager (or call ``aclose()``). With verification
    on, ChecksumMismatchError is raised as the last chunk is consumed.
    """

    def __init__(self, response: httpx.Response, file: File, verify: bool) -> None:
        self._response = response
        self._verify = verify
        self.file = file

    def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        chunks = self._response.aiter_bytes(chunk_size)
        if self._verify and self.file.checksum_sha256:
            return averify_bytes(chunks, self.file.checksum_sha256)
        return chunks

    async def aread(self) -> bytes:
        """Consume the whole stream (still verified) into memory."""
        parts = [chunk async for chunk in self.aiter_bytes()]
        return b"".join(parts)

    async def aclose(self) -> None:
        await self._response.aclose()

    async def __aenter__(self) -> AsyncDownload:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


class AsyncCairnMark:
    """Asynchronous client for one CairnMark server. See CairnMark for the
    full semantics — the two expose the same operations."""

    def __init__(
        self,
        base_url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | httpx.Timeout = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        user_agent: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._retries = retries
        merged = dict(headers or {})
        merged["User-Agent"] = user_agent or f"cairnmark-python/{__version__}"
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers=merged,
            timeout=timeout,
            transport=transport,
            follow_redirects=False,
        )

    # -- core request loop -------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
        follow_redirects: bool = False,
        expect: int = 200,
    ) -> httpx.Response:
        attempt = 0
        while True:
            try:
                resp = await self._http.request(
                    method,
                    path,
                    params=params,
                    headers=headers,
                    content=content,
                    follow_redirects=follow_redirects,
                )
            except httpx.TransportError:
                if attempt < self._retries:
                    await asyncio.sleep(backoff_delay(attempt))
                    attempt += 1
                    continue
                raise
            if resp.status_code < 400:
                if resp.status_code != expect:
                    raise unexpected_status(resp, expect)
                return resp
            err = error_from_response(resp)
            if isinstance(err, ServerError) and attempt < self._retries:
                await asyncio.sleep(backoff_delay(attempt))
                attempt += 1
                continue
            raise err

    # -- uploads -------------------------------------------------------------

    async def upload(
        self,
        content: Any,
        *,
        filename: str | None = None,
        content_type: str | None = None,
        size: int | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> File:
        """Stream ``content`` up; see CairnMark.upload for the semantics."""
        key = resolve_idempotency_key(idempotency_key)
        if isinstance(content, (bytearray, memoryview)):
            content = bytes(content)  # httpx's typed surface takes bytes
        if isinstance(content, bytes):
            size = None  # httpx sets Content-Length for in-memory bodies
        params, headers = upload_params_headers(filename, content_type, metadata, key, size)
        rewind = rewinder(content)
        retryable = key is not None and rewind is not None

        attempt = 0
        while True:
            if attempt and rewind is not None:
                rewind()
            try:
                # Wrapped fresh per attempt: a rewound file needs a new stream.
                resp = await self._http.post(
                    "/files", params=params, headers=headers, content=_as_async_content(content)
                )
            except httpx.TransportError:
                if retryable and attempt < self._retries:
                    await asyncio.sleep(backoff_delay(attempt))
                    attempt += 1
                    continue
                raise
            if resp.status_code == 201:
                return File.from_json(resp.json())
            err = error_from_response(resp)
            if retryable and attempt < self._retries and err.status in (409, *range(500, 600)):
                await asyncio.sleep(retry_delay(err if err.status == 409 else None, attempt))
                attempt += 1
                continue
            raise err

    async def upload_file(
        self,
        path: str | os.PathLike[str],
        *,
        filename: str | None = None,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> File:
        """Upload the file at ``path``; name and size are inferred."""
        with open(path, "rb") as f:
            return await self.upload(
                f,
                filename=filename or os.path.basename(os.fspath(path)),
                content_type=content_type,
                size=os.fstat(f.fileno()).st_size,
                metadata=metadata,
                idempotency_key=idempotency_key,
            )

    # -- downloads -----------------------------------------------------------

    async def download(
        self,
        file_id: str,
        *,
        verify: bool = False,
        offset: int | None = None,
        length: int | None = None,
    ) -> AsyncDownload:
        """Open the file's content; see CairnMark.download for the semantics."""
        ranged = offset is not None or length is not None
        if verify and ranged:
            raise ValueError("verify is incompatible with a range download")

        file = await self.get_metadata(file_id)
        if verify and not file.checksum_sha256:
            raise CairnMarkError(f"file {file_id} has no stored checksum to verify against")

        headers = {"Range": range_header(offset or 0, length)} if ranged else None
        expect = 206 if ranged else 200
        attempt = 0
        while True:
            request = self._http.build_request("GET", _file_path(file_id), headers=headers)
            try:
                resp = await self._http.send(request, stream=True, follow_redirects=True)
            except httpx.TransportError:
                if attempt < self._retries:
                    await asyncio.sleep(backoff_delay(attempt))
                    attempt += 1
                    continue
                raise
            if resp.status_code == expect:
                return AsyncDownload(resp, file, verify)
            await resp.aread()
            await resp.aclose()
            err = error_from_response(resp)
            if isinstance(err, ServerError) and attempt < self._retries:
                await asyncio.sleep(backoff_delay(attempt))
                attempt += 1
                continue
            raise err

    async def download_to_file(
        self, file_id: str, path: str | os.PathLike[str], *, verify: bool = True
    ) -> File:
        """Download into ``path``, checksum-verified; removed on failure."""
        async with await self.download(file_id, verify=verify) as dl:
            try:
                with open(path, "wb") as out:
                    async for chunk in dl.aiter_bytes():
                        out.write(chunk)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.remove(path)
                raise
        return dl.file

    async def presign_url(self, file_id: str) -> str:
        """The presigned object-store URL, without following it."""
        resp = await self._request("GET", _file_path(file_id), expect=302)
        location = resp.headers.get("location")
        if not location:
            raise CairnMarkError("presign response has no Location header")
        return location

    # -- metadata ------------------------------------------------------------

    async def get_metadata(self, file_id: str) -> File:
        """Fetch the metadata record for ``file_id``."""
        resp = await self._request("GET", _file_path(file_id) + "/metadata")
        return File.from_json(resp.json())

    async def update_metadata(
        self, file_id: str, tags: dict[str, Any], *, mode: str = "merge"
    ) -> File:
        """Merge (default) or replace (``mode="replace"``) the file's tags."""
        if mode not in ("merge", "replace"):
            raise ValueError(f'mode must be "merge" or "replace", not {mode!r}')
        params = {"mode": "replace"} if mode == "replace" else None
        resp = await self._request(
            "PATCH",
            _file_path(file_id) + "/metadata",
            params=params,
            headers={"Content-Type": "application/json"},
            content=json.dumps(tags).encode(),
        )
        return File.from_json(resp.json())

    async def delete(self, file_id: str) -> None:
        """Soft-delete ``file_id``; the stored object is purged asynchronously."""
        await self._request("DELETE", _file_path(file_id), expect=204)

    # -- listing ---------------------------------------------------------------

    async def list(
        self,
        *,
        content_type: str | None = None,
        tags: dict[str, str] | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> ListPage:
        """One page of files matching the filter, newest first."""
        resp = await self._request(
            "GET", "/files", params=list_params(content_type, tags, limit, cursor)
        )
        return ListPage.from_json(resp.json())

    async def iter_files(
        self,
        *,
        content_type: str | None = None,
        tags: dict[str, str] | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[File]:
        """Every file matching the filter, fetching pages lazily."""
        cursor: str | None = None
        while True:
            page = await self.list(content_type=content_type, tags=tags, limit=limit, cursor=cursor)
            for file in page.files:
                yield file
            if not page.next_cursor:
                return
            cursor = page.next_cursor

    # -- probes / lifecycle ------------------------------------------------------

    async def health(self) -> None:
        """Raise unless the server answers 200 on /healthz (liveness)."""
        await self._request("GET", "/healthz")

    async def ready(self) -> None:
        """Raise unless /readyz says storage and database are reachable."""
        await self._request("GET", "/readyz")

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncCairnMark:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


def _file_path(file_id: str) -> str:
    return "/files/" + quote(file_id, safe="")


def _as_async_content(content: Any) -> Any:
    """Make ``content`` acceptable to httpx.AsyncClient, which refuses sync
    streams. In-memory bytes and async iterables pass through; sync file-likes
    and iterators are wrapped into async generators (their reads still block —
    fine for files, use an async iterable for anything slower)."""
    if isinstance(content, (bytes, bytearray, memoryview)) or hasattr(content, "__aiter__"):
        return content
    read = cast("Callable[[int], bytes] | None", getattr(content, "read", None))
    if callable(read):

        async def from_file() -> AsyncIterator[bytes]:
            while chunk := read(65536):
                yield chunk

        return from_file()
    if hasattr(content, "__iter__"):
        iterator = cast("Iterator[bytes]", iter(content))

        async def from_iter() -> AsyncIterator[bytes]:
            for chunk in iterator:
                yield chunk

        return from_iter()
    return content

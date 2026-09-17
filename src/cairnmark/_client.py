"""The synchronous CairnMark client."""

from __future__ import annotations

import contextlib
import json
import os
import time
from typing import Any, Iterable, Iterator
from urllib.parse import quote

import httpx

from ._http import (
    DEFAULT_POLL_INTERVAL,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    backoff_delay,
    deadline_from,
    error_from_response,
    extract_request,
    job_error,
    list_params,
    next_poll_delay,
    range_header,
    remaining,
    resolve_idempotency_key,
    retry_delay,
    rewinder,
    unexpected_status,
    upload_params_headers,
    verify_bytes,
)
from .errors import (
    APIError,
    CairnMarkError,
    IdempotencyConflictError,
    InvalidParameterError,
    ServerError,
    WaitTimeoutError,
)
from .models import ArchiveEntry, ExtractSummary, File, Job, ListPage
from .version import __version__


class Download:
    """An open download stream plus the file's metadata record.

    Use as a context manager (or call ``close()``). With verification on,
    ChecksumMismatchError is raised as the last chunk is consumed.
    """

    def __init__(self, response: httpx.Response, file: File, verify: bool) -> None:
        self._response = response
        self._verify = verify
        self.file = file

    def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
        chunks = self._response.iter_bytes(chunk_size)
        if self._verify and self.file.checksum_sha256:
            return verify_bytes(chunks, self.file.checksum_sha256)
        return chunks

    def read(self) -> bytes:
        """Consume the whole stream (still verified) into memory."""
        return b"".join(self.iter_bytes())

    def close(self) -> None:
        self._response.close()

    def __enter__(self) -> Download:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class CairnMark:
    """Synchronous client for one CairnMark server.

    The server has no auth — put it behind your gateway and inject
    credentials via ``headers``. ``timeout`` is httpx's per-operation timeout
    (connect / single read), so it does not cut off large streams.
    ``poll_interval`` is the first wait between polls of an extraction job;
    each wait doubles up to a 10s ceiling.
    """

    def __init__(
        self,
        base_url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | httpx.Timeout = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        user_agent: str | None = None,
        transport: httpx.BaseTransport | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        self._retries = retries
        self._poll_interval = poll_interval
        merged = dict(headers or {})
        merged["User-Agent"] = user_agent or f"cairnmark-python/{__version__}"
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers=merged,
            timeout=timeout,
            transport=transport,
            follow_redirects=False,
        )

    # -- core request loop -------------------------------------------------

    def _request(
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
        """One non-streaming call, retried on network errors and 5xx."""
        attempt = 0
        while True:
            try:
                resp = self._http.request(
                    method,
                    path,
                    params=params,
                    headers=headers,
                    content=content,
                    follow_redirects=follow_redirects,
                )
            except httpx.TransportError:
                if attempt < self._retries:
                    time.sleep(backoff_delay(attempt))
                    attempt += 1
                    continue
                raise
            if resp.status_code < 400:
                if resp.status_code != expect:
                    raise unexpected_status(resp, expect)
                return resp
            err = error_from_response(resp)
            if isinstance(err, ServerError) and attempt < self._retries:
                time.sleep(retry_delay(err, attempt))
                attempt += 1
                continue
            raise err

    # -- uploads -------------------------------------------------------------

    def upload(
        self,
        content: Any,
        *,
        filename: str | None = None,
        content_type: str | None = None,
        size: int | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> File:
        """Stream ``content`` (bytes or a binary file-like/iterator) up.

        ``size`` sets Content-Length for file-like bodies whose length you
        know, letting the server reject oversized uploads up front. Pass
        ``idempotency_key="auto"`` to get a generated key, which is also what
        makes retries safe: without a key (or with a body that can't be
        rewound) a transient failure is raised, never retried, since a resend
        could duplicate the file.
        """
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
                resp = self._http.post("/files", params=params, headers=headers, content=content)
            except httpx.TransportError:
                if retryable and attempt < self._retries:
                    time.sleep(backoff_delay(attempt))
                    attempt += 1
                    continue
                raise
            if resp.status_code == 201:
                return File.from_json(resp.json())
            err = error_from_response(resp)
            if retryable and attempt < self._retries and err.status in (409, *range(500, 600)):
                time.sleep(retry_delay(err if err.status == 409 else None, attempt))
                attempt += 1
                continue
            raise err

    def upload_file(
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
            return self.upload(
                f,
                filename=filename or os.path.basename(os.fspath(path)),
                content_type=content_type,
                size=os.fstat(f.fileno()).st_size,
                metadata=metadata,
                idempotency_key=idempotency_key,
            )

    # -- downloads -----------------------------------------------------------

    def download(
        self,
        file_id: str,
        *,
        verify: bool = False,
        offset: int | None = None,
        length: int | None = None,
    ) -> Download:
        """Open the file's content for reading.

        Full downloads follow the server's 302 to a presigned object-store
        URL; ``offset``/``length`` request a 206 range through the server.
        ``verify`` hashes the stream against the stored SHA-256 (incompatible
        with a range — partial bytes can't match a whole-file hash).
        """
        ranged = offset is not None or length is not None
        if verify and ranged:
            raise InvalidParameterError("verify is incompatible with a range download")

        # The metadata record supplies the stored checksum for verification
        # and rounds out the result either way.
        file = self.get_metadata(file_id)
        if verify and not file.checksum_sha256:
            raise CairnMarkError(f"file {file_id} has no stored checksum to verify against")

        headers = {"Range": range_header(offset or 0, length)} if ranged else None
        expect = 206 if ranged else 200
        attempt = 0
        while True:
            request = self._http.build_request("GET", _file_path(file_id), headers=headers)
            try:
                resp = self._http.send(request, stream=True, follow_redirects=True)
            except httpx.TransportError:
                if attempt < self._retries:
                    time.sleep(backoff_delay(attempt))
                    attempt += 1
                    continue
                raise
            if resp.status_code == expect:
                return Download(resp, file, verify)
            resp.read()
            resp.close()
            err = error_from_response(resp)
            if isinstance(err, ServerError) and attempt < self._retries:
                time.sleep(backoff_delay(attempt))
                attempt += 1
                continue
            raise err

    def download_to_file(
        self, file_id: str, path: str | os.PathLike[str], *, verify: bool = True
    ) -> File:
        """Download into ``path`` (created or truncated), checksum-verified.

        On any failure — including a checksum mismatch — the partial file is
        removed.
        """
        with self.download(file_id, verify=verify) as dl:
            try:
                with open(path, "wb") as out:
                    for chunk in dl.iter_bytes():
                        out.write(chunk)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.remove(path)
                raise
        return dl.file

    def presign_url(self, file_id: str) -> str:
        """The presigned object-store URL, without following it.

        Hand it to a browser or another service; it expires after the
        server's configured TTL, and its host is only as reachable as the
        server's CAIRNMARK_S3_PUBLIC_ENDPOINT makes it.
        """
        resp = self._request("GET", _file_path(file_id), expect=302)
        location = resp.headers.get("location")
        if not location:
            raise CairnMarkError("presign response has no Location header")
        return location

    # -- metadata ------------------------------------------------------------

    def get_metadata(self, file_id: str) -> File:
        """Fetch the metadata record for ``file_id``."""
        resp = self._request("GET", _file_path(file_id) + "/metadata")
        return File.from_json(resp.json())

    def update_metadata(self, file_id: str, tags: dict[str, Any], *, mode: str = "merge") -> File:
        """Merge (default) or replace (``mode="replace"``) the file's tags."""
        if mode not in ("merge", "replace"):
            raise InvalidParameterError(f'mode must be "merge" or "replace", not {mode!r}')
        params = {"mode": "replace"} if mode == "replace" else None
        resp = self._request(
            "PATCH",
            _file_path(file_id) + "/metadata",
            params=params,
            headers={"Content-Type": "application/json"},
            content=json.dumps(tags).encode(),
        )
        return File.from_json(resp.json())

    def delete(self, file_id: str) -> None:
        """Soft-delete ``file_id``; the stored object is purged asynchronously."""
        self._request("DELETE", _file_path(file_id), expect=204)

    # -- listing ---------------------------------------------------------------

    def list(
        self,
        *,
        content_type: str | None = None,
        tags: dict[str, str] | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        entries: str | None = None,
    ) -> ListPage:
        """One page of files matching the filter, newest first.

        ``entries`` scopes files extracted from archives: ``"include"`` (the
        default) lists them with everything else, ``"exclude"`` hides them,
        ``"only"`` lists them alone. The archives themselves are found with
        ``tags={TAG_ARCHIVE: "true"}``.
        """
        params = list_params(content_type, tags, limit, cursor, entries)
        resp = self._request("GET", "/files", params=params)
        return ListPage.from_json(resp.json())

    def iter_files(
        self,
        *,
        content_type: str | None = None,
        tags: dict[str, str] | None = None,
        limit: int | None = None,
        entries: str | None = None,
    ) -> Iterator[File]:
        """Every file matching the filter, fetching pages lazily."""
        cursor: str | None = None
        while True:
            page = self.list(
                content_type=content_type, tags=tags, limit=limit, cursor=cursor, entries=entries
            )
            yield from page.files
            # A page without a cursor is the last (a final empty page is
            # normal when the total is an exact multiple of the page size).
            if not page.next_cursor:
                return
            cursor = page.next_cursor

    # -- archives ----------------------------------------------------------------

    def archive_entries(self, file_id: str) -> list[ArchiveEntry]:
        """List the entries of the zip stored as ``file_id``, each marked
        selectable or carrying the reason the server would skip it. Nothing is
        written. Raises NotArchiveError if the file is not a zip, TooLargeError
        if it has more entries than the server's cap, or a central directory
        larger than the server will parse."""
        resp = self._request("GET", _file_path(file_id) + "/archive")
        return [ArchiveEntry.from_json(e) for e in resp.json().get("entries") or []]

    def extract(
        self,
        file_id: str,
        *,
        entries: Iterable[int] | None = None,
        timeout: float | None = None,
    ) -> ExtractSummary:
        """Store the zip's entries as ordinary files — each tagged with
        TAG_ARCHIVE_ID, TAG_ARCHIVE_PATH and TAG_ARCHIVE_INDEX — and return
        the summary once the extraction has finished.

        The server runs an extraction as a job; this is ``extract_async``
        followed by ``wait_for_job``, kept as one call because most callers
        want exactly that. ``entries`` restricts the run to those directory
        indexes (from ``archive_entries``); None extracts every selectable
        entry, an empty list extracts nothing. ``timeout`` bounds the whole
        wait in seconds (None, the default, waits as long as it takes) and
        raises WaitTimeoutError without cancelling the job. Another
        extraction of the same archive in flight is waited out and this one
        then resubmitted, since extraction resumes past entries already
        stored and the other job's selection may not be ours; a re-run after
        success is a no-op that reports every entry as already_extracted. A
        job that fails raises ExtractionFailedError; one that is cancelled
        raises ExtractionCancelledError with the partial summary.
        """
        deadline = deadline_from(timeout)
        job = self._submit_waiting_out_conflicts(file_id, entries, deadline)
        job = self._wait(job.id, deadline)
        if job.status != "succeeded":
            raise job_error(job)
        if job.summary is None:
            raise CairnMarkError(f"extraction job {job.id} succeeded without a summary")
        return job.summary

    def extract_async(self, file_id: str, *, entries: Iterable[int] | None = None) -> Job:
        """Submit an extraction and return the pending job without waiting.

        Everything that can be refused up front is refused now, with the
        errors a synchronous run gave — NotArchiveError, TooLargeError,
        InvalidRequestError for a selection out of range — so a job only
        fails on what could not be known at submission. Another job for the
        same archive still pending or running raises
        IdempotencyConflictError, whose ``job_id`` names it: poll that one,
        or wait for it and resubmit, which is what ``extract`` does.
        """
        headers, content = extract_request(entries)
        resp = self._request(
            "POST", _file_path(file_id) + "/extract", headers=headers, content=content, expect=202
        )
        return Job.from_json(resp.json())

    def job(self, job_id: str) -> Job:
        """The job's current state. NotFoundError once the server has purged
        it — CAIRNMARK_JOB_RETENTION after it finished — so a poller slower
        than that loses the result; the extracted files are permanent."""
        resp = self._request("GET", _job_path(job_id))
        return Job.from_json(resp.json())

    def cancel_job(self, job_id: str) -> Job:
        """Ask the job to stop and return its state after the request.

        A pending job is cancelled at once; a running one stops after the
        entry it is writing and keeps the entries stored so far, so a later
        ``extract`` resumes past them; a finished job comes back unchanged.
        Idempotent.
        """
        resp = self._request("POST", _job_path(job_id) + "/cancel", expect=202)
        return Job.from_json(resp.json())

    def wait_for_job(self, job_id: str, *, timeout: float | None = None) -> Job:
        """Poll until the job is terminal and return it whatever the outcome
        — inspect ``status``, or let ``extract`` turn a failure or a
        cancellation into an error. Polls every ``poll_interval`` seconds,
        doubling to a 10s ceiling; ``timeout`` bounds the wait and raises
        WaitTimeoutError, which does not cancel the job."""
        return self._wait(job_id, deadline_from(timeout))

    def _wait(self, job_id: str, deadline: float | None) -> Job:
        delay = self._poll_interval
        while True:
            job = self.job(job_id)
            if job.terminal:
                return job
            left = remaining(deadline)
            if left is not None:
                if left <= 0:
                    raise WaitTimeoutError(job)
                delay = min(delay, left)
            time.sleep(delay)
            delay = next_poll_delay(delay, self._poll_interval)

    def _submit_waiting_out_conflicts(
        self, file_id: str, entries: Iterable[int] | None, deadline: float | None
    ) -> Job:
        """extract_async that, on a 409, waits for the job holding the archive
        to end — whatever its outcome — and resubmits, up to ``retries``
        times. Waiting on that job beats sleeping out Retry-After: it is
        exactly as long as needed and never longer."""
        selection = None if entries is None else list(entries)  # an iterator is one-shot
        attempt = 0
        while True:
            try:
                return self.extract_async(file_id, entries=selection)
            except IdempotencyConflictError as err:
                if attempt >= self._retries:
                    raise
                attempt += 1
                if err.job_id:
                    # A 404 (purged already) or any other failure to observe
                    # it means the archive is, or is about to be, free.
                    with contextlib.suppress(APIError):
                        self._wait(err.job_id, deadline)
                else:
                    time.sleep(retry_delay(err, attempt - 1))

    # -- probes / lifecycle ------------------------------------------------------

    def health(self) -> None:
        """Raise unless the server answers 200 on /healthz (liveness)."""
        self._request("GET", "/healthz")

    def ready(self) -> None:
        """Raise unless /readyz says storage and database are reachable."""
        self._request("GET", "/readyz")

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> CairnMark:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _file_path(file_id: str) -> str:
    return "/files/" + quote(file_id, safe="")


def _job_path(job_id: str) -> str:
    return "/jobs/" + quote(job_id, safe="")

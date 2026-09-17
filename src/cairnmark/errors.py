"""Typed errors for the CairnMark API's client-facing failure modes."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Job


class CairnMarkError(Exception):
    """Base class for every error this SDK raises deliberately."""


class InvalidParameterError(CairnMarkError, ValueError):
    """An argument this SDK rejects before any request is made.

    Also a :class:`ValueError`, which is what a caller passing a bad argument
    expects to catch, so that ``CairnMarkError`` really is the base of every
    error the SDK raises deliberately.
    """


class ChecksumMismatchError(CairnMarkError):
    """A verified download's bytes did not hash to the stored SHA-256.

    Raised from the stream once it is fully consumed — never by the server.
    """

    def __init__(self, got: str, expected: str) -> None:
        super().__init__(f"cairnmark: downloaded sha256 {got}, stored {expected}")
        self.got = got
        self.expected = expected


class APIError(CairnMarkError):
    """A non-2xx response from the server."""

    def __init__(
        self,
        status: int,
        message: str,
        retry_after: float | None = None,
        job_id: str | None = None,
    ) -> None:
        super().__init__(f"cairnmark: server returned {status}: {message}")
        self.status = status
        self.message = message
        #: Seconds the server asked to wait (409 conflicts); else None.
        self.retry_after = retry_after
        #: On a 409 extraction conflict: the id of the job holding the archive.
        self.job_id = job_id


class InvalidRequestError(APIError):
    """400: malformed request — bad id, bad parameters, bad metadata JSON."""


class NotFoundError(APIError):
    """404: the file id does not exist (or was soft-deleted), or the job id
    does not exist (or was purged past the server's retention)."""


class IdempotencyConflictError(APIError):
    """409: an upload with the same Idempotency-Key is still in flight
    (``retry_after`` says when to ask again), or another extraction job for
    the same archive is pending or running (``job_id`` names it — the one to
    poll)."""


class IdempotencyGoneError(APIError):
    """410: the file created under this Idempotency-Key was deleted.

    Retrying the same key can never succeed — switch to a new key.
    """


class TooLargeError(APIError):
    """413: the body exceeds a server cap (upload size or metadata patch), or
    an archive exceeds the extraction caps."""


class NotArchiveError(APIError):
    """415: an archive endpoint was pointed at a file that is not a zip."""


class RangeNotSatisfiableError(APIError):
    """416: the requested byte range lies outside the file."""


class ServerError(APIError):
    """5xx: the server failed; the message is generic by design."""


_BY_STATUS: dict[int, type[APIError]] = {
    400: InvalidRequestError,
    404: NotFoundError,
    409: IdempotencyConflictError,
    410: IdempotencyGoneError,
    413: TooLargeError,
    415: NotArchiveError,
    416: RangeNotSatisfiableError,
}


def error_class_for(status: int) -> type[APIError]:
    """The APIError subclass for an HTTP status."""
    cls = _BY_STATUS.get(status)
    if cls is not None:
        return cls
    return ServerError if status >= 500 else APIError


class JobError(CairnMarkError):
    """An extraction job ended other than by succeeding; ``job`` is its
    terminal state. Raised by ``extract``; never by ``wait_for_job``, which
    returns the job whatever its outcome."""

    def __init__(self, job: Job, message: str) -> None:
        super().__init__(message)
        self.job = job


class ExtractionFailedError(JobError):
    """The job failed; ``job.error`` says why."""

    def __init__(self, job: Job) -> None:
        super().__init__(job, f"cairnmark: extraction job {job.id} failed: {job.error}")


class ExtractionCancelledError(JobError):
    """The job was cancelled; ``job.summary`` is the partial result, and the
    entries stored before the cancel remain — a later ``extract`` resumes
    past them. Distinct from :class:`ExtractionFailedError` so "I cancelled
    this" and "it broke" are told apart."""

    def __init__(self, job: Job) -> None:
        super().__init__(
            job,
            f"cairnmark: extraction job {job.id} cancelled after "
            f"{job.progress.done} of {job.progress.total} entries",
        )


class WaitTimeoutError(CairnMarkError, TimeoutError):
    """The wait for a job outlasted its ``timeout``. A timeout stops the wait
    and nothing else: the job goes on, ``cancel_job`` cancels it, and
    ``wait_for_job`` picks the wait back up.

    ``job`` is the last state seen of the job that was being waited on: the
    one ``extract`` submitted, or — when the timeout struck while ``extract``
    was still waiting out another job that held the archive — that other
    job, in which case nothing of the caller's was submitted at all.
    """

    def __init__(self, job: Job) -> None:
        super().__init__(
            f"cairnmark: the wait for extraction job {job.id} timed out while it was "
            f"{job.status}; the job was not cancelled"
        )
        self.job = job

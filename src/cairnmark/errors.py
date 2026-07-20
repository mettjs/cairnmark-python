"""Typed errors for the CairnMark API's client-facing failure modes."""

from __future__ import annotations


class CairnMarkError(Exception):
    """Base class for every error this SDK raises deliberately."""


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

    def __init__(self, status: int, message: str, retry_after: float | None = None) -> None:
        super().__init__(f"cairnmark: server returned {status}: {message}")
        self.status = status
        self.message = message
        #: Seconds the server asked to wait (409 idempotency conflicts); else None.
        self.retry_after = retry_after


class InvalidRequestError(APIError):
    """400: malformed request — bad id, bad parameters, bad metadata JSON."""


class NotFoundError(APIError):
    """404: the file id does not exist (or was soft-deleted)."""


class IdempotencyConflictError(APIError):
    """409: an upload with the same Idempotency-Key is still in flight.

    ``retry_after`` says when to ask again.
    """


class IdempotencyGoneError(APIError):
    """410: the file created under this Idempotency-Key was deleted.

    Retrying the same key can never succeed — switch to a new key.
    """


class TooLargeError(APIError):
    """413: the body exceeds a server cap (upload size or metadata patch)."""


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
    416: RangeNotSatisfiableError,
}


def error_class_for(status: int) -> type[APIError]:
    """The APIError subclass for an HTTP status."""
    cls = _BY_STATUS.get(status)
    if cls is not None:
        return cls
    return ServerError if status >= 500 else APIError

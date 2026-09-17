"""Official Python client for the CairnMark file service.

Synchronous::

    from cairnmark import CairnMark

    with CairnMark("http://localhost:8080") as cm:
        f = cm.upload(b"hello", filename="hello.txt", metadata={"env": "demo"},
                      idempotency_key="auto")
        cm.download_to_file(f.id, "hello-copy.txt")   # checksum-verified
        summary = cm.extract(archive.id)               # a server-side job, waited for

Asynchronous::

    from cairnmark import AsyncCairnMark

    async with AsyncCairnMark("http://localhost:8080") as cm:
        f = await cm.upload(b"hello", filename="hello.txt")
        async for file in cm.iter_files(tags={"env": "demo"}):
            ...
"""

from ._async_client import AsyncCairnMark, AsyncDownload
from ._client import CairnMark, Download
from .errors import (
    APIError,
    CairnMarkError,
    ChecksumMismatchError,
    ExtractionCancelledError,
    ExtractionFailedError,
    IdempotencyConflictError,
    IdempotencyGoneError,
    InvalidParameterError,
    InvalidRequestError,
    JobError,
    NotArchiveError,
    NotFoundError,
    RangeNotSatisfiableError,
    ServerError,
    TooLargeError,
    WaitTimeoutError,
)
from .models import (
    JOB_STATUSES,
    TAG_ARCHIVE,
    TAG_ARCHIVE_ID,
    TAG_ARCHIVE_INDEX,
    TAG_ARCHIVE_PATH,
    TERMINAL_JOB_STATUSES,
    ArchiveEntry,
    ExtractSummary,
    File,
    Job,
    JobProgress,
    ListPage,
    SkippedEntry,
)
from .version import __version__

__all__ = [
    "APIError",
    "ArchiveEntry",
    "AsyncCairnMark",
    "AsyncDownload",
    "CairnMark",
    "CairnMarkError",
    "ChecksumMismatchError",
    "Download",
    "ExtractSummary",
    "ExtractionCancelledError",
    "ExtractionFailedError",
    "File",
    "IdempotencyConflictError",
    "IdempotencyGoneError",
    "InvalidParameterError",
    "InvalidRequestError",
    "JOB_STATUSES",
    "Job",
    "JobError",
    "JobProgress",
    "ListPage",
    "NotArchiveError",
    "NotFoundError",
    "RangeNotSatisfiableError",
    "ServerError",
    "SkippedEntry",
    "TAG_ARCHIVE",
    "TAG_ARCHIVE_ID",
    "TAG_ARCHIVE_INDEX",
    "TAG_ARCHIVE_PATH",
    "TERMINAL_JOB_STATUSES",
    "TooLargeError",
    "WaitTimeoutError",
    "__version__",
]

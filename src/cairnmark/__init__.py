"""Official Python client for the CairnMark file service.

Synchronous::

    from cairnmark import CairnMark

    with CairnMark("http://localhost:8080") as cm:
        f = cm.upload(b"hello", filename="hello.txt", metadata={"env": "demo"},
                      idempotency_key="auto")
        cm.download_to_file(f.id, "hello-copy.txt")   # checksum-verified

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
    IdempotencyConflictError,
    IdempotencyGoneError,
    InvalidRequestError,
    NotFoundError,
    RangeNotSatisfiableError,
    ServerError,
    TooLargeError,
)
from .models import File, ListPage
from .version import __version__

__all__ = [
    "APIError",
    "AsyncCairnMark",
    "AsyncDownload",
    "CairnMark",
    "CairnMarkError",
    "ChecksumMismatchError",
    "Download",
    "File",
    "IdempotencyConflictError",
    "IdempotencyGoneError",
    "InvalidRequestError",
    "ListPage",
    "NotFoundError",
    "RangeNotSatisfiableError",
    "ServerError",
    "TooLargeError",
    "__version__",
]

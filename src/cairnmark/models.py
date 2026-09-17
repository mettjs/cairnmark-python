"""Public data models returned by the CairnMark API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _parse_time(value: str) -> datetime:
    # The server emits RFC 3339 with a Z suffix; fromisoformat on Python 3.10
    # only accepts a numeric offset.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass(frozen=True)
class File:
    """A stored file's metadata record. Files are addressed by ``id`` only."""

    id: str
    filename: str
    content_type: str
    size_bytes: int
    #: Hex SHA-256 computed by the server during upload; None if absent.
    checksum_sha256: str | None
    #: The queryable tag set (arbitrary JSON values).
    metadata: dict[str, Any]
    created_at: datetime
    #: None until the file's tags are first changed — a quick way to tell an
    #: untouched original from an edited record.
    updated_at: datetime | None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> File:
        updated = data.get("updated_at")
        return cls(
            id=data["id"],
            filename=data["filename"],
            content_type=data["content_type"],
            size_bytes=data["size_bytes"],
            checksum_sha256=data.get("checksum_sha256"),
            metadata=data.get("metadata") or {},
            created_at=_parse_time(data["created_at"]),
            updated_at=_parse_time(updated) if updated else None,
        )


@dataclass(frozen=True)
class ListPage:
    """One page of list results.

    A non-None ``next_cursor`` means more may follow; its absence is the
    definitive end-of-list signal.
    """

    files: list[File]
    limit: int
    count: int
    next_cursor: str | None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> ListPage:
        return cls(
            files=[File.from_json(f) for f in data.get("files") or []],
            limit=data["limit"],
            count=data["count"],
            next_cursor=data.get("next_cursor"),
        )


# Tag keys the server writes on extracted archive entries and on archives.
# Reserved — the server rejects any client writing a ``cm:`` key — but
# readable: filter by them to find an archive's entries.
#: On an extracted entry: the id of the archive it came from.
TAG_ARCHIVE_ID = "cm:archive_id"
#: On an extracted entry: its full path inside the archive.
TAG_ARCHIVE_PATH = "cm:archive_path"
#: On an extracted entry: its index in the archive's directory (a number).
TAG_ARCHIVE_INDEX = "cm:archive_index"
#: On an archive that has been extracted, with the value "true".
TAG_ARCHIVE = "cm:archive"


@dataclass(frozen=True)
class ArchiveEntry:
    """One member of a stored zip, as read from its directory.

    ``index`` is the handle to extract by: names need not be unique inside a
    zip, indexes are, and the archive is immutable so an index stays valid.
    """

    index: int
    #: Full path inside the archive.
    name: str
    #: Uncompressed length in bytes.
    size: int
    #: From the name's extension; None when unknown (the server sniffs on extraction).
    content_type: str | None
    #: The archive's own checksum of the content, hex-encoded.
    crc32: str
    #: False when the server would skip the entry; ``reason`` says why.
    selectable: bool
    #: e.g. "platform_metadata", "encrypted", "unsupported_method"; None when selectable.
    reason: str | None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> ArchiveEntry:
        return cls(
            index=data["index"],
            name=data["name"],
            size=data["size"],
            content_type=data.get("content_type"),
            crc32=data["crc32"],
            selectable=bool(data.get("selectable")),
            reason=data.get("reason"),
        )


@dataclass(frozen=True)
class SkippedEntry:
    """One skipped entry and why."""

    index: int
    name: str
    reason: str


@dataclass(frozen=True)
class ExtractSummary:
    """The server's bounded report of one extraction.

    It never lists the files created: enumerate them with ``iter_files`` and a
    ``TAG_ARCHIVE_ID`` filter.
    """

    archive_id: str
    #: Entries in the archive's directory.
    entries: int
    extracted: int
    skipped: int
    #: Skips per reason ("platform_metadata", "already_extracted", "not_selected", …).
    skipped_by_reason: dict[str, int]
    #: The first skipped entries (at most 20).
    sample_skipped: list[SkippedEntry]

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> ExtractSummary:
        return cls(
            archive_id=data["archive_id"],
            entries=data["entries"],
            extracted=data["extracted"],
            skipped=data["skipped"],
            skipped_by_reason=dict(data.get("skipped_by_reason") or {}),
            sample_skipped=[
                SkippedEntry(index=s["index"], name=s["name"], reason=s["reason"])
                for s in data.get("sample_skipped") or []
            ],
        )


#: The states an extraction job can be in. A job goes back from "running" to
#: "pending" when its worker shuts down or stops reporting, so leaving
#: "running" is not the end — only the three terminal states are.
JOB_STATUSES = ("pending", "running", "succeeded", "failed", "cancelled")
TERMINAL_JOB_STATUSES = frozenset({"succeeded", "failed", "cancelled"})


@dataclass(frozen=True)
class JobProgress:
    """Where a run is: ``done`` entries processed of the ``total`` it intends
    to write — not the archive's entry count. A job resumed after an
    interruption counts only what remained."""

    done: int
    total: int


@dataclass(frozen=True)
class Job:
    """One extraction job, as the server reports it.

    A job id is a bearer capability like a file id: whoever holds it can poll
    and cancel the job.
    """

    id: str
    archive_id: str
    #: "pending" | "running" | "succeeded" | "failed" | "cancelled"
    status: str
    progress: JobProgress
    cancel_requested: bool
    #: The result once the job is terminal — the same shape ``extract``
    #: returns. None before then, and on a job that never ran (cancelled
    #: while pending, or failed before the archive could be opened).
    summary: ExtractSummary | None
    #: The reason when ``status`` is "failed".
    error: str | None
    created_at: datetime
    updated_at: datetime
    #: None until terminal.
    finished_at: datetime | None

    @property
    def terminal(self) -> bool:
        """Whether the job will not change state again."""
        return self.status in TERMINAL_JOB_STATUSES

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Job:
        progress = data.get("progress") or {}
        summary = data.get("summary")
        finished = data.get("finished_at")
        return cls(
            id=data["id"],
            archive_id=data["archive_id"],
            status=data["status"],
            progress=JobProgress(done=progress.get("done", 0), total=progress.get("total", 0)),
            cancel_requested=bool(data.get("cancel_requested")),
            summary=ExtractSummary.from_json(summary) if summary else None,
            error=data.get("error") or None,
            created_at=_parse_time(data["created_at"]),
            updated_at=_parse_time(data["updated_at"]),
            finished_at=_parse_time(finished) if finished else None,
        )

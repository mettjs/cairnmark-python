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

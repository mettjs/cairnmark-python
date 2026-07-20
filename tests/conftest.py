from typing import Any

import pytest

BASE = "http://test"

_FILE_JSON: dict[str, Any] = {
    "id": "abc",
    "filename": "a.txt",
    "content_type": "text/plain",
    "size_bytes": 5,
    "checksum_sha256": "cafe",
    "metadata": {"env": "demo"},
    "created_at": "2026-01-01T00:00:00Z",
    "updated_at": None,
}


@pytest.fixture
def file_json():
    """A canned file record; keyword overrides replace fields."""

    def make(**overrides: Any) -> dict[str, Any]:
        return {**_FILE_JSON, **overrides}

    return make

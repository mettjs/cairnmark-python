from datetime import datetime, timezone

import pytest
import respx

from cairnmark import CairnMark, NotFoundError

from conftest import BASE


@respx.mock
def test_get_metadata_parses_record(file_json):
    respx.get(f"{BASE}/files/abc/metadata").respond(200, json=file_json())
    cm = CairnMark(BASE)

    f = cm.get_metadata("abc")
    assert f.id == "abc"
    assert f.checksum_sha256 == "cafe"
    assert f.metadata == {"env": "demo"}
    assert f.created_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert f.updated_at is None  # untouched original


@respx.mock
@pytest.mark.parametrize("mode,expect_param", [("merge", None), ("replace", "replace")])
def test_update_metadata(file_json, mode, expect_param):
    route = respx.patch(f"{BASE}/files/abc/metadata").respond(
        200, json=file_json(updated_at="2026-02-01T00:00:00Z")
    )
    cm = CairnMark(BASE)

    f = cm.update_metadata("abc", {"reviewed": True}, mode=mode)
    assert f.updated_at is not None
    req = route.calls.last.request
    assert req.url.params.get("mode") == expect_param
    assert req.content == b'{"reviewed": true}'
    assert req.headers["Content-Type"] == "application/json"


def test_update_metadata_rejects_bad_mode():
    cm = CairnMark(BASE)
    with pytest.raises(ValueError):
        cm.update_metadata("abc", {}, mode="overwrite")


@respx.mock
def test_delete():
    route = respx.delete(f"{BASE}/files/abc").respond(204)
    cm = CairnMark(BASE)
    cm.delete("abc")
    assert route.called


@respx.mock
def test_delete_not_found():
    respx.delete(f"{BASE}/files/nope").respond(404, json={"error": "file not found"})
    cm = CairnMark(BASE, retries=0)
    with pytest.raises(NotFoundError):
        cm.delete("nope")


@respx.mock
def test_file_id_is_path_escaped(file_json):
    route = respx.get(f"{BASE}/files/a%2Fb/metadata").respond(200, json=file_json())
    cm = CairnMark(BASE)
    cm.get_metadata("a/b")
    assert route.called

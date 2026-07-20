import io
import json

import httpx
import pytest
import respx

from cairnmark import CairnMark, ServerError, TooLargeError

from conftest import BASE


@respx.mock
def test_upload_sends_body_and_headers(file_json):
    route = respx.post(f"{BASE}/files").respond(201, json=file_json())
    cm = CairnMark(BASE)

    f = cm.upload(
        b"hello",
        filename="a.txt",
        content_type="text/plain",
        metadata={"env": "demo", "n": 3},
        idempotency_key="k1",
    )
    assert f.id == "abc"
    req = route.calls.last.request
    assert req.url.params["filename"] == "a.txt"
    assert req.headers["Content-Type"] == "text/plain"
    assert req.headers["Idempotency-Key"] == "k1"
    assert json.loads(req.headers["X-Metadata"]) == {"env": "demo", "n": 3}
    assert req.content == b"hello"


@respx.mock
def test_upload_retries_rewind_the_body(file_json):
    bodies = []

    def handler(request):
        bodies.append(request.read())
        if len(bodies) == 1:
            return httpx.Response(500, json={"error": "transient"})
        return httpx.Response(201, json=file_json())

    respx.post(f"{BASE}/files").mock(side_effect=handler)
    cm = CairnMark(BASE)

    cm.upload(io.BytesIO(b"hello"), idempotency_key="k1")
    assert bodies == [b"hello", b"hello"]


@respx.mock
def test_upload_without_key_does_not_retry():
    route = respx.post(f"{BASE}/files").respond(500, json={"error": "transient"})
    cm = CairnMark(BASE)

    with pytest.raises(ServerError):
        cm.upload(b"hello")
    assert route.call_count == 1


@respx.mock
def test_upload_unrewindable_body_does_not_retry():
    route = respx.post(f"{BASE}/files").respond(500, json={"error": "transient"})
    cm = CairnMark(BASE)

    with pytest.raises(ServerError):
        cm.upload(iter([b"hello"]), idempotency_key="k1")
    assert route.call_count == 1


@respx.mock
def test_upload_auto_idempotency_generates_distinct_keys(file_json):
    route = respx.post(f"{BASE}/files").respond(201, json=file_json())
    cm = CairnMark(BASE)

    for _ in range(2):
        cm.upload(b"x", idempotency_key="auto")
    keys = {call.request.headers["Idempotency-Key"] for call in route.calls}
    assert len(keys) == 2
    assert all(0 < len(k) <= 255 for k in keys)


@respx.mock
def test_upload_too_large():
    respx.post(f"{BASE}/files").respond(413, json={"error": "upload exceeds the 10-byte limit"})
    cm = CairnMark(BASE, retries=0)
    with pytest.raises(TooLargeError):
        cm.upload(b"hello")


@respx.mock
def test_upload_file_infers_name_and_size(tmp_path, file_json):
    route = respx.post(f"{BASE}/files").respond(201, json=file_json())
    src = tmp_path / "hello.txt"
    src.write_bytes(b"hello from disk")
    cm = CairnMark(BASE)

    cm.upload_file(src, metadata={"env": "demo"})
    req = route.calls.last.request
    assert req.url.params["filename"] == "hello.txt"
    assert req.headers["Content-Length"] == str(len(b"hello from disk"))
    assert req.read() == b"hello from disk"

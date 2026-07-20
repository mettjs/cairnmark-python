"""The async client mirrors the sync one; these cover its key flows."""

import hashlib
import io
import json

import httpx
import pytest
import respx

from cairnmark import AsyncCairnMark, ChecksumMismatchError, NotFoundError

from conftest import BASE

CONTENT = b"hello world"
CHECKSUM = hashlib.sha256(CONTENT).hexdigest()
STORE = "http://store.test/bucket/abc?X-Amz-Signature=sig"


@respx.mock
async def test_async_upload(file_json):
    route = respx.post(f"{BASE}/files").respond(201, json=file_json())
    async with AsyncCairnMark(BASE) as cm:
        f = await cm.upload(
            b"hello", filename="a.txt", metadata={"env": "demo"}, idempotency_key="k1"
        )
    assert f.id == "abc"
    req = route.calls.last.request
    assert req.url.params["filename"] == "a.txt"
    assert req.headers["Idempotency-Key"] == "k1"
    assert json.loads(req.headers["X-Metadata"]) == {"env": "demo"}
    assert req.content == b"hello"


@respx.mock
async def test_async_upload_retries_rewind_the_body(file_json):
    bodies = []

    async def handler(request):
        bodies.append(await request.aread())
        if len(bodies) == 1:
            return httpx.Response(500, json={"error": "transient"})
        return httpx.Response(201, json=file_json())

    respx.post(f"{BASE}/files").mock(side_effect=handler)
    async with AsyncCairnMark(BASE) as cm:
        await cm.upload(io.BytesIO(b"hello"), idempotency_key="k1")
    assert bodies == [b"hello", b"hello"]


def _mock_download(file_json, checksum=CHECKSUM):
    respx.get(f"{BASE}/files/abc/metadata").respond(
        200, json=file_json(checksum_sha256=checksum, size_bytes=len(CONTENT))
    )
    respx.get(f"{BASE}/files/abc").respond(302, headers={"Location": STORE})
    respx.get(STORE).respond(200, content=CONTENT)


@respx.mock
async def test_async_download_verifies(file_json):
    _mock_download(file_json)
    async with AsyncCairnMark(BASE) as cm:
        async with await cm.download("abc", verify=True) as dl:
            assert await dl.aread() == CONTENT
        assert dl.file.id == "abc"


@respx.mock
async def test_async_download_detects_corruption(file_json):
    _mock_download(file_json, checksum="deadbeef")
    async with AsyncCairnMark(BASE) as cm:
        async with await cm.download("abc", verify=True) as dl:
            with pytest.raises(ChecksumMismatchError):
                await dl.aread()


@respx.mock
async def test_async_presign_url(file_json):
    _mock_download(file_json)
    async with AsyncCairnMark(BASE) as cm:
        assert await cm.presign_url("abc") == STORE


@respx.mock
async def test_async_iter_files(file_json):
    def handler(request):
        cursor = request.url.params.get("cursor")
        start = int(cursor.removeprefix("f")) if cursor else 0
        files = [file_json(id=f"f{i + 1}") for i in range(start, min(start + 2, 4))]
        body = {"files": files, "limit": 2, "count": len(files)}
        if len(files) == 2:
            body["next_cursor"] = f"f{start + 2}"
        return httpx.Response(200, json=body)

    respx.get(f"{BASE}/files").mock(side_effect=handler)
    async with AsyncCairnMark(BASE) as cm:
        ids = [f.id async for f in cm.iter_files(limit=2)]
    assert ids == ["f1", "f2", "f3", "f4"]


@respx.mock
async def test_async_not_found():
    respx.get(f"{BASE}/files/nope/metadata").respond(404, json={"error": "file not found"})
    async with AsyncCairnMark(BASE, retries=0) as cm:
        with pytest.raises(NotFoundError):
            await cm.get_metadata("nope")

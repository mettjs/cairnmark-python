import hashlib

import httpx
import pytest
import respx

from cairnmark import CairnMark, ChecksumMismatchError, NotFoundError

from conftest import BASE

CONTENT = b"hello world"
CHECKSUM = hashlib.sha256(CONTENT).hexdigest()
STORE = "http://store.test/bucket/abc?X-Amz-Signature=sig"


def mock_download(file_json, content=CONTENT, checksum=CHECKSUM):
    """Mimic the server's contract: metadata, 302 presign, 206 for ranges."""
    respx.get(f"{BASE}/files/abc/metadata").respond(
        200, json=file_json(checksum_sha256=checksum, size_bytes=len(content))
    )

    def files_handler(request):
        rng = request.headers.get("Range")
        if rng:
            start, end = rng.removeprefix("bytes=").split("-")
            return httpx.Response(206, content=content[int(start) : int(end) + 1])
        return httpx.Response(302, headers={"Location": STORE})

    respx.get(f"{BASE}/files/abc").mock(side_effect=files_handler)
    return respx.get(STORE).respond(200, content=content)


@respx.mock
def test_download_follows_presign_and_verifies(file_json):
    mock_download(file_json)
    cm = CairnMark(BASE)

    with cm.download("abc", verify=True) as dl:
        assert dl.read() == CONTENT
    assert dl.file.id == "abc"


@respx.mock
def test_download_verify_detects_corruption(file_json):
    mock_download(file_json, checksum="deadbeef")  # not the hash of CONTENT
    cm = CairnMark(BASE)

    with cm.download("abc", verify=True) as dl:
        with pytest.raises(ChecksumMismatchError):
            dl.read()


@respx.mock
def test_download_range(file_json):
    store = mock_download(file_json)
    cm = CairnMark(BASE)

    with cm.download("abc", offset=6, length=5) as dl:
        assert dl.read() == b"world"
    assert not store.called  # ranges stream through the server, not the store


def test_download_verify_rejects_range():
    cm = CairnMark(BASE)
    with pytest.raises(ValueError):
        cm.download("abc", verify=True, offset=1)


@respx.mock
def test_download_to_file(tmp_path, file_json):
    mock_download(file_json)
    cm = CairnMark(BASE)
    dest = tmp_path / "out.bin"

    f = cm.download_to_file("abc", dest)
    assert f.id == "abc"
    assert dest.read_bytes() == CONTENT


@respx.mock
def test_download_to_file_removes_corrupt_result(tmp_path, file_json):
    mock_download(file_json, checksum="deadbeef")
    cm = CairnMark(BASE)
    dest = tmp_path / "out.bin"

    with pytest.raises(ChecksumMismatchError):
        cm.download_to_file("abc", dest)
    assert not dest.exists()


@respx.mock
def test_presign_url_does_not_follow(file_json):
    store = mock_download(file_json)
    cm = CairnMark(BASE)

    assert cm.presign_url("abc") == STORE
    assert not store.called


@respx.mock
def test_download_not_found():
    respx.get(f"{BASE}/files/nope/metadata").respond(404, json={"error": "file not found"})
    cm = CairnMark(BASE, retries=0)
    with pytest.raises(NotFoundError):
        cm.download("nope")

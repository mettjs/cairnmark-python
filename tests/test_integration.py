"""Full round-trip against a live server (the service repo's compose stack):

    docker compose up -d --build   # in the CairnMark repo
    uv run pytest -m integration

Honors CAIRNMARK_BASE_URL (default http://localhost:8080).
"""

import os
import time

import pytest

from cairnmark import AsyncCairnMark, CairnMark, NotFoundError

pytestmark = pytest.mark.integration

BASE_URL = os.environ.get("CAIRNMARK_BASE_URL", "http://localhost:8080")
CONTENT = b"integration round-trip payload"


def test_sync_round_trip(tmp_path):
    tag = f"it-py-{time.time_ns()}"  # unique per run
    with CairnMark(BASE_URL) as cm:
        cm.ready()

        f = cm.upload(
            CONTENT,
            filename="roundtrip.txt",
            content_type="text/plain",
            metadata={"suite": tag},
            idempotency_key="auto",
        )
        assert f.checksum_sha256

        # Tag search finds exactly this file.
        found = [g.id for g in cm.iter_files(tags={"suite": tag})]
        assert found == [f.id]

        # Presign URL minted without being followed.
        assert cm.presign_url(f.id)

        # Verified full download via the presign redirect.
        dest = tmp_path / "out.bin"
        cm.download_to_file(f.id, dest)
        assert dest.read_bytes() == CONTENT

        # Range download of bytes 12..21 ("round-trip").
        with cm.download(f.id, offset=12, length=10) as dl:
            assert dl.read() == CONTENT[12:22]

        # Metadata patch flips updated_at from None and keeps merged tags.
        patched = cm.update_metadata(f.id, {"reviewed": True})
        assert patched.updated_at is not None
        assert patched.metadata["suite"] == tag

        # Delete, then confirm the id is gone.
        cm.delete(f.id)
        with pytest.raises(NotFoundError):
            cm.get_metadata(f.id)


async def test_async_round_trip():
    tag = f"it-pyasync-{time.time_ns()}"
    async with AsyncCairnMark(BASE_URL) as cm:
        await cm.ready()

        f = await cm.upload(
            CONTENT, filename="roundtrip.txt", metadata={"suite": tag}, idempotency_key="auto"
        )

        found = [g.id async for g in cm.iter_files(tags={"suite": tag})]
        assert found == [f.id]

        async with await cm.download(f.id, verify=True) as dl:
            assert await dl.aread() == CONTENT

        await cm.delete(f.id)
        with pytest.raises(NotFoundError):
            await cm.get_metadata(f.id)

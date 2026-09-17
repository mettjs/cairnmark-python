"""Full round-trip against a live server (the service repo's compose stack):

    docker compose up -d --build   # in the CairnMark repo
    uv run pytest -m integration

Honors CAIRNMARK_BASE_URL (default http://localhost:8080).
"""

import io
import os
import time
import zipfile

import pytest

from cairnmark import TAG_ARCHIVE_ID, TAG_ARCHIVE_PATH, AsyncCairnMark, CairnMark, NotFoundError

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


DOC = b"%PDF-1.7 integration quarterly report"


def _zip_fixture() -> bytes:
    """Two documents plus the resource-fork twin Finder would add."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("reports/q3.pdf", DOC)
        zf.writestr("reports/data.json", b'{"ok":true}')
        zf.writestr("__MACOSX/reports/._q3.pdf", b"junk")
    return buf.getvalue()


def test_sync_archive():
    with CairnMark(BASE_URL) as cm:
        cm.ready()
        arch = cm.upload(
            _zip_fixture(),
            filename="docs.zip",
            content_type="application/zip",
            idempotency_key="auto",
        )
        try:
            # List: the junk is flagged, the document is selectable with its type.
            entries = cm.archive_entries(arch.id)
            q3 = next(e for e in entries if e.name == "reports/q3.pdf")
            assert q3.selectable and q3.content_type == "application/pdf" and q3.size == len(DOC)
            junk = next(e for e in entries if e.name.startswith("__MACOSX/"))
            assert not junk.selectable and junk.reason == "platform_metadata"

            # Extract just that entry.
            summary = cm.extract(arch.id, entries=[q3.index])
            assert summary.extracted == 1
            assert summary.skipped_by_reason["not_selected"] == 1
            assert summary.skipped_by_reason["platform_metadata"] == 1

            # Find it by its tags and download it verified.
            children = list(
                cm.iter_files(tags={TAG_ARCHIVE_ID: arch.id, TAG_ARCHIVE_PATH: "reports/q3.pdf"})
            )
            assert len(children) == 1
            child = children[0]
            try:
                assert child.filename == "q3.pdf"
                with cm.download(child.id, verify=True) as dl:
                    assert dl.read() == DOC

                # The scoped listing sees it; a re-run is a no-op.
                assert cm.list(tags={TAG_ARCHIVE_ID: arch.id}, entries="only").count == 1
                again = cm.extract(arch.id, entries=[q3.index])
                assert again.extracted == 0
                assert again.skipped_by_reason["already_extracted"] == 1

                # The job surface underneath: submit without waiting, read it,
                # wait for it, and cancel a finished job as a no-op.
                job = cm.extract_async(arch.id)
                assert job.archive_id == arch.id and not job.terminal
                assert cm.job(job.id).id == job.id
                done = cm.wait_for_job(job.id)
                assert done.status == "succeeded" and done.summary is not None
                assert done.summary.extracted == 1  # data.json, the entry not selected before
                assert cm.cancel_job(job.id).status == "succeeded"
                # Cancel straight after submitting: usually still pending and
                # cancelled outright, but a fast pickup may finish it first —
                # both are correct terminal states.
                job = cm.extract_async(arch.id)
                cm.cancel_job(job.id)
                assert cm.wait_for_job(job.id).status in ("cancelled", "succeeded")
                for extra in cm.iter_files(tags={TAG_ARCHIVE_ID: arch.id}):
                    if extra.id != child.id:
                        cm.delete(extra.id)
            finally:
                cm.delete(child.id)
        finally:
            cm.delete(arch.id)


async def test_async_archive():
    async with AsyncCairnMark(BASE_URL) as cm:
        arch = await cm.upload(_zip_fixture(), filename="docs.zip", idempotency_key="auto")
        try:
            entries = await cm.archive_entries(arch.id)
            q3 = next(e for e in entries if e.name == "reports/q3.pdf")
            summary = await cm.extract(arch.id, entries=[q3.index])
            assert summary.extracted == 1
            children = [
                c async for c in cm.iter_files(tags={TAG_ARCHIVE_ID: arch.id}, entries="only")
            ]
            assert [c.filename for c in children] == ["q3.pdf"]

            job = await cm.extract_async(arch.id, entries=[q3.index])
            done = await cm.wait_for_job(job.id)
            assert done.status == "succeeded" and done.summary is not None
            assert done.summary.skipped_by_reason["already_extracted"] == 1
            assert (await cm.cancel_job(job.id)).status == "succeeded"
            await cm.delete(children[0].id)
        finally:
            await cm.delete(arch.id)

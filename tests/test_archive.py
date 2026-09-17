import httpx
import pytest
import respx

from cairnmark import (
    ArchiveEntry,
    AsyncCairnMark,
    CairnMark,
    ExtractionCancelledError,
    ExtractionFailedError,
    ExtractSummary,
    IdempotencyConflictError,
    Job,
    JobError,
    JobProgress,
    NotArchiveError,
    NotFoundError,
    SkippedEntry,
    TooLargeError,
    WaitTimeoutError,
)

from conftest import BASE

LISTING = {
    "archive_id": "zip1",
    "entries": [
        {
            "index": 0,
            "name": "reports/q3.pdf",
            "size": 184322,
            "content_type": "application/pdf",
            "crc32": "8f2a91c4",
            "selectable": True,
        },
        {
            "index": 1,
            "name": "__MACOSX/._q3.pdf",
            "size": 220,
            "crc32": "d1c0b3aa",
            "selectable": False,
            "reason": "platform_metadata",
        },
    ],
}

SUMMARY = {
    "archive_id": "zip1",
    "entries": 2,
    "extracted": 1,
    "skipped": 1,
    "skipped_by_reason": {"platform_metadata": 1},
    "sample_skipped": [{"index": 1, "name": "__MACOSX/._q3.pdf", "reason": "platform_metadata"}],
}

EXPECTED_SUMMARY = ExtractSummary(
    archive_id="zip1",
    entries=2,
    extracted=1,
    skipped=1,
    skipped_by_reason={"platform_metadata": 1},
    sample_skipped=[SkippedEntry(index=1, name="__MACOSX/._q3.pdf", reason="platform_metadata")],
)


def job_json(status, done=0, total=0, summary=None, error=None, job_id="j1"):
    """A job in the server's shape."""
    terminal = status in ("succeeded", "failed", "cancelled")
    body = {
        "id": job_id,
        "archive_id": "zip1",
        "status": status,
        "progress": {"done": done, "total": total},
        "cancel_requested": status == "cancelled",
        "summary": summary,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:05Z",
        "finished_at": "2026-01-01T00:00:10Z" if terminal else None,
    }
    if error:
        body["error"] = error
    return body


PENDING = job_json("pending")
SUCCEEDED = job_json("succeeded", 2, 2, SUMMARY)


def script(*bodies):
    """A side effect that answers each poll with the next body and then
    sticks at the last — a terminal job keeps answering the same way."""
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json=bodies[min(len(calls) - 1, len(bodies) - 1)])

    return respond


def client(**kw) -> CairnMark:
    return CairnMark(BASE, poll_interval=0.001, **kw)


@respx.mock
def test_archive_entries_parses_listing():
    route = respx.get(f"{BASE}/files/zip1/archive").respond(200, json=LISTING)
    cm = CairnMark(BASE)

    entries = cm.archive_entries("zip1")
    assert route.called
    assert entries[0] == ArchiveEntry(
        index=0,
        name="reports/q3.pdf",
        size=184322,
        content_type="application/pdf",
        crc32="8f2a91c4",
        selectable=True,
        reason=None,
    )
    junk = entries[1]
    assert junk.selectable is False and junk.reason == "platform_metadata"
    assert junk.content_type is None


@respx.mock
def test_archive_entries_not_archive():
    respx.get(f"{BASE}/files/txt/archive").respond(
        415, json={"error": "files: not a supported archive (zip)"}
    )
    cm = CairnMark(BASE, retries=0)
    with pytest.raises(NotArchiveError):
        cm.archive_entries("txt")


@respx.mock
@pytest.mark.parametrize(
    "entries,want_body,want_type",
    [
        (None, b"", None),
        ([0, 4], b'{"entries":[0,4]}', "application/json"),
        ([], b'{"entries":[]}', "application/json"),
        (iter((3,)), b'{"entries":[3]}', "application/json"),
    ],
)
def test_extract_sends_selection_and_waits(entries, want_body, want_type):
    submit = respx.post(f"{BASE}/files/zip1/extract").respond(
        202, json=PENDING, headers={"Location": "/jobs/j1"}
    )
    polls = respx.get(f"{BASE}/jobs/j1").mock(
        side_effect=script(PENDING, job_json("running", 1, 2), SUCCEEDED)
    )

    summary = client().extract("zip1", entries=entries)
    assert summary == EXPECTED_SUMMARY
    req = submit.calls.last.request
    assert req.content == want_body
    assert req.headers.get("Content-Type") == want_type
    # Submitted once, polled until terminal.
    assert submit.call_count == 1
    assert polls.call_count == 3


@respx.mock
def test_extract_async_returns_the_pending_job_at_once():
    respx.post(f"{BASE}/files/zip1/extract").respond(202, json=PENDING)
    polls = respx.get(f"{BASE}/jobs/j1").respond(200, json=SUCCEEDED)

    job = client().extract_async("zip1", entries=[0])
    assert job == Job(
        id="j1",
        archive_id="zip1",
        status="pending",
        progress=JobProgress(0, 0),
        cancel_requested=False,
        summary=None,
        error=None,
        created_at=job.created_at,
        updated_at=job.updated_at,
        finished_at=None,
    )
    assert not job.terminal
    assert not polls.called


@respx.mock
def test_extract_waits_for_the_active_job_then_resubmits():
    # Another job holds the archive: 409 naming it. Its selection may not be
    # ours, so the right move is to wait for it, then submit our own.
    submit = respx.post(f"{BASE}/files/zip1/extract").mock(
        side_effect=[
            httpx.Response(
                409,
                json={"error": "extraction already in progress", "job_id": "other"},
                headers={"Retry-After": "30"},
            ),
            httpx.Response(202, json=PENDING),
        ]
    )
    other = respx.get(f"{BASE}/jobs/other").mock(
        side_effect=script(
            job_json("running", 3, 9, job_id="other"),
            job_json("failed", 3, 9, error="boom", job_id="other"),
        )
    )
    mine = respx.get(f"{BASE}/jobs/j1").respond(200, json=SUCCEEDED)

    assert client().extract("zip1").extracted == 1
    assert submit.call_count == 2
    assert other.call_count == 2  # waited out, whatever its outcome
    assert mine.called


@respx.mock
def test_extract_async_raises_the_conflict_with_the_job_id():
    respx.post(f"{BASE}/files/zip1/extract").respond(
        409,
        json={"error": "extraction already in progress", "job_id": "other"},
        headers={"Retry-After": "30"},
    )
    polls = respx.get(f"{BASE}/jobs/other").respond(200, json=SUCCEEDED)

    with pytest.raises(IdempotencyConflictError) as exc:
        client().extract_async("zip1")
    assert exc.value.job_id == "other" and exc.value.retry_after == 30.0
    assert not polls.called
    # With retries off the blocking call gives up on the conflict too.
    with pytest.raises(IdempotencyConflictError):
        client(retries=0).extract("zip1")


@respx.mock
def test_wait_timeout_does_not_cancel():
    respx.post(f"{BASE}/files/zip1/extract").respond(202, json=PENDING)
    respx.get(f"{BASE}/jobs/j1").respond(200, json=job_json("running", 1, 2))
    cancel = respx.post(f"{BASE}/jobs/j1/cancel").respond(202, json=job_json("cancelled"))

    with pytest.raises(WaitTimeoutError) as exc:
        client().extract("zip1", timeout=0.02)
    assert isinstance(exc.value, TimeoutError)
    assert exc.value.job.status == "running"
    assert not cancel.called


@respx.mock
def test_polls_use_the_client_timeout():
    # Each request an extraction makes is a small call under the client's
    # timeout; only the wait as a whole is unbounded.
    respx.post(f"{BASE}/files/zip1/extract").respond(202, json=PENDING)
    polls = respx.get(f"{BASE}/jobs/j1").respond(200, json=SUCCEEDED)

    client(timeout=7.5).extract("zip1")
    assert polls.calls.last.request.extensions["timeout"]["read"] == 7.5


@respx.mock
def test_failed_and_cancelled_are_distinct():
    respx.post(f"{BASE}/files/zip1/extract").respond(202, json=PENDING)
    respx.get(f"{BASE}/jobs/j1").respond(
        200, json=job_json("failed", 1, 2, error="files: store object: boom")
    )
    with pytest.raises(ExtractionFailedError) as failed:
        client().extract("zip1")
    assert isinstance(failed.value, JobError)
    assert failed.value.job.error == "files: store object: boom"
    assert "boom" in str(failed.value)

    respx.get(f"{BASE}/jobs/j1").respond(200, json=job_json("cancelled", 1, 2, SUMMARY))
    with pytest.raises(ExtractionCancelledError) as cancelled:
        client().extract("zip1")
    assert not isinstance(cancelled.value, ExtractionFailedError)
    # The partial summary rides along.
    assert cancelled.value.job.summary == EXPECTED_SUMMARY
    assert cancelled.value.job.progress == JobProgress(1, 2)


@respx.mock
def test_job_and_cancel_job():
    respx.get(f"{BASE}/jobs/j1").respond(200, json=job_json("running", 1, 2))
    cancel = respx.post(f"{BASE}/jobs/j1/cancel").respond(
        202, json=job_json("cancelled", 1, 2, SUMMARY)
    )
    cm = client()

    job = cm.job("j1")
    assert job.status == "running" and job.progress == JobProgress(1, 2) and job.summary is None
    job = cm.cancel_job("j1")
    assert cancel.called
    assert job.status == "cancelled" and job.terminal and job.finished_at is not None


@respx.mock
def test_job_gone_after_retention():
    respx.get(f"{BASE}/jobs/old").respond(404, json={"error": "files: extraction job not found"})
    with pytest.raises(NotFoundError):
        client(retries=0).wait_for_job("old")


@respx.mock
def test_extract_cap_breach_is_too_large():
    # Caps are checked at submission, so the blocking call fails at once.
    respx.post(f"{BASE}/files/zip1/extract").respond(
        413, json={"error": "archive exceeds the extraction limits: 1200 entries"}
    )
    with pytest.raises(TooLargeError) as exc:
        client(retries=0).extract("zip1")
    assert "1200 entries" in exc.value.message


@respx.mock
async def test_async_extract_flow():
    submit = respx.post(f"{BASE}/files/zip1/extract").respond(202, json=PENDING)
    polls = respx.get(f"{BASE}/jobs/j1").mock(
        side_effect=script(PENDING, job_json("running", 1, 2), SUCCEEDED)
    )
    async with AsyncCairnMark(BASE, poll_interval=0.001) as cm:
        assert await cm.extract("zip1", entries=[0]) == EXPECTED_SUMMARY
        assert submit.calls.last.request.content == b'{"entries":[0]}'
        assert polls.call_count == 3

        job = await cm.extract_async("zip1")
        assert job.status == "pending"
        assert (await cm.job("j1")).terminal
        assert (await cm.wait_for_job("j1")).summary == EXPECTED_SUMMARY


@respx.mock
async def test_async_cancelled_and_timeout():
    respx.post(f"{BASE}/files/zip1/extract").respond(202, json=PENDING)
    respx.get(f"{BASE}/jobs/j1").respond(200, json=job_json("cancelled", 1, 2, SUMMARY))
    cancel = respx.post(f"{BASE}/jobs/j1/cancel").respond(
        202, json=job_json("cancelled", 1, 2, SUMMARY)
    )
    async with AsyncCairnMark(BASE, poll_interval=0.001) as cm:
        with pytest.raises(ExtractionCancelledError) as exc:
            await cm.extract("zip1")
        assert exc.value.job.summary == EXPECTED_SUMMARY
        assert (await cm.cancel_job("j1")).status == "cancelled"
        assert cancel.called

        respx.get(f"{BASE}/jobs/j1").respond(200, json=job_json("running", 1, 2))
        with pytest.raises(WaitTimeoutError):
            await cm.wait_for_job("j1", timeout=0.02)


@respx.mock
def test_timeout_while_waiting_out_another_job_names_that_job():
    # The wait on the job holding the archive shares extract's deadline. A
    # timeout there is raised as-is, with that job as the last state seen —
    # nothing of ours was ever submitted.
    submit = respx.post(f"{BASE}/files/zip1/extract").respond(
        409, json={"error": "busy", "job_id": "other"}, headers={"Retry-After": "30"}
    )
    respx.get(f"{BASE}/jobs/other").respond(200, json=job_json("running", 3, 9, job_id="other"))

    with pytest.raises(WaitTimeoutError) as exc:
        client().extract("zip1", timeout=0.02)
    assert exc.value.job.id == "other"
    assert submit.call_count == 1

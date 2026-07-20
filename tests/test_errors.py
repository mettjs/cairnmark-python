import httpx
import pytest
import respx

from cairnmark import (
    APIError,
    CairnMark,
    IdempotencyConflictError,
    IdempotencyGoneError,
    InvalidRequestError,
    NotFoundError,
    RangeNotSatisfiableError,
    ServerError,
    TooLargeError,
)

from conftest import BASE

CASES = [
    (400, InvalidRequestError),
    (404, NotFoundError),
    (409, IdempotencyConflictError),
    (410, IdempotencyGoneError),
    (413, TooLargeError),
    (416, RangeNotSatisfiableError),
    (500, ServerError),
]


@pytest.mark.parametrize("status,exc_type", CASES)
@respx.mock
def test_error_mapping(status, exc_type):
    respx.get(f"{BASE}/files/abc/metadata").respond(
        status, json={"error": "boom"}, headers={"Retry-After": "7"}
    )
    cm = CairnMark(BASE, retries=0)
    with pytest.raises(exc_type) as exc:
        cm.get_metadata("abc")
    err = exc.value
    assert isinstance(err, APIError)
    assert err.status == status
    assert err.message == "boom"
    if status == 409:
        assert err.retry_after == 7.0


@respx.mock
def test_non_json_error_body():
    respx.get(f"{BASE}/files/abc/metadata").respond(404, text="plain not found")
    cm = CairnMark(BASE, retries=0)
    with pytest.raises(NotFoundError) as exc:
        cm.get_metadata("abc")
    assert exc.value.message == "plain not found"


@respx.mock
def test_retry_on_5xx(file_json):
    route = respx.get(f"{BASE}/files/abc/metadata").mock(
        side_effect=[
            httpx.Response(500, json={"error": "transient"}),
            httpx.Response(200, json=file_json()),
        ]
    )
    cm = CairnMark(BASE)
    assert cm.get_metadata("abc").id == "abc"
    assert route.call_count == 2


@respx.mock
def test_default_headers_sent(file_json):
    route = respx.get(f"{BASE}/files/abc/metadata").respond(200, json=file_json())
    cm = CairnMark(BASE, headers={"Authorization": "Bearer tok"})
    cm.get_metadata("abc")
    req = route.calls.last.request
    assert req.headers["Authorization"] == "Bearer tok"
    assert req.headers["User-Agent"].startswith("cairnmark-python/")

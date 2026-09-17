import httpx
import pytest
import respx

from cairnmark import CairnMark, InvalidRequestError

from conftest import BASE


@respx.mock
def test_list_sends_filter_params(file_json):
    route = respx.get(f"{BASE}/files").respond(200, json={"files": [], "limit": 10, "count": 0})
    cm = CairnMark(BASE)

    page = cm.list(
        content_type="text/plain", tags={"env": "demo"}, limit=10, cursor="cur1", entries="only"
    )
    assert page.files == [] and page.next_cursor is None
    params = route.calls.last.request.url.params
    assert params["content_type"] == "text/plain"
    assert params["tag.env"] == "demo"
    assert params["entries"] == "only"
    assert params["limit"] == "10"
    assert params["cursor"] == "cur1"


def test_list_rejects_bad_entries_scope():
    cm = CairnMark(BASE)
    with pytest.raises(ValueError):
        cm.list(entries="archives")


def paged_handler(file_json, n):
    """Serve n files two per page, exercising the exact-multiple case: the
    last full page still carries a cursor to one final empty page."""

    def handler(request):
        cursor = request.url.params.get("cursor")
        start = int(cursor.removeprefix("f")) if cursor else 0
        files = [file_json(id=f"f{i + 1}") for i in range(start, min(start + 2, n))]
        body = {"files": files, "limit": 2, "count": len(files)}
        if len(files) == 2:
            body["next_cursor"] = f"f{start + 2}"
        return httpx.Response(200, json=body)

    return handler


@respx.mock
def test_iter_files_paginates(file_json):
    respx.get(f"{BASE}/files").mock(side_effect=paged_handler(file_json, 4))
    cm = CairnMark(BASE)

    ids = [f.id for f in cm.iter_files(limit=2)]
    assert ids == ["f1", "f2", "f3", "f4"]


@respx.mock
def test_iter_files_is_lazy(file_json):
    route = respx.get(f"{BASE}/files").mock(side_effect=paged_handler(file_json, 100))
    cm = CairnMark(BASE)

    seen = 0
    for _ in cm.iter_files(limit=2):
        seen += 1
        if seen == 3:
            break
    assert route.call_count == 2  # breaking mid-page 2 must not fetch page 3


@respx.mock
def test_iter_files_raises_on_error():
    respx.get(f"{BASE}/files").respond(400, json={"error": "bad cursor"})
    cm = CairnMark(BASE, retries=0)
    with pytest.raises(InvalidRequestError):
        next(iter(cm.iter_files()))

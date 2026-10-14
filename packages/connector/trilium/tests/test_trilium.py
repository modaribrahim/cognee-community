"""Unit tests for the Trilium dlt connector.

Two layers, all runnable in CI without a Trilium server:

* DB-free tests for HTML flattening, note→row mapping (tree path, labels),
  which notes are skipped, the ETAPI client (via ``httpx.MockTransport``), and
  the generic document DataItem tagging (``source="trilium"``).
* dlt-pipeline tests (fake client, temp sqlite destination) covering the
  acceptance criteria: re-sync reflects edits, unchanged notes keep identical
  rows, and deleted notes drop out of the full-snapshot load (forget-on-delete).
"""

from types import SimpleNamespace
from uuid import NAMESPACE_OID, uuid5

import pytest

# The row → document-DataItem mapping is generic and owned by the ingestion
# layer (any document source uses it), not the connector.
from cognee.tasks.ingestion.resolve_dlt_sources import _build_document_data_item

from cognee_community_connector_trilium.trilium import (
    TRILIUM_SOURCE_NAME,
    _html_to_text,
    _is_readable,
    _is_transient,
    _labels,
    _note_to_row,
    _paths,
    _tidy,
)

# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


def _note(
    note_id,
    title="Untitled",
    *,
    parents=("root",),
    note_type="text",
    protected=False,
    labels=(),
    modified="2026-10-14 08:00:00.000Z",
):
    """A note shaped like ETAPI's ``Note`` schema."""
    return {
        "noteId": note_id,
        "title": title,
        "type": note_type,
        "isProtected": protected,
        "parentNoteIds": list(parents),
        "blobId": f"blob-{note_id}",
        "utcDateModified": modified,
        "attributes": [
            {"noteId": note_id, "type": "label", "name": name, "value": value, "position": n}
            for n, (name, value) in enumerate(labels)
        ],
    }


class FakeTriliumClient:
    """Stand-in for _TriliumClient backed by in-memory notes and contents."""

    def __init__(self, notes, contents):
        self._notes = notes
        self._contents = contents
        self.params = None

    def search(self, params):
        self.params = params
        return self._notes

    def get_content(self, note_id):
        return self._contents.get(note_id)


# ---------------------------------------------------------------------------
# HTML flattening
# ---------------------------------------------------------------------------


def test_html_to_text_strips_tags_and_keeps_text():
    assert _html_to_text("<p>Hello <strong>world</strong></p>") == "Hello world"


def test_html_to_text_keeps_block_boundaries():
    assert _html_to_text("<h2>Title</h2><p>one</p><p>two</p>").splitlines() == [
        "Title",
        "one",
        "two",
    ]


def test_html_to_text_breaks_on_list_items_and_br():
    assert _html_to_text("<ul><li>alpha</li><li>beta</li></ul>").splitlines() == [
        "alpha",
        "beta",
    ]
    assert _html_to_text("a<br>b") == "a\nb"


def test_html_to_text_separates_table_cells():
    html = "<table><tr><th>Env</th><th>URL</th></tr><tr><td>prod</td><td>p.io</td></tr></table>"
    assert _html_to_text(html).splitlines() == ["Env URL", "prod p.io"]


def test_html_to_text_decodes_entities_and_drops_scripts():
    html = "<style>p{}</style><p>Tom &amp; Jerry</p><script>evil()</script>"
    assert _html_to_text(html) == "Tom & Jerry"


def test_tidy_collapses_blank_runs_and_strips():
    assert _tidy("a\n\n\n\nb  ") == "a\n\nb"


# ---------------------------------------------------------------------------
# Note → row mapping
# ---------------------------------------------------------------------------


def test_note_to_row_keeps_only_identity_and_text():
    note = _note("n1", " Linux ")

    row = _note_to_row(note, "<p>kernel notes</p>", {"n1": note}, "http://trilium:8080")

    assert row == {
        "id": "n1",
        "url": "http://trilium:8080/#root/n1",
        "title": "Linux",
        "content": "kernel notes",
    }


def test_note_to_row_has_no_url_without_server():
    note = _note("n1")
    assert _note_to_row(note, "<p>x</p>", {"n1": note}, None)["url"] is None


def test_code_note_body_is_kept_verbatim():
    note = _note("n1", "deploy.sh", note_type="code")

    row = _note_to_row(note, "echo <b>hi</b>\n", {"n1": note}, None)

    assert row["content"] == "echo <b>hi</b>"


def test_empty_folder_note_has_empty_content():
    note = _note("n1", "Folder")
    assert _note_to_row(note, "<p></p>", {"n1": note}, None)["content"] == ""


def test_title_only_task_note_keeps_its_labels():
    # Trilium task notes are often just a title plus labels.
    note = _note("n1", "Buy milk", labels=[("task", ""), ("todoDate", "2021-12-24")])
    row = _note_to_row(note, "", {"n1": note}, None)
    assert row["content"] == "Labels: #task, #todoDate=2021-12-24"


def test_path_lists_ancestors_top_down():
    notes = {
        "root": _note("root", "root", parents=()),
        "a": _note("a", "Work"),
        "b": _note("b", "Projects", parents=("a",)),
        "c": _note("c", "Launch plan", parents=("b",)),
    }

    row = _note_to_row(notes["c"], "<p>ship it</p>", notes, None)

    assert row["content"] == "Path: Work / Projects\nship it"


def test_top_level_note_has_no_path_line():
    notes = {"root": _note("root", "root", parents=()), "a": _note("a", "Inbox")}
    assert _paths(notes["a"], notes) == []


def test_cloned_note_lists_every_path_sorted():
    notes = {
        "root": _note("root", "root", parents=()),
        "w": _note("w", "Work"),
        "h": _note("h", "Home"),
        "c": _note("c", "Bash scripting", parents=("w", "h")),
    }
    assert _paths(notes["c"], notes) == ["Home", "Work"]


def test_path_stops_at_the_synced_subtree():
    # With ancestor_note_id, parents above the subtree are not in the listing.
    notes = {
        "top": _note("top", "Tech", parents=("outside",)),
        "n": _note("n", "x", parents=("top",)),
    }
    assert _paths(notes["n"], notes) == ["Tech"]


def test_path_survives_a_parent_cycle():
    notes = {"a": _note("a", "A", parents=("b",)), "b": _note("b", "B", parents=("a",))}
    assert _paths(_note("n", parents=("a",)), notes) == ["B / A"]


def test_user_labels_reach_the_text():
    note = _note("n1", labels=[("tag", "shopping"), ("todoDate", "2021-12-24"), ("urgent", "")])

    row = _note_to_row(note, "<p>Buy milk</p>", {"n1": note}, None)

    assert row["content"] == "Labels: #tag=shopping, #todoDate=2021-12-24, #urgent\nBuy milk"


def test_builtin_namespaced_and_inherited_labels_are_skipped():
    note = _note(
        "n1",
        labels=[("iconClass", "bx"), ("sorted", ""), ("label:author", "promoted"), ("x", "1")],
    )
    note["attributes"].append(
        {"noteId": "parent", "type": "label", "name": "inherited", "value": "", "position": 9}
    )
    note["attributes"].append(
        {"noteId": "n1", "type": "relation", "name": "author", "value": "n2", "position": 10}
    )
    assert _labels(note) == ["#x=1"]


def test_row_ignores_volatile_metadata():
    # blobId / utcDateModified change on every edit; only the text may drive the hash.
    before = _note("n1", "Alpha")
    after = _note("n1", "Alpha", modified="2026-10-15 09:00:00.000Z")
    after["blobId"] = "other"

    assert _note_to_row(before, "<p>same</p>", {}, None) == _note_to_row(
        after, "<p>same</p>", {}, None
    )


@pytest.mark.parametrize(
    ("note", "readable"),
    [
        (_note("n1"), True),
        (_note("n1", note_type="code"), True),
        (_note("root", parents=()), False),
        (_note("n1", protected=True), False),
        (_note("n1", note_type="image"), False),
        (_note("n1", note_type="book"), False),
    ],
)
def test_is_readable(note, readable):
    assert _is_readable(note) is readable


# ---------------------------------------------------------------------------
# ETAPI client (httpx.MockTransport, no network)
# ---------------------------------------------------------------------------


def _mock_client(monkeypatch, handler):
    import httpx

    from cognee_community_connector_trilium import trilium

    monkeypatch.setattr(trilium.time, "sleep", lambda _s: None)
    client = trilium._TriliumClient("http://trilium:8080", "tok")
    client._client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://trilium:8080/etapi",
        headers=client._client.headers,
    )
    return client


def _status_error(status):
    import httpx

    request = httpx.Request("GET", "http://trilium:8080/etapi/notes")
    return httpx.HTTPStatusError("boom", request=request, response=httpx.Response(status))


def test_error_classification():
    import httpx

    assert _is_transient(_status_error(429)) is True
    assert _is_transient(_status_error(503)) is True
    assert _is_transient(httpx.ReadTimeout("t")) is True
    assert _is_transient(httpx.ConnectError("t")) is True
    assert _is_transient(_status_error(401)) is False
    assert _is_transient(_status_error(404)) is False
    assert _is_transient(ValueError()) is False


def test_client_sends_token_and_search_params(monkeypatch):
    import httpx

    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"results": [_note("n1")]})

    client = _mock_client(monkeypatch, handler)

    notes = client.search({"search": 'note.noteId != ""', "ancestorNoteId": "abc"})

    assert [n["noteId"] for n in notes] == ["n1"]
    assert seen[0].headers["Authorization"] == "tok"
    assert seen[0].url.path == "/etapi/notes"
    assert seen[0].url.params["search"] == 'note.noteId != ""'
    assert seen[0].url.params["ancestorNoteId"] == "abc"


def test_client_retries_server_errors_then_succeeds(monkeypatch):
    import httpx

    responses = iter(
        [
            httpx.Response(429, headers={"Retry-After": "1"}),
            httpx.Response(503),
            httpx.Response(200, json={"results": []}),
        ]
    )
    client = _mock_client(monkeypatch, lambda request: next(responses))

    assert client.search({"search": "x"}) == []


def test_client_auth_error_raises_without_retry(monkeypatch):
    import httpx

    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(401, json={"code": "NOT_AUTHENTICATED"})

    client = _mock_client(monkeypatch, handler)

    with pytest.raises(httpx.HTTPStatusError):
        client.search({"search": "x"})
    assert len(calls) == 1


@pytest.mark.parametrize("payload", [{"oops": 1}, [], {"results": None}])
def test_client_rejects_unexpected_search_payload(monkeypatch, payload):
    # A 200 that is not a result list must abort, never read as "zero notes".
    import httpx

    client = _mock_client(monkeypatch, lambda request: httpx.Response(200, json=payload))

    with pytest.raises(ValueError, match="results list"):
        client.search({"search": "x"})


def test_client_reads_content_and_maps_404_to_none(monkeypatch):
    import httpx

    def handler(request):
        if request.url.path == "/etapi/notes/gone/content":
            return httpx.Response(404, json={"code": "NOTE_NOT_FOUND"})
        return httpx.Response(200, text="<p>hi</p>")

    client = _mock_client(monkeypatch, handler)

    assert client.get_content("n1") == "<p>hi</p>"
    assert client.get_content("gone") is None


def test_client_content_server_error_raises(monkeypatch):
    import httpx

    client = _mock_client(monkeypatch, lambda request: httpx.Response(500))

    with pytest.raises(httpx.HTTPStatusError):
        client.get_content("n1")


# ---------------------------------------------------------------------------
# Source construction / document marker (DB-free)
# ---------------------------------------------------------------------------


def test_trilium_source_declares_document_marker():
    from cognee.tasks.ingestion.dlt_utils import document_source_tag

    from cognee_community_connector_trilium.trilium import trilium_source

    source = trilium_source(server_url="http://trilium:8080", token="t")
    assert TRILIUM_SOURCE_NAME == "trilium"
    assert document_source_tag(source) == "trilium"


def test_trilium_source_requires_settings(monkeypatch):
    from cognee_community_connector_trilium.trilium import trilium_source

    monkeypatch.delenv("TRILIUM_URL", raising=False)
    monkeypatch.delenv("TRILIUM_TOKEN", raising=False)
    with pytest.raises(ValueError, match="settings required"):
        trilium_source()


def test_build_document_data_item_tags_source():
    row = SimpleNamespace(
        row_data={
            "id": "n1",
            "url": "http://trilium:8080/#root/n1",
            "title": "Linux",
            "content": "Path: Tech\nkernel notes",
        },
        content_hash="abc123",
    )
    data_id = uuid5(NAMESPACE_OID, "n1")

    item = _build_document_data_item(row, data_id, "trilium")

    # source="trilium" (not "dlt") is what routes the note through normal cognify.
    assert item.external_metadata["source"] == "trilium"
    assert item.external_metadata["url"] == "http://trilium:8080/#root/n1"
    assert item.external_metadata["external_id"] == "n1"
    assert item.data.startswith("# Linux")
    assert "kernel notes" in item.data


# ---------------------------------------------------------------------------
# dlt pipeline: full-snapshot sync + forget-on-delete (needs dlt + cognee)
# ---------------------------------------------------------------------------


@pytest.fixture
def dlt_mod():
    return pytest.importorskip("dlt")


def _run_sync(dlt, tmp_path, notes, contents, name="trilium_test", **kwargs):
    from cognee_community_connector_trilium.trilium import trilium_source

    db_path = (tmp_path / f"{name}.db").as_posix()
    pipeline = dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.sqlalchemy(f"sqlite:///{db_path}"),
        dataset_name="trilium_ds",
        pipelines_dir=str(tmp_path / "state"),
    )
    client = FakeTriliumClient(notes, contents)
    pipeline.run(trilium_source(server_url="http://trilium:8080", client=client, **kwargs))
    return pipeline, client


def _read_notes(pipeline):
    """Return {id: row-dict} for the trilium_notes table (read positionally)."""
    with (
        pipeline.sql_client() as client,
        client.execute_query("SELECT id, title, content, url FROM trilium_notes") as cursor,
    ):
        rows = cursor.fetchall()
    return {row[0]: {"title": row[1], "content": row[2], "url": row[3]} for row in rows}


def _tree():
    notes = [
        _note("root", "root", parents=()),
        _note("a", "Work"),
        _note("b", "Launch plan", parents=("a",), labels=[("status", "draft"), ("owner", "Ana")]),
        _note("img", "diagram.png", parents=("a",), note_type="image"),
        _note("sec", "Secrets", protected=True),
    ]
    contents = {"a": "<p>work stuff</p>", "b": "<p>ship v1</p>", "img": "PNG", "sec": "x"}
    return notes, contents


def test_first_sync_loads_readable_notes(dlt_mod, tmp_path):
    notes, contents = _tree()

    pipeline, client = _run_sync(dlt_mod, tmp_path, notes, contents)

    rows = _read_notes(pipeline)
    assert set(rows) == {"a", "b"}
    assert rows["b"]["content"] == "Path: Work\nLabels: #owner=Ana\nship v1"
    assert rows["b"]["url"] == "http://trilium:8080/#root/b"
    assert client.params == {"search": 'note.noteId != ""'}


def test_scope_and_archived_options_reach_the_search(dlt_mod, tmp_path):
    _, client = _run_sync(dlt_mod, tmp_path, [], {}, ancestor_note_id="a", include_archived=True)
    assert client.params == {
        "search": 'note.noteId != ""',
        "ancestorNoteId": "a",
        "includeArchivedNotes": "true",
    }


def test_resync_only_changes_rows_for_edited_notes(dlt_mod, tmp_path):
    # cognee keys each document by a content hash of its row, so a note whose
    # row is byte-identical across syncs is not re-cognified.
    notes, contents = _tree()
    before = _read_notes(_run_sync(dlt_mod, tmp_path, notes, contents)[0])

    notes[1]["utcDateModified"] = "2026-10-15 09:00:00.000Z"
    notes[1]["blobId"] = "touched"
    contents["b"] = "<p>ship v2</p>"
    after = _read_notes(_run_sync(dlt_mod, tmp_path, notes, contents, name="trilium_edit")[0])

    assert after["a"] == before["a"]
    assert "ship v2" in after["b"]["content"]
    assert "ship v1" not in after["b"]["content"]


def test_deleted_note_is_removed_on_resync(dlt_mod, tmp_path):
    notes, contents = _tree()
    _run_sync(dlt_mod, tmp_path, notes, contents)

    without_b = [n for n in notes if n["noteId"] != "b"]
    pipeline, _ = _run_sync(dlt_mod, tmp_path, without_b, contents, name="trilium_deleted")

    assert set(_read_notes(pipeline)) == {"a"}


def test_note_deleted_mid_sync_and_empty_notes_are_left_out(dlt_mod, tmp_path):
    notes, contents = _tree()
    del contents["b"]  # listed, but 404 by the time its content is read
    notes.append(_note("empty", "Folder"))
    contents["empty"] = "<p></p>"

    pipeline, _ = _run_sync(dlt_mod, tmp_path, notes, contents, name="trilium_gone")

    assert set(_read_notes(pipeline)) == {"a"}

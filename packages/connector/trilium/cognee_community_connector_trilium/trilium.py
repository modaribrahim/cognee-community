"""DLT source for Trilium Notes (full-snapshot sync + forget-on-delete).

Notes are ingested as documents (``DOCUMENT_SOURCE_ATTR = "trilium"``), so they
go through cognify like Notion pages. Each run is a full snapshot under
``write_disposition="replace"``: deleted notes drop out of ETAPI search and
cognee's ``orphan_cleanup`` forgets them. Unchanged notes keep the same content
hash, so only edited notes are re-cognified.

ETAPI search has no paging and no modified-since filter, so every run lists the
whole tree (or subtree) in one call, then reads each note's content.
"""

import os
import time
from html.parser import HTMLParser
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

logger = get_logger("trilium_connector")

# dlt resource / staging-table name for Trilium notes.
TRILIUM_TABLE_NAME = "trilium_notes"
TRILIUM_SOURCE_NAME = "trilium"

# ETAPI requires a search string; this one matches every note.
_ALL_NOTES_QUERY = 'note.noteId != ""'
# Text notes hold HTML, code notes plain text. Other types (images, files,
# canvases, collections) have no readable body.
_TEXT_TYPES = frozenset({"text", "code"})
_ROOT_NOTE_ID = "root"
_MAX_RETRIES = 5

_EXTRA_HINT = 'The Trilium connector requires httpx: pip install "httpx>=0.27,<1"'

# Labels Trilium itself gives a meaning to (icons, sorting, sharing, scripts...),
# from packages/commons/src/lib/builtin_attributes.ts in Trilium v0.106.0.
# Namespaced labels (``calendar:view``, ``label:author`` definitions...) are
# skipped by their colon. Only the user's own labels reach the note text.
_BUILTIN_LABELS = frozenset(
    """
    aiQuickAction ancestorDepth appCss appTheme appThemeBase archived autoReadOnlyDisabled
    baseSize beta bookmarkFolder bottom builtinWidget calendarRoot clipType clipperInbox
    collection color command cssClass customFont customRequestHandler customResourceProvider
    dateNote datePattern debug desktopOnly disableInclusion disableVersioning displayMode
    displayRelations docName docUrl electronOnly enableQuarterNote enableWeekNote endDate
    endTime excludeFromExport excludeFromNoteMap executeButton executeDescription executeTitle
    expanded fastSearch fullContentWidth geoShape geolocation growthFactor hideChildrenOverview
    hideHighlightWidget hidePromotedAttributes hideRelations iconClass iconPack inbox
    includeArchived includeArchivedNotes indentWithTabs internalBookmark isCompleted isHidden
    keepCurrentHoisting keyboardShortcut language launcherType limit llmChatHome
    mapExcludeRelation mapIncludeRelation mapRootNoteId mapType markdownSymbol maxNestingDepth
    mediaNotesPlayMode monthNote monthPattern newNotesOnTop oneNoteImportFailed oneNotePageId
    oneNoteSectionId orderBy orderDirection originalFileName pageSize pageUrl printLandscape
    printMargins printPageSize printScale quarterNote quarterPattern readOnly recurrence run
    runAtHour runOnInstance scriptInLauncherContent searchHome searchString sentFromSender
    serverOnly shareAlias shareCredentials shareDescription shareDisallowRobotIndexing
    shareExternal shareExternalLink shareHiddenFromTree shareHtmlLocation shareIndex
    shareLogoHeight shareLogoWidth shareOmitDefaultCss shareOpenGraphColor shareOpenGraphDomain
    shareOpenGraphImage shareOpenGraphURL shareRaw shareRoot shareRootLink
    similarNotesWidgetDisabled snippet snippetDescription sortDirection sortFoldersFirst
    sortLocale sortNatural sorted sqlConsoleHome startDate startTime stateId status
    subtreeHidden tabWidth template textSnippet textSnippetDescription titleTemplate toc top
    versioningLimit viewType webViewSrc weekNote weekPattern widget workspace
    workspaceCalendarRoot workspaceIconClass workspaceInbox workspaceLlmChatHome
    workspaceSearchHome workspaceTabBackgroundColor workspaceTemplate wrapLines yearNote
    yearPattern
    """.split()  # noqa: SIM905 - one name per line would be 158 lines
)

# Tags that imply a line break when flattening note HTML to text.
_BLOCK_TAGS = frozenset(
    {
        "blockquote",
        "div",
        "figcaption",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "li",
        "p",
        "pre",
        "section",
        "tr",
    }
)
# Never surface script/style bodies as note content.
_SKIPPED_TAGS = frozenset({"script", "style"})
# Table cells are inline in HTML; separate them so adjacent cells don't fuse.
_CELL_TAGS = frozenset({"td", "th"})


def trilium_source(
    server_url: str | None = None,
    token: str | None = None,
    *,
    ancestor_note_id: str | None = None,
    include_archived: bool = False,
    client: Any = None,
):
    """Create a dlt source that yields Trilium notes as text documents.

    Args:
        server_url: Trilium server address, e.g. ``http://localhost:8080``.
            Falls back to ``TRILIUM_URL``.
        token: ETAPI token (Options → ETAPI). Falls back to ``TRILIUM_TOKEN``.
        ancestor_note_id: Only ingest this note and its subtree. Omit to ingest
            the whole tree.
        include_archived: Also ingest archived notes (hidden from search by
            default).
        client: Pre-built client exposing ``search(params)`` and
            ``get_content(note_id)`` (mainly a test-injection point); when
            omitted an HTTP client is built from the settings above.

    Returns:
        A dlt source suitable for ``cognee.add(...)`` / ``cognee.remember(...)``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc

    server_url = (server_url or os.environ.get("TRILIUM_URL") or "").rstrip("/") or None
    if client is None:
        resolved_token = token or os.environ.get("TRILIUM_TOKEN")
        if not server_url or not resolved_token:
            raise ValueError(
                "Trilium settings required: pass server_url= and token=, or set TRILIUM_URL "
                "and TRILIUM_TOKEN."
            )
        client = _TriliumClient(server_url, resolved_token)

    params: dict[str, Any] = {"search": _ALL_NOTES_QUERY}
    if ancestor_note_id:
        params["ancestorNoteId"] = ancestor_note_id
    if include_archived:
        params["includeArchivedNotes"] = "true"

    @dlt.resource(name=TRILIUM_TABLE_NAME, primary_key="id", write_disposition="replace")
    def trilium_notes():
        # Fetch errors are not swallowed: under replace, a partial snapshot
        # would forget live notes. Aborting leaves memory untouched.
        notes = {note["noteId"]: note for note in client.search(params)}
        count = 0
        for note in notes.values():
            if not _is_readable(note):
                continue
            body = client.get_content(note["noteId"])
            if body is None:
                # Deleted between listing and reading: leave it out.
                continue
            row = _note_to_row(note, body, notes, server_url)
            if row["content"]:
                count += 1
                yield row
        logger.info("Trilium: synced %d note(s).", count)

    @dlt.source(name=TRILIUM_SOURCE_NAME)
    def _trilium():
        return trilium_notes

    source = _trilium()
    # Route notes through cognify as documents, not dlt rows.
    setattr(source, DOCUMENT_SOURCE_ATTR, TRILIUM_SOURCE_NAME)
    return source


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


class _TriliumClient:
    """Minimal ETAPI client: token auth, search and note content."""

    def __init__(self, server_url: str, token: str, timeout: float = 30.0):
        import httpx

        self._client = httpx.Client(
            base_url=f"{server_url}/etapi", headers={"Authorization": token}, timeout=timeout
        )

    def search(self, params: dict) -> list[dict]:
        """Return every note matching the search params."""
        body = _request(self._get, "/notes", params=params).json()
        notes = body.get("results") if isinstance(body, dict) else None
        # Never read an unexpected body as "no notes": that would forget everything.
        if not isinstance(notes, list):
            raise ValueError("Trilium: expected a search response with a results list")
        return notes

    def get_content(self, note_id: str) -> str | None:
        """Return a note's content, or ``None`` if the note no longer exists."""
        import httpx

        try:
            return _request(self._get, f"/notes/{note_id}/content").text
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise

    def _get(self, url: str, **kwargs):
        # httpx does not raise on 4xx/5xx; raise so _request can retry or abort.
        response = self._client.get(url, **kwargs)
        response.raise_for_status()
        return response

    def close(self) -> None:
        self._client.close()


def _request(method, *args, **kwargs):
    """Call ETAPI, retrying 429/5xx/network errors (honours ``Retry-After``)."""
    for attempt in range(_MAX_RETRIES):
        try:
            return method(*args, **kwargs)
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1 or not _is_transient(exc):
                raise
            delay = _retry_after(getattr(exc, "response", None), attempt)
            logger.warning(
                "Trilium: %s — retrying in %.1fs (%d/%d).", exc, delay, attempt + 1, _MAX_RETRIES
            )
            time.sleep(delay)


def _is_transient(exc: Exception) -> bool:
    """True for rate-limit / server / timeout / network errors worth retrying."""
    import httpx

    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return True
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status == 429 or (status is not None and 500 <= status < 600)


def _retry_after(response, attempt: int) -> float:
    """Seconds to wait before retrying: the Retry-After header, else backoff."""
    headers = getattr(response, "headers", None) or {}
    header = headers.get("retry-after") or headers.get("Retry-After")
    try:
        return float(header)
    except (TypeError, ValueError):
        return float(2**attempt)


# ---------------------------------------------------------------------------
# Note mapping
# ---------------------------------------------------------------------------


def _is_readable(note: dict) -> bool:
    """Text and code notes, minus the root and protected (encrypted) notes."""
    return (
        note.get("type") in _TEXT_TYPES
        and note["noteId"] != _ROOT_NOTE_ID
        and not note.get("isProtected")
    )


def _note_to_row(note: dict, body: str, notes: dict[str, dict], server_url: str | None) -> dict:
    """Flatten a note into a document row.

    No volatile fields (``utcDateModified``, ``blobId``), so the content hash
    only changes when the text, labels or place in the tree do.
    """
    text = _html_to_text(body) if note.get("type") == "text" else body.strip()
    labels = _labels(note)
    lines = [f"Path: {path}" for path in _paths(note, notes)]
    if labels:
        lines.append("Labels: " + ", ".join(labels))
    if text:
        lines.append(text)
    note_id = note["noteId"]
    return {
        "id": note_id,
        "url": f"{server_url}/#root/{note_id}" if server_url else None,
        "title": (note.get("title") or "").strip(),
        # A task note can be just a title and labels; a bare folder is skipped.
        "content": "\n".join(lines) if text or labels else "",
    }


def _paths(note: dict, notes: dict[str, dict]) -> list[str]:
    """Where the note sits in the tree, one path per parent (clones have several)."""
    paths = {_ancestry(parent_id, notes) for parent_id in note.get("parentNoteIds") or []}
    return sorted(path for path in paths if path)


def _ancestry(note_id: str, notes: dict[str, dict]) -> str:
    """Titles from the top of the synced tree down to ``note_id``."""
    titles: list[str] = []
    seen: set[str] = set()
    while note_id in notes and note_id != _ROOT_NOTE_ID and note_id not in seen:
        seen.add(note_id)
        note = notes[note_id]
        titles.append(note.get("title") or "")
        # Sorted so a cloned ancestor always resolves the same way.
        parents = sorted(p for p in note.get("parentNoteIds") or [] if p in notes)
        note_id = parents[0] if parents else _ROOT_NOTE_ID
    return " / ".join(reversed(titles))


def _labels(note: dict) -> list[str]:
    """The note's own user labels as ``#name`` / ``#name=value``.

    Inherited labels are left out, so editing a parent does not re-cognify its
    whole subtree.
    """
    labels = []
    for attr in sorted(note.get("attributes") or [], key=lambda a: a.get("position", 0)):
        name = attr.get("name") or ""
        if (
            attr.get("type") != "label"
            or attr.get("noteId") != note["noteId"]
            or name in _BUILTIN_LABELS
            or ":" in name
        ):
            continue
        value = attr.get("value")
        labels.append(f"#{name}={value}" if value else f"#{name}")
    return labels


def _html_to_text(content: str) -> str:
    """Flatten a text note's HTML body to plain text."""
    extractor = _TextExtractor()
    extractor.feed(content)
    extractor.close()
    return _tidy(extractor.text())


def _tidy(text: str) -> str:
    """Collapse runs of blank lines and strip trailing spaces per line."""
    lines = [line.strip() for line in text.splitlines()]
    out: list[str] = []
    for line in lines:
        if not line and (not out or not out[-1]):
            continue
        out.append(line)
    return "\n".join(out).strip()


class _TextExtractor(HTMLParser):
    """Collect the readable text of a note body, keeping block boundaries."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in _SKIPPED_TAGS:
            self._skip_depth += 1
        elif not self._skip_depth and tag == "br":
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in _SKIPPED_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
        elif not self._skip_depth and tag in _BLOCK_TAGS:
            self._parts.append("\n")
        elif not self._skip_depth and tag in _CELL_TAGS:
            self._parts.append(" ")

    def handle_data(self, data):
        if not self._skip_depth:
            self._parts.append(data)

    def text(self) -> str:
        return "".join(self._parts)

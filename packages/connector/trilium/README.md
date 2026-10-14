# Trilium Notes connector for cognee

Turn your [Trilium Notes](https://github.com/TriliumNext/Trilium) tree into memory
your AI agents can query, with forget-on-delete.

Each text or code note becomes one document in cognee and goes through the normal
cognify pipeline, so agents can answer questions *across* notes, not just look one
up. The note's place in the tree and its labels travel with the text:

```
Path: Work / Projects
Labels: #owner=Ana, #priority=high
...note text...
```

That matters in Trilium, where the same title ("Ideas", "TODO", "Meeting notes")
shows up under many parents.

## Setup

1. Install the connector and its dependencies:

   ```bash
   pip install -e .
   ```

2. Create an ETAPI token in Trilium: **Options → ETAPI → Create new ETAPI token**.

3. Export the settings:

   ```bash
   export TRILIUM_URL="http://localhost:8080"
   export TRILIUM_TOKEN="..."
   ```

4. Run the example, with your LLM key available:

   ```bash
   export LLM_API_KEY="sk-..."
   python examples/example.py
   ```

## Usage

```python
import cognee
from cognee_community_connector_trilium import trilium_source

# The whole tree.
source = trilium_source()

# Or one subtree (the note itself and everything under it).
source = trilium_source(ancestor_note_id="abc123XYZ")

await cognee.remember(source, dataset_name="trilium")
```

`trilium_source(server_url=..., token=...)` takes explicit settings; both fall back
to `TRILIUM_URL` / `TRILIUM_TOKEN`. Pass `include_archived=True` to also ingest
archived notes, which Trilium hides from search by default.

## What gets ingested

- **Text notes** (HTML turned into plain text) and **code notes** (as-is).
- Images, files, canvases, collections and other note types have no readable text
  and are skipped. A note with no text is kept if it has labels (task notes are
  often just a title and labels) and skipped otherwise (plain folders).
- **Protected notes are never read.** They are encrypted, and ETAPI can't open them.
- **Labels:** only your own labels, like `#tag=shopping`. Trilium's built-in labels
  (`#iconClass`, `#sorted`, `#template`...) and promoted-attribute definitions
  (`#label:author`...) are left out, so changing an icon doesn't re-process a note.
  Inherited labels are left out too, so editing a parent doesn't re-process its
  whole subtree.
- **Clones:** a note that lives in several places is ingested once, with one
  `Path:` line per place.
- The document URL is `<server>/#root/<noteId>`, which opens the note in Trilium.

## How syncing behaves

Each run is a **full snapshot** of the selected tree:

- **Incremental by content:** cognee keys each note by a hash of its text, so only
  notes whose text, labels or place in the tree changed are re-cognified.
- **Deletes propagate:** a deleted note drops out of the snapshot and cognee's
  orphan cleanup forgets it from the graph and vector stores.
- ETAPI search has no paging and no "modified since" filter, so each run lists
  the whole tree in one call and reads each note's content.

If the listing or a note's content fails (after retries on rate limits and 5xx),
the whole run aborts and memory is left untouched, rather than forgetting notes
that were just not fetched. A note deleted between the listing and the read is
simply left out.

## Privacy

This reads every unprotected note the token can see. It is opt-in: nothing is
fetched until you call it. Scope with `ancestor_note_id`, and use a dedicated
dataset so you can wipe it with a single `cognee.prune`.

## Tests

```bash
pytest tests/
```

The suite runs without a Trilium server: HTTP goes through `httpx.MockTransport`
or a fake client, and the dlt pipeline tests write to a temporary SQLite database.

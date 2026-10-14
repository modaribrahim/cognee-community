"""Trilium connector demo — give your AI agents your notes.

Pull Trilium notes into cognee, with forget-on-delete. ``trilium_source``
returns a ``dlt`` source you hand straight to ``cognee.remember``. Notes are
ingested as normal documents, so they go through the full cognify pipeline.

Each run is a full snapshot: unchanged notes are not re-cognified, and notes
you delete in Trilium drop out of the snapshot, so cognee's orphan cleanup
forgets them from memory on the next sync.

────────────────────────────────────────────────────────────────────────────
Privacy / opt-in
────────────────────────────────────────────────────────────────────────────
This reads every unprotected note the token can see. Nothing is fetched until
you run this script. Protected notes are never read. Scope what you ingest with
``ancestor_note_id``, and use a dedicated dataset so you can wipe it with a
single ``cognee.prune``.

────────────────────────────────────────────────────────────────────────────
One-time setup
────────────────────────────────────────────────────────────────────────────
1. Install this connector's dependencies:

       pip install -e .

2. Create an ETAPI token in Trilium (Options → ETAPI → Create new ETAPI token).
3. Export the settings and your LLM key, then run:

       export TRILIUM_URL="http://localhost:8080"
       export TRILIUM_TOKEN="..."
       export LLM_API_KEY="sk-..."
       python examples/example.py

Re-run after editing or deleting a note in Trilium to see the re-sync and
forget-on-delete.
"""

import asyncio
import os

import cognee

from cognee_community_connector_trilium import trilium_source

# Keep Trilium in its own dataset so it is easy to inspect and forget.
DATASET_NAME = "trilium"


async def main() -> None:
    if not (os.environ.get("TRILIUM_URL") and os.environ.get("TRILIUM_TOKEN")):
        print("Set TRILIUM_URL and TRILIUM_TOKEN (plus LLM_API_KEY) to run this example.")
        return

    # Scope with ancestor_note_id="..." to sync one subtree only.
    source = trilium_source()

    print("Syncing Trilium notes into cognee ...")
    await cognee.remember(source, dataset_name=DATASET_NAME)

    answer = await cognee.search(
        query_text="Summarize what these notes are about.",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("\nSearch result:\n", answer)

    print(
        "\nEdit or delete a note in Trilium, then re-run: edits re-sync and "
        "deleted notes are reconciled out of memory."
    )


if __name__ == "__main__":
    asyncio.run(main())

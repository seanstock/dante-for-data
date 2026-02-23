"""Unified search combining keyword matching and embedding similarity.

This is the main search entry point: given a natural-language query, it
retrieves relevant knowledge from both the keyword layer (fast substring
matching) and the embedding layer (vector similarity). Results are merged,
deduplicated, and returned as a ranked list.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from dante.config import knowledge_dir
from dante.knowledge import keywords as kw_module
from dante.knowledge import embeddings as emb_module
from dante.knowledge import vectorize

logger = logging.getLogger(__name__)


def _db_path(root: Path | None = None) -> Path:
    return knowledge_dir(root) / "embeddings.db"


async def search_async(
    query: str,
    top_k: int = 5,
    threshold: float = 0.3,
    root: Path | None = None,
) -> list[dict]:
    """Search keywords and embeddings, merge results.

    Returns a list of dicts, each with keys:
        question, sql, source, dashboard, similarity, keyword_match, description
    """
    results: list[dict] = []
    seen_ids: set[str] = set()

    # --- 1. Keyword matches (always fast, no API call) ---
    kw_matches = kw_module.match(query, root)
    for m in kw_matches:
        entry = {
            "question": "",
            "sql": "",
            "source": "keyword",
            "dashboard": "",
            "description": m["content"],
            "similarity": 1.0,  # exact keyword match
            "keyword_match": m["keyword"],
        }
        entry_id = f"kw:{m['keyword']}"
        if entry_id not in seen_ids:
            seen_ids.add(entry_id)
            results.append(entry)

    # --- 2. Embedding similarity search ---
    db_path = _db_path(root)
    if db_path.exists():
        try:
            query_vec = await vectorize.generate_embedding(query)
            conn = emb_module.init_db(db_path)
            try:
                emb_results = emb_module.search(conn, query_vec, top_k=top_k, threshold=threshold)
                for r in emb_results:
                    entry_id = f"emb:{r['id']}"
                    if entry_id not in seen_ids:
                        seen_ids.add(entry_id)
                        results.append({
                            "question": r.get("question", ""),
                            "sql": r.get("sql", ""),
                            "source": r.get("source", ""),
                            "dashboard": r.get("dashboard", ""),
                            "description": r.get("description", ""),
                            "similarity": r.get("similarity", 0.0),
                            "keyword_match": None,
                        })
            finally:
                conn.close()
        except RuntimeError as e:
            # OPENAI_API_KEY not set -- skip embedding search silently
            logger.debug("Embedding search skipped: %s", e)
        except Exception as e:
            logger.warning("Embedding search failed: %s", e)

    # --- 3. Sort by similarity descending ---
    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:top_k]


def search(
    query: str,
    top_k: int = 5,
    threshold: float = 0.3,
    root: Path | None = None,
) -> list[dict]:
    """Synchronous wrapper around search_async.

    If an event loop is already running (e.g. inside an async MCP handler),
    the caller should use search_async directly.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # We're inside an async context -- create a new task
        # This shouldn't normally happen when called from sync code,
        # but handle it gracefully.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(
                asyncio.run,
                search_async(query, top_k=top_k, threshold=threshold, root=root),
            )
            return future.result()
    else:
        return asyncio.run(
            search_async(query, top_k=top_k, threshold=threshold, root=root)
        )

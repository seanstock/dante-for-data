"""SQLite-based embedding storage with cosine similarity search.

Stores embeddings as JSON arrays in a standard SQLite database and
performs cosine similarity in Python with numpy. This avoids a hard
dependency on sqlite-vec while still supporting the full workflow.

If sqlite-vec becomes available later, we can add a vec0 virtual table
path as an optimization.

Schema:
    CREATE TABLE embeddings (
        id          TEXT PRIMARY KEY,
        question    TEXT NOT NULL,
        sql         TEXT NOT NULL DEFAULT '',
        source      TEXT NOT NULL DEFAULT 'manual',
        dashboard   TEXT NOT NULL DEFAULT '',
        description TEXT NOT NULL DEFAULT '',
        embedding   TEXT NOT NULL,          -- JSON array of floats
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    );
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(db_path: Path) -> sqlite3.Connection:
    """Open (or create) the embeddings database and ensure the schema exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS embeddings (
            id          TEXT PRIMARY KEY,
            question    TEXT NOT NULL,
            sql         TEXT NOT NULL DEFAULT '',
            source      TEXT NOT NULL DEFAULT 'manual',
            dashboard   TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            embedding   TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def upsert(
    conn: sqlite3.Connection,
    id: str,
    question: str,
    sql: str = "",
    source: str = "manual",
    dashboard: str = "",
    description: str = "",
    embedding_vector: list[float] | None = None,
) -> None:
    """Insert or update an embedding row.

    *embedding_vector* is a list of floats that gets stored as a JSON array.
    """
    now = _now_iso()
    embedding_json = json.dumps(embedding_vector) if embedding_vector else "[]"

    conn.execute(
        """
        INSERT INTO embeddings (id, question, sql, source, dashboard, description,
                                embedding, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            question    = excluded.question,
            sql         = excluded.sql,
            source      = excluded.source,
            dashboard   = excluded.dashboard,
            description = excluded.description,
            embedding   = excluded.embedding,
            updated_at  = excluded.updated_at
        """,
        (id, question, sql, source, dashboard, description, embedding_json, now, now),
    )
    conn.commit()


def delete(conn: sqlite3.Connection, id: str) -> bool:
    """Delete an embedding by id. Returns True if it existed."""
    cursor = conn.execute("DELETE FROM embeddings WHERE id = ?", (id,))
    conn.commit()
    return cursor.rowcount > 0


def get(conn: sqlite3.Connection, id: str) -> dict | None:
    """Fetch a single embedding row by id."""
    row = conn.execute("SELECT * FROM embeddings WHERE id = ?", (id,)).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def search(
    conn: sqlite3.Connection,
    embedding_vector: list[float],
    top_k: int = 5,
    threshold: float = 0.3,
) -> list[dict]:
    """Find the most similar embeddings using cosine similarity.

    Computes similarity in Python (brute-force). Returns up to *top_k*
    results with similarity >= *threshold*, sorted by descending similarity.
    """
    rows = conn.execute(
        "SELECT id, question, sql, source, dashboard, description, embedding, "
        "created_at, updated_at FROM embeddings"
    ).fetchall()

    if not rows:
        return []

    results = []
    for row in rows:
        stored_vec = json.loads(row["embedding"])
        if not stored_vec:
            continue
        sim = _cosine_similarity(embedding_vector, stored_vec)
        if sim >= threshold:
            d = _row_to_dict(row)
            d["similarity"] = round(sim, 4)
            results.append(d)

    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:top_k]


def stats(conn: sqlite3.Connection) -> dict:
    """Return embedding counts grouped by source, plus total and last update."""
    rows = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM embeddings GROUP BY source"
    ).fetchall()

    by_source = {row["source"]: row["cnt"] for row in rows}
    total = sum(by_source.values())

    last_row = conn.execute(
        "SELECT MAX(updated_at) as last FROM embeddings"
    ).fetchone()
    last_updated = last_row["last"] if last_row else None

    return {
        "by_source": by_source,
        "total": total,
        "last_updated": last_updated,
    }


def count(conn: sqlite3.Connection) -> int:
    """Return total number of embeddings."""
    row = conn.execute("SELECT COUNT(*) as cnt FROM embeddings").fetchone()
    return row["cnt"]


def list_all(conn: sqlite3.Connection) -> list[dict]:
    """Return all embedding rows (without the raw embedding vectors)."""
    rows = conn.execute(
        "SELECT id, question, sql, source, dashboard, description, "
        "created_at, updated_at FROM embeddings ORDER BY updated_at DESC"
    ).fetchall()
    return [dict(row) for row in rows]


def _row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a plain dict, excluding the raw embedding."""
    d = dict(row)
    d.pop("embedding", None)
    return d


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors.

    Pure-Python implementation (no numpy dependency required). For the
    typical text-embedding-3-large dimension of 3072, this is fast enough
    for databases with up to a few thousand rows.
    """
    if len(a) != len(b):
        return 0.0

    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))

    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)

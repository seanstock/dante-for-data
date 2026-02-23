"""Tests for dante.knowledge.embeddings — SQLite embedding storage."""

import math
import sqlite3
from pathlib import Path

import pytest

from dante.knowledge.embeddings import (
    init_db,
    upsert,
    get,
    delete,
    search,
    stats,
    count,
    list_all,
    _cosine_similarity,
)


@pytest.fixture
def db(tmp_path):
    """Return an open in-memory-ish SQLite connection for tests."""
    db_path = tmp_path / "embeddings.db"
    conn = init_db(db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# _cosine_similarity
# ---------------------------------------------------------------------------

def test_cosine_identical_vectors():
    v = [1.0, 0.0, 0.0]
    assert _cosine_similarity(v, v) == pytest.approx(1.0)


def test_cosine_orthogonal_vectors():
    a = [1.0, 0.0]
    b = [0.0, 1.0]
    assert _cosine_similarity(a, b) == pytest.approx(0.0)


def test_cosine_opposite_vectors():
    a = [1.0, 0.0]
    b = [-1.0, 0.0]
    assert _cosine_similarity(a, b) == pytest.approx(-1.0)


def test_cosine_mismatched_dims():
    assert _cosine_similarity([1.0, 2.0], [1.0]) == 0.0


def test_cosine_zero_vector():
    assert _cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0


def test_cosine_general():
    a = [3.0, 4.0]
    b = [4.0, 3.0]
    result = _cosine_similarity(a, b)
    expected = (3 * 4 + 4 * 3) / (5 * 5)
    assert result == pytest.approx(expected)


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------

def test_init_db_creates_table(tmp_path):
    db_path = tmp_path / "test.db"
    conn = init_db(db_path)
    # Verify table exists
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='embeddings'"
    ).fetchone()
    assert row is not None
    conn.close()


def test_init_db_idempotent(tmp_path):
    db_path = tmp_path / "test.db"
    conn1 = init_db(db_path)
    conn1.close()
    conn2 = init_db(db_path)  # Should not raise
    conn2.close()


def test_init_db_creates_parent_dirs(tmp_path):
    db_path = tmp_path / "nested" / "dir" / "embeddings.db"
    conn = init_db(db_path)
    assert db_path.exists()
    conn.close()


# ---------------------------------------------------------------------------
# upsert / get
# ---------------------------------------------------------------------------

def test_upsert_and_get(db):
    upsert(db, id="q1", question="Monthly revenue?", sql="SELECT 1",
           source="manual", embedding_vector=[1.0, 0.0, 0.0])
    row = get(db, "q1")
    assert row is not None
    assert row["question"] == "Monthly revenue?"
    assert row["sql"] == "SELECT 1"
    assert row["source"] == "manual"
    assert "embedding" not in row  # raw embedding stripped from dict


def test_get_nonexistent(db):
    assert get(db, "nonexistent") is None


def test_upsert_updates_on_conflict(db):
    upsert(db, id="q1", question="Old?", sql="SELECT 1", embedding_vector=[1.0, 0.0])
    upsert(db, id="q1", question="Updated?", sql="SELECT 2", embedding_vector=[1.0, 0.0])
    row = get(db, "q1")
    assert row["question"] == "Updated?"
    assert row["sql"] == "SELECT 2"
    # Should only have one row
    n = db.execute("SELECT COUNT(*) FROM embeddings WHERE id='q1'").fetchone()[0]
    assert n == 1


def test_upsert_without_embedding(db):
    upsert(db, id="bare", question="Q?", sql="SELECT 1")
    row = get(db, "bare")
    assert row is not None


def test_upsert_stores_timestamps(db):
    upsert(db, id="ts", question="Q?", sql="SELECT 1", embedding_vector=[1.0])
    row = db.execute("SELECT created_at, updated_at FROM embeddings WHERE id='ts'").fetchone()
    assert row["created_at"] is not None
    assert row["updated_at"] is not None


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

def test_delete_existing(db):
    upsert(db, id="del1", question="Q?", sql="SELECT 1", embedding_vector=[1.0])
    result = delete(db, "del1")
    assert result is True
    assert get(db, "del1") is None


def test_delete_nonexistent(db):
    result = delete(db, "ghost")
    assert result is False


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------

def test_count_empty(db):
    assert count(db) == 0


def test_count_after_inserts(db):
    upsert(db, id="a", question="Q1?", sql="SELECT 1", embedding_vector=[1.0])
    upsert(db, id="b", question="Q2?", sql="SELECT 2", embedding_vector=[0.0, 1.0])
    assert count(db) == 2


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

def test_search_empty_db(db):
    results = search(db, [1.0, 0.0, 0.0])
    assert results == []


def test_search_returns_similar(db):
    upsert(db, id="q1", question="Revenue?", sql="SELECT SUM(amount)",
           source="manual", embedding_vector=[1.0, 0.0, 0.0])
    upsert(db, id="q2", question="Churn?", sql="SELECT COUNT(*)",
           source="manual", embedding_vector=[0.0, 1.0, 0.0])

    # Query vector closest to q1
    results = search(db, [0.99, 0.01, 0.0], threshold=0.5)
    assert len(results) >= 1
    assert results[0]["id"] == "q1"


def test_search_threshold_filters(db):
    upsert(db, id="close", question="Close?", sql="SELECT 1",
           embedding_vector=[1.0, 0.0])
    upsert(db, id="far", question="Far?", sql="SELECT 2",
           embedding_vector=[0.0, 1.0])
    results = search(db, [1.0, 0.0], threshold=0.9)
    ids = [r["id"] for r in results]
    assert "close" in ids
    assert "far" not in ids


def test_search_top_k_limit(db):
    for i in range(10):
        upsert(db, id=f"q{i}", question=f"Q{i}?", sql=f"SELECT {i}",
               embedding_vector=[1.0, float(i) * 0.01])
    results = search(db, [1.0, 0.0], top_k=3, threshold=0.0)
    assert len(results) <= 3


def test_search_sorted_by_similarity(db):
    upsert(db, id="a", question="A?", sql="SELECT 1", embedding_vector=[1.0, 0.0])
    upsert(db, id="b", question="B?", sql="SELECT 2", embedding_vector=[0.8, 0.6])
    results = search(db, [1.0, 0.0], threshold=0.0)
    sims = [r["similarity"] for r in results]
    assert sims == sorted(sims, reverse=True)


def test_search_skips_empty_embedding(db):
    """Rows with empty embeddings should be skipped."""
    upsert(db, id="empty", question="Empty?", sql="SELECT 1")  # no embedding
    results = search(db, [1.0, 0.0], threshold=0.0)
    assert all(r["id"] != "empty" for r in results)


def test_search_similarity_in_result(db):
    upsert(db, id="q1", question="Q?", sql="SELECT 1", embedding_vector=[1.0, 0.0])
    results = search(db, [1.0, 0.0], threshold=0.5)
    assert "similarity" in results[0]
    assert isinstance(results[0]["similarity"], float)


# ---------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------

def test_stats_empty(db):
    s = stats(db)
    assert s["total"] == 0
    assert s["by_source"] == {}


def test_stats_by_source(db):
    upsert(db, id="a", question="Q1?", sql="SELECT 1",
           source="manual", embedding_vector=[1.0])
    upsert(db, id="b", question="Q2?", sql="SELECT 2",
           source="looker", embedding_vector=[1.0])
    upsert(db, id="c", question="Q3?", sql="SELECT 3",
           source="looker", embedding_vector=[1.0])
    s = stats(db)
    assert s["total"] == 3
    assert s["by_source"]["manual"] == 1
    assert s["by_source"]["looker"] == 2


def test_stats_last_updated(db):
    upsert(db, id="q1", question="Q?", sql="SELECT 1", embedding_vector=[1.0])
    s = stats(db)
    assert s["last_updated"] is not None


# ---------------------------------------------------------------------------
# list_all
# ---------------------------------------------------------------------------

def test_list_all_empty(db):
    assert list_all(db) == []


def test_list_all_returns_rows(db):
    upsert(db, id="a", question="Q1?", sql="SELECT 1", embedding_vector=[1.0])
    upsert(db, id="b", question="Q2?", sql="SELECT 2", embedding_vector=[1.0])
    rows = list_all(db)
    assert len(rows) == 2
    # No raw embedding in output
    for row in rows:
        assert "embedding" not in row

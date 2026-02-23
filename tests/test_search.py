"""Tests for dante.knowledge.search — unified keyword + embedding search."""

import pytest

from dante.knowledge import keywords as kw_module
from dante.knowledge.search import search


def test_search_empty_returns_empty(tmp_path):
    results = search("what is revenue?", root=tmp_path)
    assert results == []


def test_search_keyword_match(tmp_path):
    kw_module.add("revenue", "Revenue = SUM(amount) from orders.", root=tmp_path)
    results = search("show me revenue", root=tmp_path)
    assert len(results) >= 1
    kw_result = next((r for r in results if r["source"] == "keyword"), None)
    assert kw_result is not None
    assert kw_result["keyword_match"] == "revenue"
    assert kw_result["similarity"] == 1.0


def test_search_no_keyword_match(tmp_path):
    kw_module.add("churn", "Churn definition.", root=tmp_path)
    results = search("what is revenue?", root=tmp_path)
    assert results == []


def test_search_multiple_keyword_matches(tmp_path):
    kw_module.add("revenue", "Revenue definition.", root=tmp_path)
    kw_module.add("churn", "Churn definition.", root=tmp_path)
    results = search("show revenue and churn", root=tmp_path)
    sources = {r["keyword_match"] for r in results if r.get("keyword_match")}
    assert "revenue" in sources
    assert "churn" in sources


def test_search_top_k_limits_results(tmp_path):
    for i in range(10):
        kw_module.add(f"keyword{i}", f"Content {i}.", root=tmp_path)
    # Build a query that matches all keywords
    query = " ".join(f"keyword{i}" for i in range(10))
    results = search(query, top_k=3, root=tmp_path)
    assert len(results) <= 3


def test_search_no_embedding_db_still_works(tmp_path):
    """When there's no embeddings.db, keyword search still works."""
    kw_module.add("mrr", "Monthly Recurring Revenue.", root=tmp_path)
    results = search("what is mrr?", root=tmp_path)
    assert len(results) == 1


def test_search_embedding_skipped_without_api_key(tmp_path, monkeypatch):
    """Without OPENAI_API_KEY the embedding search is skipped gracefully."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    # Create a dummy embeddings DB
    from dante.knowledge.embeddings import init_db, upsert
    from dante.config import project_dir
    db_path = project_dir(tmp_path) / "knowledge" / "embeddings.db"
    conn = init_db(db_path)
    upsert(conn, id="q1", question="Revenue?", sql="SELECT 1",
           embedding_vector=[1.0, 0.0])
    conn.close()

    # Should not raise, just skip embedding search
    results = search("revenue", root=tmp_path)
    # Keyword results may be empty (no keywords added), but no exception
    assert isinstance(results, list)


def test_search_result_structure(tmp_path):
    kw_module.add("arr", "Annual Recurring Revenue.", root=tmp_path)
    results = search("show arr", root=tmp_path)
    assert len(results) == 1
    r = results[0]
    assert "question" in r
    assert "sql" in r
    assert "source" in r
    assert "dashboard" in r
    assert "description" in r
    assert "similarity" in r
    assert "keyword_match" in r

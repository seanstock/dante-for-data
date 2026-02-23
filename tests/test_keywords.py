"""Tests for dante.knowledge.keywords."""

from pathlib import Path

import pytest

from dante.knowledge.keywords import add, load, save, remove, list_keywords, match


def test_load_empty(tmp_path):
    assert load(tmp_path) == {}


def test_add_and_load(tmp_path):
    add("revenue", "Revenue = SUM(amount) from orders.", root=tmp_path)
    kw = load(tmp_path)
    assert kw["revenue"] == "Revenue = SUM(amount) from orders."


def test_add_updates_existing(tmp_path):
    add("churn", "Old content.", root=tmp_path)
    add("churn", "New content.", root=tmp_path)
    kw = load(tmp_path)
    assert kw["churn"] == "New content."
    assert len(kw) == 1


def test_save_and_load_roundtrip(tmp_path):
    data = {"mrr": "Monthly recurring revenue.", "arr": "Annual recurring revenue."}
    save(data, root=tmp_path)
    loaded = load(tmp_path)
    assert loaded == data


def test_save_overwrites(tmp_path):
    save({"old": "gone"}, root=tmp_path)
    save({"new": "here"}, root=tmp_path)
    loaded = load(tmp_path)
    assert "old" not in loaded
    assert "new" in loaded


def test_remove_existing(tmp_path):
    add("temp", "temporary", root=tmp_path)
    result = remove("temp", root=tmp_path)
    assert result is True
    assert "temp" not in load(tmp_path)


def test_remove_nonexistent(tmp_path):
    result = remove("ghost", root=tmp_path)
    assert result is False


def test_remove_preserves_others(tmp_path):
    add("keep", "keep me", root=tmp_path)
    add("drop", "drop me", root=tmp_path)
    remove("drop", root=tmp_path)
    kw = load(tmp_path)
    assert "keep" in kw
    assert "drop" not in kw


def test_list_keywords_empty(tmp_path):
    assert list_keywords(tmp_path) == []


def test_list_keywords_sorted(tmp_path):
    add("zebra", "z", root=tmp_path)
    add("alpha", "a", root=tmp_path)
    add("mango", "m", root=tmp_path)
    result = list_keywords(tmp_path)
    assert [r["keyword"] for r in result] == ["alpha", "mango", "zebra"]


def test_list_keywords_structure(tmp_path):
    add("cac", "Customer Acquisition Cost.", root=tmp_path)
    result = list_keywords(tmp_path)
    assert result[0] == {"keyword": "cac", "content": "Customer Acquisition Cost."}


# ---------------------------------------------------------------------------
# match()
# ---------------------------------------------------------------------------

def test_match_empty_index(tmp_path):
    assert match("what is revenue", root=tmp_path) == []


def test_match_exact(tmp_path):
    add("revenue", "Revenue definition.", root=tmp_path)
    results = match("revenue", root=tmp_path)
    assert len(results) == 1
    assert results[0]["keyword"] == "revenue"


def test_match_case_insensitive(tmp_path):
    add("revenue", "Revenue definition.", root=tmp_path)
    results = match("Show me Revenue numbers", root=tmp_path)
    assert len(results) == 1


def test_match_substring(tmp_path):
    add("churn", "Churn definition.", root=tmp_path)
    results = match("what is our monthly churn rate?", root=tmp_path)
    assert len(results) == 1
    assert results[0]["keyword"] == "churn"


def test_match_multiple_keywords(tmp_path):
    add("revenue", "Revenue definition.", root=tmp_path)
    add("churn", "Churn definition.", root=tmp_path)
    add("cac", "CAC definition.", root=tmp_path)
    results = match("show revenue and churn", root=tmp_path)
    keywords = {r["keyword"] for r in results}
    assert "revenue" in keywords
    assert "churn" in keywords
    assert "cac" not in keywords


def test_match_no_match(tmp_path):
    add("revenue", "Revenue definition.", root=tmp_path)
    results = match("what is cac?", root=tmp_path)
    assert results == []


def test_load_ignores_non_dict_file(tmp_path):
    """Malformed keywords.yaml returns empty dict."""
    path = tmp_path / ".dante" / "knowledge" / "keywords.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("- a list item\n")
    result = load(tmp_path)
    assert result == {}

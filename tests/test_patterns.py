"""Tests for dante.knowledge.patterns."""

from pathlib import Path

import pytest
import yaml

from dante.knowledge.patterns import (
    save_pattern,
    load_pattern,
    list_patterns,
    delete_pattern,
    get_pattern,
    _slugify,
    _parse_frontmatter,
)


# ---------------------------------------------------------------------------
# _slugify
# ---------------------------------------------------------------------------

def test_slugify_basic():
    assert _slugify("What is our monthly churn rate?") == "what-is-our-monthly-churn-rate"


def test_slugify_lowercase():
    assert _slugify("Revenue By Month") == "revenue-by-month"


def test_slugify_strips_punctuation():
    result = _slugify("What's the ARR?!")
    assert "?" not in result
    assert "!" not in result
    assert "'" not in result


def test_slugify_collapses_spaces():
    assert _slugify("a   b") == "a-b"


def test_slugify_truncates_long():
    long_question = "x" * 200
    assert len(_slugify(long_question)) <= 80


def test_slugify_unicode():
    # Non-ASCII chars get stripped via NFKD normalization
    result = _slugify("Café revenue")
    assert result  # something non-empty


# ---------------------------------------------------------------------------
# _parse_frontmatter
# ---------------------------------------------------------------------------

def test_parse_frontmatter_basic():
    content = "---\nquestion: What is churn?\ntables: [subscriptions]\n---\nSELECT 1\n"
    fm, sql = _parse_frontmatter(content)
    assert fm["question"] == "What is churn?"
    assert fm["tables"] == ["subscriptions"]
    assert sql == "SELECT 1"


def test_parse_frontmatter_no_frontmatter():
    content = "SELECT 1"
    fm, sql = _parse_frontmatter(content)
    assert fm == {}
    assert sql == "SELECT 1"


def test_parse_frontmatter_unclosed():
    content = "---\nquestion: test\nSELECT 1"
    fm, sql = _parse_frontmatter(content)
    assert fm == {}
    assert sql == content


# ---------------------------------------------------------------------------
# save_pattern / load_pattern
# ---------------------------------------------------------------------------

def test_save_and_load_pattern(tmp_path):
    path = save_pattern(
        question="What is monthly revenue?",
        sql="SELECT DATE_TRUNC('month', created_at), SUM(amount) FROM orders GROUP BY 1",
        tables=["orders"],
        description="Monthly revenue by month",
        root=tmp_path,
    )
    assert path.exists()
    assert path.suffix == ".sql"

    p = load_pattern(path)
    assert p["question"] == "What is monthly revenue?"
    assert "SUM(amount)" in p["sql"]
    assert p["tables"] == ["orders"]
    assert p["description"] == "Monthly revenue by month"
    assert p["source"] == "manual"
    assert p["filename"] == path.name


def test_save_pattern_creates_slug_filename(tmp_path):
    path = save_pattern(
        question="What is our churn rate?",
        sql="SELECT COUNT(*) FROM subscriptions WHERE canceled_at IS NOT NULL",
        root=tmp_path,
    )
    assert "what-is-our-churn-rate" in path.name


def test_save_pattern_default_source(tmp_path):
    path = save_pattern("Test question?", "SELECT 1", root=tmp_path)
    p = load_pattern(path)
    assert p["source"] == "manual"


def test_save_pattern_custom_source(tmp_path):
    path = save_pattern("Test?", "SELECT 1", source="looker", root=tmp_path)
    p = load_pattern(path)
    assert p["source"] == "looker"


def test_save_pattern_overwrites(tmp_path):
    save_pattern("Same question?", "SELECT 1", root=tmp_path)
    path = save_pattern("Same question?", "SELECT 2", root=tmp_path)
    p = load_pattern(path)
    assert "SELECT 2" in p["sql"]


def test_save_pattern_no_tables(tmp_path):
    path = save_pattern("Bare question?", "SELECT 1", root=tmp_path)
    p = load_pattern(path)
    assert p["tables"] == []


def test_save_pattern_created_date(tmp_path):
    from datetime import date
    path = save_pattern("Date test?", "SELECT 1", root=tmp_path)
    p = load_pattern(path)
    assert p["created"] == date.today().isoformat()


# ---------------------------------------------------------------------------
# list_patterns
# ---------------------------------------------------------------------------

def test_list_patterns_empty(tmp_path):
    assert list_patterns(tmp_path) == []


def test_list_patterns_returns_all(tmp_path):
    save_pattern("Question one?", "SELECT 1", root=tmp_path)
    save_pattern("Question two?", "SELECT 2", root=tmp_path)
    save_pattern("Question three?", "SELECT 3", root=tmp_path)
    results = list_patterns(tmp_path)
    assert len(results) == 3


def test_list_patterns_sorted_by_filename(tmp_path):
    save_pattern("Zebra question?", "SELECT 1", root=tmp_path)
    save_pattern("Apple question?", "SELECT 2", root=tmp_path)
    results = list_patterns(tmp_path)
    filenames = [r["filename"] for r in results]
    assert filenames == sorted(filenames)


# ---------------------------------------------------------------------------
# delete_pattern
# ---------------------------------------------------------------------------

def test_delete_existing(tmp_path):
    path = save_pattern("Delete me?", "SELECT 1", root=tmp_path)
    result = delete_pattern(path.name, root=tmp_path)
    assert result is True
    assert not path.exists()


def test_delete_nonexistent(tmp_path):
    result = delete_pattern("ghost.sql", root=tmp_path)
    assert result is False


def test_delete_does_not_affect_others(tmp_path):
    save_pattern("Keep me?", "SELECT 1", root=tmp_path)
    path2 = save_pattern("Delete me?", "SELECT 2", root=tmp_path)
    delete_pattern(path2.name, root=tmp_path)
    remaining = list_patterns(tmp_path)
    assert len(remaining) == 1
    assert remaining[0]["question"] == "Keep me?"


# ---------------------------------------------------------------------------
# get_pattern
# ---------------------------------------------------------------------------

def test_get_pattern_existing(tmp_path):
    save_pattern("Find me by question?", "SELECT 42", root=tmp_path)
    p = get_pattern("Find me by question?", root=tmp_path)
    assert p is not None
    assert p["sql"] == "SELECT 42"


def test_get_pattern_nonexistent(tmp_path):
    assert get_pattern("Does not exist?", root=tmp_path) is None

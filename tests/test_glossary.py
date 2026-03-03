"""Tests for dante.knowledge.glossary."""

from pathlib import Path

import pytest
import yaml

from dante.knowledge.glossary import define, load, save, undefine, list_terms


def test_load_empty(tmp_path):
    assert load(tmp_path) == {}


def test_define_and_load(tmp_path):
    define("ARR", "Annual Recurring Revenue. MRR * 12.", root=tmp_path)
    terms = load(tmp_path)
    assert terms["ARR"] == "Annual Recurring Revenue. MRR * 12."


def test_define_updates_existing(tmp_path):
    define("churn", "Old definition.", root=tmp_path)
    define("churn", "Updated definition.", root=tmp_path)
    terms = load(tmp_path)
    assert terms["churn"] == "Updated definition."
    assert len(terms) == 1


def test_define_multiple_terms(tmp_path):
    define("ARR", "Annual Recurring Revenue.", root=tmp_path)
    define("MRR", "Monthly Recurring Revenue.", root=tmp_path)
    define("churn", "Lost customers.", root=tmp_path)
    terms = load(tmp_path)
    assert len(terms) == 3
    assert "ARR" in terms
    assert "MRR" in terms
    assert "churn" in terms


def test_save_and_load_roundtrip(tmp_path):
    data = {"CAC": "Customer Acquisition Cost.", "LTV": "Lifetime Value."}
    save(data, root=tmp_path)
    loaded = load(tmp_path)
    assert loaded == data


def test_save_overwrites(tmp_path):
    save({"old": "value"}, root=tmp_path)
    save({"new": "value"}, root=tmp_path)
    loaded = load(tmp_path)
    assert "old" not in loaded
    assert loaded["new"] == "value"


def test_undefine_existing(tmp_path):
    define("temp", "temporary", root=tmp_path)
    result = undefine("temp", root=tmp_path)
    assert result is True
    assert "temp" not in load(tmp_path)


def test_undefine_nonexistent(tmp_path):
    result = undefine("ghost", root=tmp_path)
    assert result is False


def test_undefine_preserves_other_terms(tmp_path):
    define("keep", "keep me", root=tmp_path)
    define("remove", "remove me", root=tmp_path)
    undefine("remove", root=tmp_path)
    terms = load(tmp_path)
    assert "keep" in terms
    assert "remove" not in terms


def test_list_terms_empty(tmp_path):
    assert list_terms(tmp_path) == []


def test_list_terms_sorted(tmp_path):
    define("zebra", "last", root=tmp_path)
    define("apple", "first", root=tmp_path)
    define("mango", "middle", root=tmp_path)
    result = list_terms(tmp_path)
    assert [r["term"] for r in result] == ["apple", "mango", "zebra"]


def test_list_terms_structure(tmp_path):
    define("ARR", "Annual Recurring Revenue.", root=tmp_path)
    result = list_terms(tmp_path)
    assert len(result) == 1
    assert result[0]["term"] == "ARR"
    assert result[0]["definition"] == "Annual Recurring Revenue."


def test_terms_yaml_format(tmp_path):
    """Ensure the file is valid YAML after write."""
    define("NRR", "Net Revenue Retention.", root=tmp_path)
    path = tmp_path / ".dante" / "knowledge" / "terms.yaml"
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert isinstance(data, dict)
    assert data["NRR"] == "Net Revenue Retention."


def test_load_ignores_non_dict_file(tmp_path):
    """If terms.yaml contains a non-dict (malformed), load returns {}."""
    path = tmp_path / ".dante" / "knowledge" / "terms.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("- just a list\n- not a dict\n")
    result = load(tmp_path)
    assert result == {}

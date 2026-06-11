"""Tests for dante.ingest._common — shared ingestion utilities."""

import pytest

from dante.ingest._common import make_embedding_id, get_credentials


# ---------------------------------------------------------------------------
# make_embedding_id
# ---------------------------------------------------------------------------


def test_make_embedding_id_deterministic():
    id1 = make_embedding_id("looker", "lkr", "dash1", "elem1")
    id2 = make_embedding_id("looker", "lkr", "dash1", "elem1")
    assert id1 == id2


def test_make_embedding_id_uses_prefix():
    id1 = make_embedding_id("looker", "lkr", "d", "e")
    assert id1.startswith("lkr-")


def test_make_embedding_id_different_platforms_differ():
    id_lkr = make_embedding_id("looker", "lkr", "d", "e")
    id_dbr = make_embedding_id("databricks", "dbr", "d", "e")
    assert id_lkr != id_dbr


def test_make_embedding_id_different_elements_differ():
    id1 = make_embedding_id("looker", "lkr", "d", "e1")
    id2 = make_embedding_id("looker", "lkr", "d", "e2")
    assert id1 != id2


# ---------------------------------------------------------------------------
# get_credentials
# ---------------------------------------------------------------------------


def test_get_credentials_returns_none_when_missing(tmp_path, monkeypatch):
    """No credentials file → returns None."""
    result = get_credentials("looker", ["base_url"])
    assert result is None


def test_get_credentials_returns_none_when_incomplete(tmp_path, monkeypatch):
    """Credentials exist but missing required key → returns None."""
    from pathlib import Path
    import yaml

    fake_home = Path.home()
    creds_path = fake_home / ".dante" / "credentials.yaml"
    creds_path.parent.mkdir(parents=True, exist_ok=True)
    creds_path.write_text(
        yaml.dump({"looker": {"base_url": "https://example.com"}}),
        encoding="utf-8",
    )

    # Requires both base_url and client_id — client_id missing
    result = get_credentials("looker", ["base_url", "client_id"])
    assert result is None


def test_get_credentials_returns_dict_when_complete(tmp_path, monkeypatch):
    """All required keys present → returns credential dict."""
    from pathlib import Path
    import yaml

    fake_home = Path.home()
    creds_path = fake_home / ".dante" / "credentials.yaml"
    creds_path.parent.mkdir(parents=True, exist_ok=True)
    creds_path.write_text(
        yaml.dump({"databricks": {"workspace_url": "https://x", "token": "tok"}}),
        encoding="utf-8",
    )

    result = get_credentials("databricks", ["workspace_url", "token"])
    assert result is not None
    assert result["workspace_url"] == "https://x"
    assert result["token"] == "tok"


# ---------------------------------------------------------------------------
# embed_charts — error accumulation semantics
# ---------------------------------------------------------------------------


def _make_charts(n):
    return [
        {
            "dashboard_id": f"d{i}",
            "dashboard_title": "Dash",
            "element_id": f"e{i}",
            "element_title": f"Chart {i}",
            "sql": "SELECT 1",
        }
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_embed_charts_aborts_on_repeated_failures(tmp_path, monkeypatch):
    """A systemic failure (e.g. bad API key) should abort early, not retry N times."""
    from dante.ingest import IngestionConfig
    from dante.ingest import _common

    calls = {"n": 0}

    async def _boom(text):
        calls["n"] += 1
        raise RuntimeError("OPENAI_API_KEY invalid")

    async def _simplify(sql, title, enabled=True):
        return sql

    monkeypatch.setattr(_common, "_embed_text_fn", None, raising=False)
    monkeypatch.setattr("dante.knowledge.vectorize.generate_embedding", _boom)
    monkeypatch.setattr("dante.ingest.sql_simplifier.simplify_sql", _simplify)

    config = IngestionConfig(sources=["looker"])
    result = await _common.embed_charts(
        _make_charts(25),
        source="looker",
        config=config,
        make_id=lambda a, b: f"id-{a}-{b}",
    )

    # Should give up well before trying all 25 charts.
    assert calls["n"] < 25
    assert result.created == 0
    assert result.errors >= 1

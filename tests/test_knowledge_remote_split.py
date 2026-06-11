"""Tests for the local/remote split in the knowledge layer.

These exercise the config-driven path through dante.knowledge (not the
RemoteKnowledge client directly), which is where the real routing bugs lived:
remote-mode saves must not touch local files, reads must consult the remote,
and an explicit project-level ``remote.enabled: false`` must force local mode.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import dante.knowledge as knowledge
from dante.config import save_project_config


def _configure_remote(root):
    save_project_config(
        {"remote": {"api_url": "https://api.example.com", "api_key": "sk-test"}},
        root=root,
    )


# ── save_pattern in remote mode ───────────────────────────────────────────────


def test_save_pattern_remote_mode_does_not_crash_and_skips_local(tmp_path):
    _configure_remote(tmp_path)
    fake = MagicMock()
    fake.save_pattern.return_value = {"id": "p1", "question": "How many users?"}

    with patch("dante.remote._get_remote_client", return_value=fake):
        result = knowledge.save_pattern(
            "How many users?", "SELECT count(*) FROM users", root=tmp_path
        )

    fake.save_pattern.assert_called_once()
    assert isinstance(result, dict)
    # No local .sql file should have been written in remote mode.
    patterns_dir = tmp_path / ".dante" / "knowledge" / "patterns"
    assert not list(patterns_dir.glob("*.sql"))


@pytest.mark.asyncio
async def test_save_pattern_async_remote_mode(tmp_path):
    _configure_remote(tmp_path)
    fake = MagicMock()
    fake.save_pattern.return_value = {"id": "p1"}

    with patch("dante.remote._get_remote_client", return_value=fake):
        result = await knowledge.save_pattern_async(
            "How many users?", "SELECT 1", root=tmp_path
        )

    fake.save_pattern.assert_called_once()
    assert result is not None
    patterns_dir = tmp_path / ".dante" / "knowledge" / "patterns"
    assert not list(patterns_dir.glob("*.sql"))


# ── read paths in remote mode ─────────────────────────────────────────────────


def test_list_patterns_remote_mode_reads_remote(tmp_path):
    _configure_remote(tmp_path)
    fake = MagicMock()
    fake.list_patterns.return_value = [{"id": "p1"}, {"id": "p2"}]

    with patch("dante.remote._get_remote_client", return_value=fake):
        result = knowledge.list_patterns(root=tmp_path)

    fake.list_patterns.assert_called_once()
    assert result == [{"id": "p1"}, {"id": "p2"}]


def test_stats_remote_mode_reads_remote(tmp_path):
    _configure_remote(tmp_path)
    fake = MagicMock()
    fake.stats.return_value = {"pattern_count": 5, "keyword_count": 3}

    with patch("dante.remote._get_remote_client", return_value=fake):
        result = knowledge.stats(root=tmp_path)

    fake.stats.assert_called_once()
    assert result["pattern_count"] == 5


# ── enabled: false forces local mode ──────────────────────────────────────────


def test_enabled_false_in_project_forces_local(tmp_path, monkeypatch):
    """An explicit project-level remote.enabled:false must not fall through to global."""
    import yaml
    from pathlib import Path
    from dante.remote import _get_remote_client

    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home_dir))

    global_dante = home_dir / ".dante"
    global_dante.mkdir(parents=True, exist_ok=True)
    (global_dante / "config.yaml").write_text(
        yaml.dump({"remote": {"api_url": "https://g.example.com", "api_key": "gk"}})
    )

    project_root = tmp_path / "project"
    project_root.mkdir()
    save_project_config(
        {"remote": {"enabled": False, "api_url": "https://p.example.com",
                    "api_key": "pk"}},
        root=project_root,
    )

    assert _get_remote_client(root=project_root) is None


def test_bare_remote_key_does_not_crash(tmp_path):
    """A config with a bare `remote:` (YAML null) must not raise AttributeError."""
    from dante.remote import _get_remote_client

    (tmp_path / ".dante").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".dante" / "config.yaml").write_text("remote:\n")
    assert _get_remote_client(root=tmp_path) is None

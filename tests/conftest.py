"""Shared test fixtures — isolate all tests from real user config."""

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_home(tmp_path, monkeypatch):
    """Prevent tests from reading ~/.dante/ config.

    Every test gets a fake home directory so _get_remote_client
    and global config functions never see the real user config.
    """
    fake_home = tmp_path / "fakehome"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", staticmethod(lambda: fake_home))

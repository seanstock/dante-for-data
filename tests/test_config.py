"""Tests for dante.config — project and global configuration management."""

import os
from pathlib import Path

import pytest
import yaml

from dante.config import (
    global_dir,
    project_dir,
    load_global_connections,
    save_global_connections,
    load_global_credentials,
    save_global_credentials,
    load_project_config,
    save_project_config,
    get_default_connection_name,
    get_connection_config,
    _find_project_root,
)


# ---------------------------------------------------------------------------
# global_dir
# ---------------------------------------------------------------------------

def test_global_dir_creates_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    d = global_dir()
    assert d.exists()
    assert d.name == ".dante"


# ---------------------------------------------------------------------------
# project_dir
# ---------------------------------------------------------------------------

def test_project_dir_creates_dot_dante(tmp_path):
    d = project_dir(tmp_path)
    assert d == tmp_path / ".dante"
    assert d.exists()


def test_project_dir_idempotent(tmp_path):
    d1 = project_dir(tmp_path)
    d2 = project_dir(tmp_path)
    assert d1 == d2


# ---------------------------------------------------------------------------
# _find_project_root
# ---------------------------------------------------------------------------

def test_find_project_root_env_var_takes_priority(tmp_path, monkeypatch):
    """DANTE_PROJECT env var overrides directory walking."""
    project = tmp_path / "myproject"
    project.mkdir()
    monkeypatch.setenv("DANTE_PROJECT", str(project))
    root = _find_project_root()
    assert root == project.resolve()


def test_find_project_root_env_var_resolves_relative(tmp_path, monkeypatch):
    """DANTE_PROJECT is resolved to an absolute path."""
    monkeypatch.setenv("DANTE_PROJECT", ".")
    monkeypatch.chdir(tmp_path)
    root = _find_project_root()
    assert root.is_absolute()


def test_find_project_root_no_env_var_walks(tmp_path, monkeypatch):
    """Without DANTE_PROJECT, normal directory walking applies."""
    monkeypatch.delenv("DANTE_PROJECT", raising=False)
    (tmp_path / ".dante").mkdir()
    monkeypatch.chdir(tmp_path)
    root = _find_project_root()
    assert root == tmp_path


def test_find_project_root_finds_dante_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("DANTE_PROJECT", raising=False)
    (tmp_path / ".dante").mkdir()
    monkeypatch.chdir(tmp_path)
    root = _find_project_root()
    assert root == tmp_path


def test_find_project_root_finds_mcp_json(tmp_path, monkeypatch):
    monkeypatch.delenv("DANTE_PROJECT", raising=False)
    (tmp_path / ".mcp.json").write_text("{}")
    monkeypatch.chdir(tmp_path)
    root = _find_project_root()
    assert root == tmp_path


def test_find_project_root_walks_up(tmp_path, monkeypatch):
    monkeypatch.delenv("DANTE_PROJECT", raising=False)
    (tmp_path / ".dante").mkdir()
    sub = tmp_path / "a" / "b"
    sub.mkdir(parents=True)
    monkeypatch.chdir(sub)
    root = _find_project_root()
    assert root == tmp_path


def test_find_project_root_falls_back_to_cwd(tmp_path, monkeypatch):
    monkeypatch.delenv("DANTE_PROJECT", raising=False)
    monkeypatch.chdir(tmp_path)
    root = _find_project_root()
    assert root == tmp_path


# ---------------------------------------------------------------------------
# Global connections
# ---------------------------------------------------------------------------

def test_load_global_connections_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    data = load_global_connections()
    assert data == {"connections": {}}


def test_save_and_load_global_connections(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    payload = {
        "connections": {
            "prod": {"dialect": "postgresql", "host": "db.example.com", "database": "mydb"}
        }
    }
    save_global_connections(payload)
    loaded = load_global_connections()
    assert loaded["connections"]["prod"]["dialect"] == "postgresql"


def test_load_global_connections_bare_dict(tmp_path, monkeypatch):
    """Handles files that don't have the 'connections' wrapper."""
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    (tmp_path / ".dante").mkdir(exist_ok=True)
    path = tmp_path / ".dante" / "connections.yaml"
    path.write_text("my_conn:\n  dialect: sqlite\n")
    loaded = load_global_connections()
    assert "connections" in loaded
    assert "my_conn" in loaded["connections"]


# ---------------------------------------------------------------------------
# Global credentials
# ---------------------------------------------------------------------------

def test_load_global_credentials_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    data = load_global_credentials()
    assert data == {}


def test_save_and_load_global_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    save_global_credentials({"OPENAI_API_KEY": "sk-test"})
    loaded = load_global_credentials()
    assert loaded["OPENAI_API_KEY"] == "sk-test"


# ---------------------------------------------------------------------------
# Project config
# ---------------------------------------------------------------------------

def test_load_project_config_empty(tmp_path):
    data = load_project_config(tmp_path)
    assert data == {}


def test_save_and_load_project_config(tmp_path):
    save_project_config({"default_connection": "prod"}, root=tmp_path)
    loaded = load_project_config(tmp_path)
    assert loaded["default_connection"] == "prod"


# ---------------------------------------------------------------------------
# get_default_connection_name
# ---------------------------------------------------------------------------

def test_get_default_connection_name_none(tmp_path):
    assert get_default_connection_name(tmp_path) is None


def test_get_default_connection_name_set(tmp_path):
    save_project_config({"default_connection": "staging"}, root=tmp_path)
    assert get_default_connection_name(tmp_path) == "staging"


# ---------------------------------------------------------------------------
# get_connection_config
# ---------------------------------------------------------------------------

def test_get_connection_config_no_config(tmp_path):
    assert get_connection_config(root=tmp_path) is None


def test_get_connection_config_resolves_named(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    save_global_connections({
        "connections": {
            "dev": {"dialect": "sqlite", "database": "/tmp/dev.db"}
        }
    })
    save_project_config({"default_connection": "dev"}, root=tmp_path)
    cfg = get_connection_config(root=tmp_path)
    assert cfg is not None
    assert cfg["dialect"] == "sqlite"


def test_get_connection_config_resolves_password_env(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.setenv("MY_DB_PW", "secret123")
    save_global_connections({
        "connections": {
            "prod": {
                "dialect": "postgresql",
                "host": "db.example.com",
                "database": "app",
                "password_env": "MY_DB_PW",
            }
        }
    })
    save_project_config({"default_connection": "prod"}, root=tmp_path)
    cfg = get_connection_config(root=tmp_path)
    assert cfg["password"] == "secret123"
    assert "password_env" not in cfg


def test_get_connection_config_missing_env_var(tmp_path, monkeypatch):
    """If password_env is set but env var doesn't exist, no password field added."""
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.delenv("MISSING_VAR", raising=False)
    save_global_connections({
        "connections": {
            "prod": {
                "dialect": "postgresql",
                "database": "app",
                "password_env": "MISSING_VAR",
            }
        }
    })
    save_project_config({"default_connection": "prod"}, root=tmp_path)
    cfg = get_connection_config(root=tmp_path)
    assert "password" not in cfg
    assert "password_env" not in cfg

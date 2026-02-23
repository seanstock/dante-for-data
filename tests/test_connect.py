"""Tests for dante.connect — database connection management."""

import pytest
from pathlib import Path

from dante.connect import _build_url, connect
# Alias to avoid pytest treating this as a test fixture (name starts with test_)
from dante.connect import test_connection as check_connection


# ---------------------------------------------------------------------------
# _build_url
# ---------------------------------------------------------------------------

def test_build_url_postgresql():
    conn = {
        "dialect": "postgresql",
        "user": "alice",
        "password": "secret",
        "host": "db.example.com",
        "port": "5432",
        "database": "mydb",
    }
    url = _build_url(conn)
    assert url.startswith("postgresql://")
    assert "alice" in url
    assert "db.example.com" in url
    assert "mydb" in url


def test_build_url_postgresql_with_driver():
    conn = {
        "dialect": "postgresql",
        "driver": "psycopg2",
        "user": "alice",
        "password": "pw",
        "host": "localhost",
        "database": "db",
    }
    url = _build_url(conn)
    assert url.startswith("postgresql+psycopg2://")


def test_build_url_sqlite():
    conn = {"dialect": "sqlite", "database": "/tmp/test.db"}
    url = _build_url(conn)
    # Absolute paths in SQLite URLs get 4 slashes: sqlite:////abs/path
    assert url.startswith("sqlite://")
    assert "tmp/test.db" in url


def test_build_url_no_port():
    conn = {"dialect": "postgresql", "user": "u", "password": "p", "host": "h", "database": "d"}
    url = _build_url(conn)
    assert ":5432" not in url


def test_build_url_no_auth():
    conn = {"dialect": "sqlite", "database": "/tmp/mydb.db"}
    url = _build_url(conn)
    assert "@" not in url


def test_build_url_snowflake():
    conn = {
        "dialect": "snowflake",
        "user": "alice",
        "password": "secret",
        "account": "myorg.us-east-1",
        "database": "PROD",
        "schema": "PUBLIC",
    }
    url = _build_url(conn)
    assert url.startswith("snowflake://")
    assert "myorg.us-east-1" in url
    assert "PROD" in url
    assert "PUBLIC" in url


def test_build_url_snowflake_no_schema():
    conn = {
        "dialect": "snowflake",
        "user": "alice",
        "password": "secret",
        "account": "myorg.us-east-1",
        "database": "PROD",
    }
    url = _build_url(conn)
    assert url.startswith("snowflake://")
    assert url.endswith("PROD")


def test_build_url_databricks():
    conn = {
        "dialect": "databricks",
        "host": "adb-123.azuredatabricks.net",
        "password": "token123",
        "http_path": "/sql/1.0/warehouses/abc",
        "database": "my_catalog",
    }
    url = _build_url(conn)
    assert url.startswith("databricks://")
    assert "adb-123.azuredatabricks.net" in url


def test_build_url_mysql():
    conn = {
        "dialect": "mysql",
        "driver": "pymysql",
        "user": "root",
        "password": "pw",
        "host": "localhost",
        "port": "3306",
        "database": "app_db",
    }
    url = _build_url(conn)
    assert url.startswith("mysql+pymysql://")


def test_build_url_special_chars_in_password():
    conn = {
        "dialect": "postgresql",
        "user": "user",
        "password": "p@ss#word!",
        "host": "localhost",
        "database": "db",
    }
    url = _build_url(conn)
    # URL-encoded special chars — should not break the URL format
    assert "postgresql://" in url
    assert "@" in url  # separator between auth and host still present


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------

def test_connect_with_sqlite_url():
    """SQLite in-memory DB can be connected to directly by URL."""
    engine = connect(url="sqlite:///:memory:")
    assert engine is not None
    # Test we can actually query it
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
        assert result[0] == 1


def test_connect_caches_engine():
    """Calling connect() twice with the same URL returns the same engine."""
    e1 = connect(url="sqlite:///:memory:")
    e2 = connect(url="sqlite:///:memory:")
    assert e1 is e2


def test_connect_no_config_raises(tmp_path):
    """Without configured connection, connect() raises ConnectionError."""
    # Use a fresh URL that isn't in the cache to force config lookup
    with pytest.raises(ConnectionError, match="No database connection configured"):
        connect(root=tmp_path)


# ---------------------------------------------------------------------------
# check_connection()
# ---------------------------------------------------------------------------

def test_check_connection_success():
    success, msg = check_connection({"url": "sqlite:///:memory:"})
    assert success is True
    assert "successful" in msg.lower()


def test_check_connection_failure():
    success, msg = check_connection({
        "dialect": "postgresql",
        "host": "localhost",
        "port": "9999",
        "database": "nonexistent",
        "user": "baduser",
        "password": "badpass",
    })
    assert success is False
    assert msg  # some error message

"""Database connection management via SQLAlchemy.

Connections are configured globally at ~/.dante/connections.yaml.
Projects reference a named connection in .dante/config.yaml.
"""

from __future__ import annotations

import threading
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from dante.config import get_connection_config

_engines: dict[str, Engine] = {}
_engines_lock = threading.Lock()


def _build_url(conn: dict) -> str:
    """Build a SQLAlchemy connection URL from a connection config dict."""
    dialect = conn.get("dialect", "postgresql")
    driver = conn.get("driver", "")
    user = conn.get("user", "")
    password = conn.get("password", "")
    host = conn.get("host", "")
    port = conn.get("port", "")
    database = conn.get("database", "")
    schema = conn.get("schema", "")
    account = conn.get("account", "")

    # Snowflake uses account-based URLs
    if dialect == "snowflake":
        url = f"snowflake://{quote_plus(user)}:{quote_plus(password)}@{account}/{database}"
        if schema:
            url += f"/{schema}"
        return url

    # Databricks uses a specific connector
    if dialect == "databricks":
        http_path = conn.get("http_path", "")
        token = conn.get("token", password)
        return (
            f"databricks://token:{quote_plus(token)}@{host}"
            f"?http_path={quote_plus(http_path)}&catalog={quote_plus(database)}"
        )

    # Standard dialects (postgresql, mysql, sqlite, etc.)
    dialect_str = f"{dialect}+{driver}" if driver else dialect
    auth = f"{quote_plus(user)}:{quote_plus(password)}@" if user else ""
    host_part = f"{host}:{port}" if port else host
    url = f"{dialect_str}://{auth}{host_part}/{database}"
    return url


def connect(url: str | None = None, name: str | None = None, root: Path | None = None) -> Engine:
    """Get or create a SQLAlchemy engine.

    Args:
        url: Direct connection URL. If provided, uses this instead of config.
        name: Named connection from ~/.dante/connections.yaml.
        root: Project root to find .dante/config.yaml.

    Returns:
        A SQLAlchemy Engine, cached by URL.
    """
    if url is not None:
        with _engines_lock:
            if url not in _engines:
                _engines[url] = create_engine(url, echo=False)
            return _engines[url]

    conn = get_connection_config(name=name, root=root)
    if conn is None:
        raise ConnectionError(
            "No database connection configured. "
            "Run 'dante ui' to set up a connection, or pass a URL directly."
        )

    # Check for a direct url field
    if "url" in conn:
        conn_url = conn["url"]
    else:
        conn_url = _build_url(conn)

    with _engines_lock:
        if conn_url not in _engines:
            _engines[conn_url] = create_engine(conn_url, echo=False)
        return _engines[conn_url]


def test_connection(conn_config: dict) -> tuple[bool, str]:
    """Test a connection config. Returns (success, message)."""
    try:
        if "url" in conn_config:
            url = conn_config["url"]
        else:
            url = _build_url(conn_config)
        engine = create_engine(url, echo=False)
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1"))
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
        finally:
            engine.dispose()
    except Exception as e:
        return False, str(e)

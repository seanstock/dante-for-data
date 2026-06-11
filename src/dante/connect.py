"""Database connection management via SQLAlchemy.

Connections are configured globally at ~/.dante/connections.yaml.
Projects reference a named connection in .dante/config.yaml.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from dante.config import get_connection_config

logger = logging.getLogger(__name__)

_engines: dict[str, Engine] = {}
_engine_last_used: dict[str, float] = {}
_engines_lock = threading.Lock()

# Engines idle for longer than this (seconds) are disposed on next access.
_IDLE_TIMEOUT = 600  # 10 minutes


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


def _get_or_create_engine(url: str) -> Engine:
    """Return a cached engine or create a new one.

    Disposes engines that have been idle longer than _IDLE_TIMEOUT.
    Must be called with _engines_lock held.
    """
    now = time.monotonic()

    if url in _engines:
        last = _engine_last_used.get(url, now)
        if now - last > _IDLE_TIMEOUT:
            logger.info("Disposing idle database engine (idle %.0fs)", now - last)
            try:
                _engines[url].dispose()
            except Exception as e:
                logger.debug("Engine dispose failed: %s", e)
            del _engines[url]
            _engine_last_used.pop(url, None)
        else:
            _engine_last_used[url] = now
            return _engines[url]

    engine = create_engine(url, echo=False, pool_pre_ping=True)
    _engines[url] = engine
    _engine_last_used[url] = now
    return engine


def connect(
    url: str | None = None, name: str | None = None, root: Path | None = None
) -> Engine:
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
            return _get_or_create_engine(url)

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
        return _get_or_create_engine(conn_url)


def dispose_all() -> None:
    """Dispose all cached engines. Useful for cleanup in tests or shutdown."""
    with _engines_lock:
        for engine in _engines.values():
            try:
                engine.dispose()
            except Exception as e:
                logger.debug("Engine dispose failed: %s", e)
        _engines.clear()
        _engine_last_used.clear()


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

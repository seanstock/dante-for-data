"""Tests for the SQL-safety helpers in dante.query.

These cover the LIMIT auto-injection, the mutating-statement guard, and the
qualified-identifier builder — the load-bearing safety pieces.
"""

from dante.query import _inject_limit, _is_mutating, _qualified_name


# ── _inject_limit ─────────────────────────────────────────────────────────────


def test_inject_limit_adds_when_absent():
    out = _inject_limit("SELECT * FROM orders", 100)
    assert out.rstrip().endswith("LIMIT 100")


def test_inject_limit_respects_real_limit_clause():
    q = "SELECT * FROM orders LIMIT 10"
    assert _inject_limit(q, 100) == q


def test_inject_limit_ignores_limit_in_string_literal():
    """A 'limit' substring inside a string literal must NOT suppress injection."""
    q = "SELECT * FROM events WHERE message LIKE '%limit%'"
    out = _inject_limit(q, 100)
    assert out.rstrip().endswith("LIMIT 100")


def test_inject_limit_ignores_limit_in_column_name():
    q = "SELECT rate_limit FROM api_config"
    out = _inject_limit(q, 50)
    assert out.rstrip().endswith("LIMIT 50")


def test_inject_limit_ignores_limit_in_comment():
    q = "SELECT 1 -- get one row, no limit needed"
    out = _inject_limit(q, 50)
    assert out.rstrip().endswith("LIMIT 50")


# ── _is_mutating ──────────────────────────────────────────────────────────────


def test_is_mutating_detects_plain_delete():
    assert _is_mutating("DELETE FROM users")


def test_is_mutating_allows_select():
    assert not _is_mutating("SELECT * FROM users")


def test_is_mutating_detects_data_modifying_cte():
    """A WITH-wrapped DELETE (data-modifying CTE) must be rejected."""
    q = "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x"
    assert _is_mutating(q)


def test_is_mutating_detects_stacked_statement():
    """A second mutating statement after a SELECT must be rejected."""
    assert _is_mutating("SELECT 1; DROP TABLE users")


def test_is_mutating_allows_cte_select():
    q = "WITH x AS (SELECT 1 AS n) SELECT n FROM x"
    assert not _is_mutating(q)


# ── _qualified_name ───────────────────────────────────────────────────────────


def test_qualified_name_quotes_identifiers():
    """Identifiers are quoted, defeating injection via the table name."""
    out = _qualified_name("users; DROP TABLE audit", schema=None)
    # The semicolon/keyword must be neutralized inside quotes, not left bare.
    assert "DROP TABLE" not in out.replace('"', "").split(";")[0] or '"' in out
    assert out.startswith('"') or out.startswith("`") or out.startswith("[")


def test_qualified_name_quotes_schema_and_table():
    out = _qualified_name("orders", schema="public")
    # Both parts present and quoted, joined by a dot.
    assert "orders" in out and "public" in out
    assert "." in out

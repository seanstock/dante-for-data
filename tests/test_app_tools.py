"""Tests for dante.tools.app_tools — MCP tool wrappers for Data Apps."""

from __future__ import annotations


import pytest

from dante.tools import app_tools


@pytest.fixture(autouse=True)
def _clean_app_registry():
    """Clear the in-memory app registry before and after each test."""
    with app_tools._apps_lock:
        app_tools._apps.clear()
    yield
    with app_tools._apps_lock:
        app_tools._apps.clear()


# ---------------------------------------------------------------------------
# dante_app_create
# ---------------------------------------------------------------------------


def test_create_returns_instructions():
    result = app_tools.dante_app_create("Sales Dashboard")
    assert "Sales Dashboard" in result
    assert "sales-dashboard" in result  # id = slug
    assert "dante_app_add_value" in result  # next-step instructions


def test_create_registers_app():
    app_tools.dante_app_create("Test App")
    with app_tools._apps_lock:
        assert "test-app" in app_tools._apps


def test_create_with_template():
    result = app_tools.dante_app_create("Report", template="report")
    assert "report" in result.lower()


# ---------------------------------------------------------------------------
# dante_app_add_value
# ---------------------------------------------------------------------------


def test_add_value_to_existing_app():
    app_tools.dante_app_create("Test App")
    result = app_tools.dante_app_add_value(
        "test-app", "total", "SELECT count(*) FROM users"
    )
    assert "total" in result
    assert "{total}" in result


def test_add_value_to_missing_app():
    result = app_tools.dante_app_add_value("nonexistent", "x", "SELECT 1")
    assert "Error" in result


def test_add_value_with_format():
    app_tools.dante_app_create("Test App")
    result = app_tools.dante_app_add_value(
        "test-app", "data", "SELECT *", format="table"
    )
    assert "table" in result


# ---------------------------------------------------------------------------
# dante_app_set_html
# ---------------------------------------------------------------------------


def test_set_html():
    app_tools.dante_app_create("Test App")
    result = app_tools.dante_app_set_html("test-app", "<div>{total}</div>")
    assert "test-app" in result


def test_set_html_with_css_and_js():
    app_tools.dante_app_create("Test App")
    app_tools.dante_app_set_html(
        "test-app",
        "<div>{total}</div>",
        css=".custom { color: red; }",
        js="console.log('hi');",
    )
    with app_tools._apps_lock:
        app = app_tools._apps["test-app"]
    assert ".custom" in app.css
    assert "console.log" in app.js


def test_set_html_missing_app():
    result = app_tools.dante_app_set_html("nope", "<div>x</div>")
    assert "Error" in result


# ---------------------------------------------------------------------------
# dante_app_render
# ---------------------------------------------------------------------------


def test_render_missing_app():
    result = app_tools.dante_app_render("nope")
    assert "Error" in result


def test_render_no_html_set():
    app_tools.dante_app_create("Empty App")
    result = app_tools.dante_app_render("empty-app")
    assert "Error" in result
    assert "HTML" in result

"""Tests for dante.app — Data App template engine."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dante.app import App, create, _format_scalar, _df_to_html_table


# ---------------------------------------------------------------------------
# _format_scalar
# ---------------------------------------------------------------------------


def test_format_scalar_large_positive():
    result = _format_scalar(2_500_000.0)
    assert "M" in result
    assert "$" in result


def test_format_scalar_large_negative():
    result = _format_scalar(-1_500_000.0)
    assert "M" in result


def test_format_scalar_thousands():
    result = _format_scalar(42_000.0)
    assert result == "42,000"


def test_format_scalar_small_float():
    result = _format_scalar(3.14159)
    assert result == "3.14"


def test_format_scalar_integer():
    assert _format_scalar(42) == "42"


def test_format_scalar_string():
    assert _format_scalar("hello") == "hello"


# ---------------------------------------------------------------------------
# _df_to_html_table — HTML escaping
# ---------------------------------------------------------------------------


def test_df_to_html_table_basic():
    df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [95, 87]})
    html = _df_to_html_table(df)
    assert '<table class="data-table">' in html
    assert "<th>name</th>" in html
    assert "<td>Alice</td>" in html


def test_df_to_html_table_escapes_values():
    """Values containing HTML should be escaped to prevent XSS."""
    df = pd.DataFrame({"col": ['<script>alert("xss")</script>']})
    html = _df_to_html_table(df)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_df_to_html_table_escapes_column_names():
    df = pd.DataFrame({"<b>bold</b>": [1]})
    html = _df_to_html_table(df)
    assert "<b>" not in html
    assert "&lt;b&gt;" in html


def test_df_to_html_table_empty():
    df = pd.DataFrame({"a": [], "b": []})
    html = _df_to_html_table(df)
    assert "<thead>" in html
    assert "<tbody>" in html


# ---------------------------------------------------------------------------
# App class — construction
# ---------------------------------------------------------------------------


def test_create_returns_app(tmp_path):
    app = create("Test App", template="blank", root=tmp_path)
    assert isinstance(app, App)
    assert app.title == "Test App"
    assert app.template == "blank"


def test_app_id_is_slug(tmp_path):
    app = App("My Dashboard 2024", root=tmp_path)
    assert app.id == "my-dashboard-2024"


def test_app_add_remove_value(tmp_path):
    app = App("Test", root=tmp_path)
    app.add_value("total", "SELECT count(*) FROM users")
    assert "total" in app.value_names()
    app.remove_value("total")
    assert "total" not in app.value_names()


def test_app_html_css_js_properties(tmp_path):
    app = App("Test", root=tmp_path)
    app.html = "<div>{total}</div>"
    app.css = ".custom { color: red; }"
    app.js = "console.log('hi');"
    assert app.html == "<div>{total}</div>"
    assert app.css == ".custom { color: red; }"
    assert app.js == "console.log('hi');"


# ---------------------------------------------------------------------------
# App.render — integration with mocked SQL
# ---------------------------------------------------------------------------


def _mock_sql(query, root=None):
    """Return a small DataFrame, asserting a real SQL query was passed."""
    assert isinstance(query, str) and len(query) > 0, f"Expected non-empty SQL, got: {query!r}"
    return pd.DataFrame({"metric": ["Users"], "value": [1234]})


@patch("dante.app.run_sql", side_effect=_mock_sql)
def test_app_render_scalar(mock_sql, tmp_path):
    app = App("Scalar Test", template="blank", root=tmp_path)
    app.html = "<div>{total}</div>"
    app.add_value("total", "SELECT count(*) FROM users", format="scalar")
    path = app.render()
    assert Path(path).exists()
    content = Path(path).read_text()
    assert "Users" in content  # scalar = first cell


@patch("dante.app.run_sql", side_effect=_mock_sql)
def test_app_render_table(mock_sql, tmp_path):
    app = App("Table Test", template="blank", root=tmp_path)
    app.html = "<div>{data}</div>"
    app.add_value("data", "SELECT * FROM users", format="table")
    path = app.render()
    content = Path(path).read_text()
    assert '<table class="data-table">' in content


@patch("dante.app.run_sql", side_effect=_mock_sql)
def test_app_render_chart(mock_sql, tmp_path):
    app = App("Chart Test", template="blank", root=tmp_path)
    app.html = "<div>{chart1}</div>"
    app.add_value("chart1", "SELECT metric, value FROM t", format="chart")
    path = app.render()
    content = Path(path).read_text()
    assert "plotly" in content.lower()


@patch("dante.app.run_sql", side_effect=Exception("connection failed"))
def test_app_render_sql_error_escaped(mock_sql, tmp_path):
    """SQL errors should be HTML-escaped in the output."""
    app = App("Error Test", template="blank", root=tmp_path)
    app.html = "<div>{broken}</div>"
    app.add_value("broken", "SELECT * FROM nonexistent")
    path = app.render()
    content = Path(path).read_text()
    assert "Error:" in content
    assert "connection failed" in content


@patch("dante.app.run_sql", side_effect=_mock_sql)
def test_app_render_creates_outputs_dir(mock_sql, tmp_path):
    outputs = tmp_path / "outputs"
    assert not outputs.exists()
    app = App("Dir Test", template="blank", root=tmp_path)
    app.html = "<p>hi</p>"
    app.render()
    assert outputs.is_dir()


@patch("dante.app.run_sql", side_effect=_mock_sql)
def test_app_render_contains_plotly_cdn(mock_sql, tmp_path):
    """All rendered apps should load Plotly.js from CDN."""
    app = App("CDN Test", template="blank", root=tmp_path)
    app.html = "<p>test</p>"
    path = app.render()
    content = Path(path).read_text()
    assert "cdn.plot.ly" in content


@patch("dante.app.run_sql", side_effect=_mock_sql)
def test_app_render_includes_custom_css_and_js(mock_sql, tmp_path):
    app = App("Custom Test", template="blank", root=tmp_path)
    app.html = "<p>test</p>"
    app.css = ".highlight { background: yellow; }"
    app.js = "document.title = 'custom';"
    path = app.render()
    content = Path(path).read_text()
    assert ".highlight" in content
    assert "document.title" in content

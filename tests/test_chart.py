"""Tests for dante.chart — Plotly chart generation."""

import os
from pathlib import Path

import pandas as pd
import pytest

from dante.chart import chart, _slugify


# ---------------------------------------------------------------------------
# _slugify
# ---------------------------------------------------------------------------

def test_slugify_basic():
    assert _slugify("Revenue By Month") == "revenue-by-month"


def test_slugify_empty():
    assert _slugify("") == "chart"


def test_slugify_special_chars():
    result = _slugify("ARR ($M) by Quarter!")
    assert "$" not in result
    assert "!" not in result


def test_slugify_spaces_to_dashes():
    assert _slugify("a b c") == "a-b-c"


# ---------------------------------------------------------------------------
# chart() — HTML output
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "month": ["Jan", "Feb", "Mar", "Apr"],
        "revenue": [100, 150, 120, 200],
        "costs": [80, 100, 90, 140],
    })


def test_chart_creates_html_file(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", title="Revenue Over Time",
                 format="html", root=tmp_path)
    assert Path(path).exists()
    assert path.endswith(".html")


def test_chart_html_contains_plotly(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", title="Test Chart",
                 format="html", root=tmp_path)
    content = Path(path).read_text()
    assert "plotly" in content.lower()


def test_chart_filename_from_title(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", title="Monthly Revenue",
                 format="html", root=tmp_path)
    assert "monthly-revenue" in Path(path).name


def test_chart_custom_filename(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", title="Ignored",
                 filename="my-custom-chart", format="html", root=tmp_path)
    assert "my-custom-chart" in Path(path).name


def test_chart_creates_outputs_dir(tmp_path, sample_df):
    outputs = tmp_path / "outputs"
    assert not outputs.exists()
    chart(sample_df, x="month", y="revenue", title="Test", format="html", root=tmp_path)
    assert outputs.exists()


def test_chart_returns_string_path(tmp_path, sample_df):
    result = chart(sample_df, x="month", y="revenue", title="Test",
                   format="html", root=tmp_path)
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Chart kinds
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["bar", "line", "scatter"])
def test_chart_kinds_basic(tmp_path, sample_df, kind):
    path = chart(sample_df, x="month", y="revenue", kind=kind,
                 title=f"{kind} chart", format="html", root=tmp_path)
    assert Path(path).exists()


def test_chart_bar_multi_y(tmp_path, sample_df):
    path = chart(sample_df, x="month", y=["revenue", "costs"],
                 kind="bar", title="Revenue vs Costs", format="html", root=tmp_path)
    assert Path(path).exists()


def test_chart_line_multi_y(tmp_path, sample_df):
    path = chart(sample_df, x="month", y=["revenue", "costs"],
                 kind="line", title="Trend", format="html", root=tmp_path)
    assert Path(path).exists()


def test_chart_pie(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", kind="pie",
                 title="Revenue Pie", format="html", root=tmp_path)
    assert Path(path).exists()


def test_chart_histogram(tmp_path, sample_df):
    path = chart(sample_df, x="revenue", kind="histogram",
                 title="Revenue Distribution", format="html", root=tmp_path)
    assert Path(path).exists()


def test_chart_box(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", kind="box",
                 title="Revenue Box", format="html", root=tmp_path)
    assert Path(path).exists()


def test_chart_heatmap(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", kind="heatmap",
                 title="Heatmap", format="html", root=tmp_path)
    assert Path(path).exists()


# ---------------------------------------------------------------------------
# Themes
# ---------------------------------------------------------------------------

def test_chart_dark_theme(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", theme="dark",
                 title="Dark", format="html", root=tmp_path)
    assert Path(path).exists()


def test_chart_light_theme(tmp_path, sample_df):
    path = chart(sample_df, x="month", y="revenue", theme="light",
                 title="Light", format="html", root=tmp_path)
    assert Path(path).exists()


# ---------------------------------------------------------------------------
# Raw Plotly spec dict
# ---------------------------------------------------------------------------

def test_chart_raw_plotly_spec(tmp_path):
    spec = {
        "data": [{"type": "bar", "x": ["A", "B", "C"], "y": [1, 2, 3]}],
        "layout": {"title": "Raw spec"},
    }
    path = chart(spec, title="Raw Spec Chart", format="html", root=tmp_path)
    assert Path(path).exists()


# ---------------------------------------------------------------------------
# Type errors
# ---------------------------------------------------------------------------

def test_chart_invalid_data_type(tmp_path):
    with pytest.raises(TypeError, match="DataFrame or Plotly spec dict"):
        chart("invalid", title="Bad", root=tmp_path)

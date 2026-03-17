"""Tests for dante.tools.chart_tools — MCP tool wrapper for chart generation."""

from __future__ import annotations

import json

from dante.tools.chart_tools import dante_chart


def test_chart_from_json_array(tmp_path, monkeypatch):
    monkeypatch.setenv("DANTE_PROJECT", str(tmp_path))
    data = json.dumps(
        [
            {"month": "Jan", "revenue": 100},
            {"month": "Feb", "revenue": 150},
        ]
    )
    result = dante_chart(data, x="month", y="revenue", title="Revenue")
    assert "Chart saved to:" in result
    assert "revenue" in result.lower()


def test_chart_from_plotly_spec(tmp_path, monkeypatch):
    monkeypatch.setenv("DANTE_PROJECT", str(tmp_path))
    spec = json.dumps(
        {
            "data": [{"type": "bar", "x": ["A", "B"], "y": [1, 2]}],
            "layout": {"title": "Test"},
        }
    )
    result = dante_chart(spec, title="Spec Chart")
    assert "Chart saved to:" in result


def test_chart_invalid_json():
    result = dante_chart("not json", title="Bad")
    assert "Error" in result


def test_chart_multi_y(tmp_path, monkeypatch):
    monkeypatch.setenv("DANTE_PROJECT", str(tmp_path))
    data = json.dumps(
        [
            {"month": "Jan", "revenue": 100, "costs": 80},
            {"month": "Feb", "revenue": 150, "costs": 100},
        ]
    )
    result = dante_chart(data, x="month", y="revenue,costs", title="Multi")
    assert "Chart saved to:" in result

"""Tests for the dataframe_to_markdown utility."""

import pandas as pd

from dante._utils import dataframe_to_markdown


def test_empty_dataframe_returns_default_message():
    df = pd.DataFrame()
    assert dataframe_to_markdown(df) == "_No results._"


def test_empty_dataframe_custom_message():
    df = pd.DataFrame()
    assert dataframe_to_markdown(df, empty_msg="Nothing found.") == "Nothing found."


def test_single_row():
    df = pd.DataFrame([{"name": "Alice", "age": 30}])
    result = dataframe_to_markdown(df)
    lines = result.split("\n")
    assert len(lines) == 3  # header + separator + 1 row
    assert "name" in lines[0]
    assert "age" in lines[0]
    assert "Alice" in lines[2]
    assert "30" in lines[2]


def test_multiple_rows():
    df = pd.DataFrame([
        {"x": 1, "y": 2},
        {"x": 3, "y": 4},
    ])
    result = dataframe_to_markdown(df)
    lines = result.split("\n")
    assert len(lines) == 4  # header + separator + 2 rows


def test_separator_row_has_dashes():
    df = pd.DataFrame([{"col": "val"}])
    result = dataframe_to_markdown(df)
    lines = result.split("\n")
    assert "---" in lines[1]

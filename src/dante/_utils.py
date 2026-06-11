"""Shared internal utilities for dante-lib."""

from __future__ import annotations

import asyncio
import re
import unicodedata
from pathlib import Path
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")


def slugify(text: str, fallback: str = "file", max_len: int = 80) -> str:
    """Convert a string into a filesystem-safe slug.

    Uses the patterns.py implementation: unicode-aware, length-limited.

    Examples:
        "What is our monthly churn rate?" -> "what-is-our-monthly-churn-rate"
        "Revenue by Region 2024" -> "revenue-by-region-2024"
    """
    text = unicodedata.normalize("NFKD", text)
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text).strip("-")
    return text[:max_len] or fallback


def ensure_outputs_dir(root: Path | None = None) -> Path:
    """Resolve the project root and return its outputs/ directory, creating it if needed."""
    from dante.config import _find_project_root

    root = root or _find_project_root()
    outputs_dir = root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    return outputs_dir


def dataframe_to_markdown(df: Any, empty_msg: str = "_No results._") -> str:
    """Convert a pandas DataFrame to a markdown table string.

    Args:
        df: A pandas DataFrame.
        empty_msg: Message to return when the DataFrame is empty.
    """
    if df.empty:
        return empty_msg

    headers = list(df.columns)
    lines = ["| " + " | ".join(str(h) for h in headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines)


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine from synchronous code.

    Handles the case where an event loop is already running (e.g. inside an
    async MCP handler) by dispatching to a ThreadPoolExecutor.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()

    return asyncio.run(coro)

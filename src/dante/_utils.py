"""Shared internal utilities for dante-lib."""

from __future__ import annotations

import asyncio
import re
import unicodedata
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

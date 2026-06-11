"""Tests for async knowledge functions."""

import pytest
from unittest.mock import patch, AsyncMock

from dante.knowledge import save_pattern_async


@pytest.mark.asyncio
async def test_save_pattern_async_returns_filename(tmp_path):
    """save_pattern_async should save the pattern file and return its name."""
    with patch(
        "dante.knowledge.vectorize.generate_embedding",
        new_callable=AsyncMock,
        return_value=[0.1, 0.2, 0.3],
    ):
        name = await save_pattern_async(
            question="What is revenue?",
            sql="SELECT sum(amount) FROM orders",
            tables=["orders"],
            description="Total revenue",
            root=tmp_path,
        )
        assert name.endswith(".sql")
        assert "revenue" in name.lower()


@pytest.mark.asyncio
async def test_save_pattern_async_works_without_api_key(tmp_path, monkeypatch):
    """When OPENAI_API_KEY is missing, pattern is still saved (embedding skipped)."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with patch(
        "dante.knowledge.vectorize.generate_embedding",
        new_callable=AsyncMock,
        side_effect=RuntimeError("OPENAI_API_KEY not set"),
    ):
        name = await save_pattern_async(
            question="What is churn?",
            sql="SELECT count(*) FROM users WHERE churned",
            tables=["users"],
            description="Churn count",
            root=tmp_path,
        )
        assert name.endswith(".sql")

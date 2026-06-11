"""Tests for MCP server dispatch — verifies async handlers are awaited."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_async_handler_is_awaited():
    """call_tool should await coroutine results from async handlers."""
    from dante.mcp_server import call_tool

    mock_result = "search results here"

    with patch("dante.mcp_server._DISPATCH", {
        "dante_search": lambda args: AsyncMock(return_value=mock_result)()
    }):
        result = await call_tool("dante_search", {"query": "test", "top_k": 5})
        assert len(result) == 1
        assert result[0].text == mock_result


@pytest.mark.asyncio
async def test_sync_handler_still_works():
    """call_tool should still handle plain sync return values."""
    from dante.mcp_server import call_tool

    with patch("dante.mcp_server._DISPATCH", {
        "dante_checkpoint": lambda args: "Checkpoint saved."
    }):
        result = await call_tool("dante_checkpoint", {"name": "test"})
        assert len(result) == 1
        assert result[0].text == "Checkpoint saved."


@pytest.mark.asyncio
async def test_unknown_tool_returns_error():
    """call_tool should return an error for unknown tool names."""
    from dante.mcp_server import call_tool

    result = await call_tool("nonexistent_tool", {})
    assert len(result) == 1
    assert "Unknown tool" in result[0].text


@pytest.mark.asyncio
async def test_error_returns_concise_message_not_traceback():
    """A handler exception must return a concise error, not a raw traceback dump."""
    from dante.mcp_server import call_tool

    def _boom(args):
        raise ValueError("db is corrupt")

    with patch("dante.mcp_server._DISPATCH", {"dante_checkpoint": _boom}):
        result = await call_tool("dante_checkpoint", {"name": "x"})

    text = result[0].text
    assert "db is corrupt" in text
    assert "ValueError" in text
    # No Python traceback frames leaked to the client.
    assert "Traceback (most recent call last)" not in text
    assert 'File "' not in text


@pytest.mark.asyncio
async def test_sync_handler_runs_off_event_loop():
    """Sync handlers must be dispatched via a worker thread, not inline."""
    import threading
    from dante.mcp_server import call_tool

    calling_thread = threading.get_ident()
    observed = {}

    def _handler(args):
        observed["thread"] = threading.get_ident()
        return "ok"

    with patch("dante.mcp_server._DISPATCH", {"dante_checkpoint": _handler}):
        result = await call_tool("dante_checkpoint", {"name": "x"})

    assert result[0].text == "ok"
    # The sync handler ran on a different thread than the event loop.
    assert observed["thread"] != calling_thread

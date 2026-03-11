"""MCP tool implementations for analysis checkpointing and rollback."""

from __future__ import annotations

from dante import analyze


def dante_checkpoint(name: str) -> str:
    """Save a named checkpoint of the current analysis and outputs state.

    Args:
        name: A descriptive name for this checkpoint.

    Returns:
        Confirmation message.
    """
    return analyze.checkpoint(name)


def dante_rollback(name: str = "") -> str:
    """Roll back to a previously saved checkpoint.

    Args:
        name: The checkpoint name to restore. If empty, uses the most recent.

    Returns:
        Confirmation message or error if no checkpoints found.
    """
    return analyze.rollback(name)

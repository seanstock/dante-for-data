"""Read, write, and append to .dante/knowledge/notes.md.

Notes are free-form markdown that is always included in the LLM context
via a CLAUDE.md @import directive. They're the simplest knowledge layer --
a scratchpad for anything the user wants the model to always "know".
"""

from __future__ import annotations

from pathlib import Path

from dante.config import knowledge_dir


def _notes_path(root: Path | None = None) -> Path:
    """Return the path to notes.md, ensuring parent dirs exist."""
    return knowledge_dir(root) / "notes.md"


def read(root: Path | None = None) -> str:
    """Return the full contents of notes.md, or empty string if missing."""
    p = _notes_path(root)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8")


def write(content: str, root: Path | None = None) -> None:
    """Overwrite notes.md with *content*."""
    p = _notes_path(root)
    p.write_text(content, encoding="utf-8")


def append(text: str, root: Path | None = None) -> None:
    """Append *text* to notes.md (adds a trailing newline)."""
    p = _notes_path(root)
    existing = read(root)
    # Ensure there's a blank line between existing content and new text
    if existing and not existing.endswith("\n\n"):
        sep = "\n\n" if not existing.endswith("\n") else "\n"
    else:
        sep = ""
    p.write_text(existing + sep + text.rstrip() + "\n", encoding="utf-8")

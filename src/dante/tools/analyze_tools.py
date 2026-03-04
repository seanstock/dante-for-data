"""MCP tool implementations for analysis checkpointing and rollback."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from dante.config import _find_project_root


def _checkpoints_dir(root: Path | None = None) -> Path:
    root = root or _find_project_root()
    p = root / ".dante" / "checkpoints"
    p.mkdir(parents=True, exist_ok=True)
    return p


def dante_checkpoint(name: str) -> str:
    """Save a named checkpoint of the current analysis and outputs state.

    Args:
        name: A descriptive name for this checkpoint.

    Returns:
        Confirmation message.
    """
    root = _find_project_root()
    cp_dir = _checkpoints_dir(root) / name

    if cp_dir.exists():
        shutil.rmtree(cp_dir)
    cp_dir.mkdir(parents=True)

    copied = []
    for dir_name in ("analysis", "outputs"):
        src = root / dir_name
        if src.exists() and any(src.iterdir()):
            shutil.copytree(src, cp_dir / dir_name)
            count = sum(1 for _ in (cp_dir / dir_name).rglob("*") if _.is_file())
            copied.append(f"{dir_name}/ ({count} files)")

    # Write metadata
    meta = {
        "name": name,
        "created": datetime.now(timezone.utc).isoformat(),
        "dirs": copied,
    }
    (cp_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    if not copied:
        return f"Checkpoint `{name}` created but no files found in analysis/ or outputs/."

    return f"Checkpoint `{name}` saved: {', '.join(copied)}."


def dante_rollback(name: str = "") -> str:
    """Roll back to a previously saved checkpoint.

    Args:
        name: The checkpoint name to restore. If empty, uses the most recent.

    Returns:
        Confirmation message or error if no checkpoints found.
    """
    root = _find_project_root()
    cp_root = _checkpoints_dir(root)

    if name:
        cp_dir = cp_root / name
        if not cp_dir.exists():
            available = sorted(d.name for d in cp_root.iterdir() if d.is_dir())
            if available:
                return f"Checkpoint `{name}` not found. Available: {', '.join(available)}."
            return f"Checkpoint `{name}` not found. No checkpoints exist."
    else:
        # Find most recent by metadata timestamp
        candidates = []
        for d in cp_root.iterdir():
            meta_path = d / "meta.json"
            if d.is_dir() and meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                candidates.append((meta.get("created", ""), d))
        if not candidates:
            return "No checkpoints found. Use `dante_checkpoint` to create one."
        candidates.sort(reverse=True)
        cp_dir = candidates[0][1]
        name = cp_dir.name

    restored = []
    for dir_name in ("analysis", "outputs"):
        src = cp_dir / dir_name
        if src.exists():
            dest = root / dir_name
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(src, dest)
            count = sum(1 for _ in dest.rglob("*") if _.is_file())
            restored.append(f"{dir_name}/ ({count} files)")

    if not restored:
        return f"Checkpoint `{name}` exists but contains no directories to restore."

    return f"Rolled back to `{name}`: {', '.join(restored)}."

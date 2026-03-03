"""Analysis tools: checkpoint, rollback, and report compilation."""

from __future__ import annotations

import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

from dante.config import _find_project_root, project_dir


def checkpoint(name: str, root: Path | None = None) -> str:
    """Snapshot analysis/ and outputs/ directories.

    Args:
        name: Checkpoint name (used as directory name).
        root: Project root.

    Returns:
        Confirmation message with checkpoint path.
    """
    root = root or _find_project_root()
    checkpoints_dir = project_dir(root) / "checkpoints"
    checkpoint_path = checkpoints_dir / name

    if checkpoint_path.exists():
        shutil.rmtree(checkpoint_path)

    checkpoint_path.mkdir(parents=True)

    # Copy analysis/ and outputs/
    analysis_dir = root / "analysis"
    outputs_dir = root / "outputs"

    if analysis_dir.exists():
        shutil.copytree(analysis_dir, checkpoint_path / "analysis")
    if outputs_dir.exists():
        shutil.copytree(outputs_dir, checkpoint_path / "outputs")

    # Write metadata
    meta = {
        "name": name,
        "created": datetime.now(timezone.utc).isoformat(),
        "analysis_files": len(list((checkpoint_path / "analysis").rglob("*"))) if (checkpoint_path / "analysis").exists() else 0,
        "output_files": len(list((checkpoint_path / "outputs").rglob("*"))) if (checkpoint_path / "outputs").exists() else 0,
    }
    import json
    (checkpoint_path / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return f"Checkpoint '{name}' saved at {checkpoint_path}"


def rollback(name: str, root: Path | None = None) -> str:
    """Restore analysis/ and outputs/ from a checkpoint.

    Args:
        name: Checkpoint name to restore.
        root: Project root.

    Returns:
        Confirmation message.
    """
    root = root or _find_project_root()
    checkpoint_path = project_dir(root) / "checkpoints" / name

    if not checkpoint_path.exists():
        available = list_checkpoints(root)
        if available:
            return f"Checkpoint '{name}' not found. Available: {', '.join(available)}"
        return f"Checkpoint '{name}' not found. No checkpoints exist."

    # Restore analysis/
    analysis_dir = root / "analysis"
    if (checkpoint_path / "analysis").exists():
        if analysis_dir.exists():
            shutil.rmtree(analysis_dir)
        shutil.copytree(checkpoint_path / "analysis", analysis_dir)

    # Restore outputs/
    outputs_dir = root / "outputs"
    if (checkpoint_path / "outputs").exists():
        if outputs_dir.exists():
            shutil.rmtree(outputs_dir)
        shutil.copytree(checkpoint_path / "outputs", outputs_dir)

    return f"Restored to checkpoint '{name}'"


def list_checkpoints(root: Path | None = None) -> list[str]:
    """List available checkpoint names."""
    root = root or _find_project_root()
    checkpoints_dir = project_dir(root) / "checkpoints"
    if not checkpoints_dir.exists():
        return []
    return sorted(d.name for d in checkpoints_dir.iterdir() if d.is_dir())


def report(
    title: str,
    sections: list[str] | None = None,
    charts: list[str] | None = None,
    root: Path | None = None,
) -> str:
    """Compile analysis scripts and charts into a self-contained HTML report.

    Args:
        title: Report title.
        sections: Paths to Python scripts to include (relative to project root).
        charts: Paths to chart HTML files to embed.
        root: Project root.

    Returns:
        Path to the generated report HTML.
    """
    root = root or _find_project_root()
    outputs_dir = root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    sections = sections or []
    charts = charts or []

    # If no sections specified, auto-discover from analysis/
    if not sections:
        analysis_dir = root / "analysis"
        if analysis_dir.exists():
            sections = sorted(str(f.relative_to(root)) for f in analysis_dir.glob("*.py"))

    if not charts:
        if outputs_dir.exists():
            charts = sorted(str(f.relative_to(root)) for f in outputs_dir.glob("*.html") if "report" not in f.name)

    html_parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        f"<title>{title}</title>",
        "<style>",
        _REPORT_CSS,
        "</style>",
        "</head><body>",
        f"<h1>{title}</h1>",
        f"<p class='meta'>Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>",
    ]

    # Include analysis scripts
    for section_path in sections:
        full_path = root / section_path
        if full_path.exists():
            code = full_path.read_text(encoding="utf-8")
            html_parts.append(f"<h2>{full_path.stem}</h2>")
            html_parts.append(f"<pre><code>{_escape_html(code)}</code></pre>")

    # Embed charts as iframes
    for chart_path in charts:
        full_path = root / chart_path
        if full_path.exists():
            html_parts.append(f"<h2>{full_path.stem}</h2>")
            chart_content = full_path.read_text(encoding="utf-8")
            # Embed inline via srcdoc
            html_parts.append(
                f'<iframe srcdoc="{_escape_attr(chart_content)}" '
                f'style="width:100%;height:500px;border:none;"></iframe>'
            )

    html_parts.extend(["</body></html>"])

    # Write report
    import re
    slug = re.sub(r"[^\w\s-]", "", title.lower())
    slug = re.sub(r"[\s_]+", "-", slug).strip("-") or "report"
    out_path = outputs_dir / f"{slug}.html"
    out_path.write_text("\n".join(html_parts), encoding="utf-8")

    return str(out_path)


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_attr(text: str) -> str:
    return text.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;").replace(">", "&gt;")


_REPORT_CSS = """
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    max-width: 1200px;
    margin: 0 auto;
    padding: 2rem;
    background: #0d0d0d;
    color: #ececec;
}
h1 { color: #de2626; border-bottom: 2px solid #333; padding-bottom: 0.5rem; }
h2 { color: #ececec; margin-top: 2rem; }
.meta { color: #8e8e8e; font-size: 0.9rem; }
pre {
    background: #171717;
    border: 1px solid #333;
    border-radius: 6px;
    padding: 1rem;
    overflow-x: auto;
    font-size: 0.85rem;
}
code { color: #ececec; }
iframe { border-radius: 6px; margin: 1rem 0; }
"""

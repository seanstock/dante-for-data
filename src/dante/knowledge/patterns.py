"""Read and write .dante/knowledge/patterns/*.sql files.

Each pattern is a SQL file with YAML frontmatter containing metadata
about the query. Patterns are saved examples of SQL that answered a
specific business question -- they serve as both documentation and
retrieval targets for the embedding layer.

File format (e.g. what-is-our-monthly-churn-rate.sql):

    ---
    question: "What is our monthly churn rate?"
    tables: ["subscriptions"]
    description: "Counts churned subscriptions by month"
    source: manual
    created: 2024-01-15
    ---
    SELECT DATE_TRUNC('month', canceled_at) AS month, COUNT(*) AS churned
    FROM subscriptions
    WHERE canceled_at IS NOT NULL
    GROUP BY 1 ORDER BY 1
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import date
from pathlib import Path

import yaml

from dante._utils import slugify
from dante.config import knowledge_dir

logger = logging.getLogger(__name__)

# Matches a closing frontmatter delimiter on its own line (not "---" appearing
# inside a field value).
_FM_CLOSE = re.compile(r"^---\s*$", re.MULTILINE)


def _patterns_dir(root: Path | None = None) -> Path:
    """Return the path to the patterns directory, ensuring it exists."""
    p = knowledge_dir(root) / "patterns"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _parse_frontmatter(content: str) -> tuple[dict, str]:
    """Split a file into (frontmatter_dict, sql_body)."""
    if not content.startswith("---"):
        return {}, content

    # Find the closing delimiter on its own line (so a value containing "---"
    # does not truncate the frontmatter mid-field).
    m = _FM_CLOSE.search(content, 3)
    if m is None:
        return {}, content

    fm_raw = content[3 : m.start()].strip()
    body = content[m.end() :].strip()
    fm = yaml.safe_load(fm_raw) or {}
    return fm, body


def _render(metadata: dict, sql: str) -> str:
    """Render a pattern file with YAML frontmatter + SQL body."""
    fm = yaml.dump(
        metadata,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    ).strip()
    return f"---\n{fm}\n---\n{sql}\n"


def save_pattern(
    question: str,
    sql: str,
    tables: list[str] | None = None,
    description: str = "",
    source: str = "manual",
    root: Path | None = None,
) -> Path:
    """Save a SQL pattern to a local ``.sql`` file with YAML frontmatter.

    This is the local store only. Remote-mode routing (save to Dante Studio
    instead) is handled one layer up in :mod:`dante.knowledge`.

    Returns the path to the created file. If two different questions slugify to
    the same name, a short content hash is appended so neither is overwritten.
    """
    patterns_dir = _patterns_dir(root)
    slug = slugify(question)
    path = patterns_dir / f"{slug}.sql"

    # Disambiguate slug collisions with a *different* question so we never
    # silently overwrite an unrelated pattern.
    if path.exists():
        existing_q = load_pattern(path).get("question", "")
        if existing_q != question:
            suffix = hashlib.sha1(question.encode("utf-8")).hexdigest()[:6]
            path = patterns_dir / f"{slug}-{suffix}.sql"

    metadata = {
        "question": question,
        "tables": tables or [],
        "description": description,
        "source": source,
        "created": date.today().isoformat(),
    }

    path.write_text(_render(metadata, sql), encoding="utf-8")
    return path


def load_pattern(path: Path) -> dict:
    """Load a single pattern file.

    Returns a dict with keys: question, sql, tables, description, source,
    created, filename.
    """
    content = path.read_text(encoding="utf-8")
    fm, sql = _parse_frontmatter(content)
    return {
        "question": fm.get("question", ""),
        "sql": sql,
        "tables": fm.get("tables", []),
        "description": fm.get("description", ""),
        "source": fm.get("source", "manual"),
        "created": fm.get("created", ""),
        "filename": path.name,
    }


def list_patterns(root: Path | None = None) -> list[dict]:
    """List all saved patterns.

    Returns a list of dicts with keys: question, sql, tables, description,
    source, created, filename -- sorted by filename.
    """
    patterns_dir = _patterns_dir(root)
    results = []
    for path in sorted(patterns_dir.glob("*.sql")):
        try:
            results.append(load_pattern(path))
        except Exception as e:
            # One corrupt file must not break the whole listing.
            logger.warning("Skipping unreadable pattern %s: %s", path.name, e)
    return results


def delete_pattern(filename: str, root: Path | None = None) -> bool:
    """Delete a pattern file by filename. Returns True if it existed."""
    path = _patterns_dir(root) / filename
    if not path.exists():
        return False
    path.unlink()
    return True


def get_pattern(question: str, root: Path | None = None) -> dict | None:
    """Look up a pattern by its question (slugified to find the file).

    Returns the pattern dict or None if not found.
    """
    slug = slugify(question)
    path = _patterns_dir(root) / f"{slug}.sql"
    if not path.exists():
        return None
    return load_pattern(path)

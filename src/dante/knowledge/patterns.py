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

import re
import unicodedata
from datetime import date
from pathlib import Path

import yaml

from dante.config import knowledge_dir


def _patterns_dir(root: Path | None = None) -> Path:
    """Return the path to the patterns directory, ensuring it exists."""
    p = knowledge_dir(root) / "patterns"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _slugify(text: str) -> str:
    """Convert a question string into a filename-safe slug.

    "What is our monthly churn rate?" -> "what-is-our-monthly-churn-rate"
    """
    # Normalize unicode, lowercase, strip non-alphanumeric
    text = unicodedata.normalize("NFKD", text)
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text).strip("-")
    # Truncate to a reasonable filename length
    return text[:80]


def _parse_frontmatter(content: str) -> tuple[dict, str]:
    """Split a file into (frontmatter_dict, sql_body)."""
    if not content.startswith("---"):
        return {}, content

    # Find the closing ---
    end = content.find("---", 3)
    if end == -1:
        return {}, content

    fm_raw = content[3:end].strip()
    body = content[end + 3:].strip()
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
    """Save a SQL pattern to a .sql file with YAML frontmatter.

    Returns the path to the created file.
    """
    slug = _slugify(question)
    filename = f"{slug}.sql"
    path = _patterns_dir(root) / filename

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
        results.append(load_pattern(path))
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
    slug = _slugify(question)
    path = _patterns_dir(root) / f"{slug}.sql"
    if not path.exists():
        return None
    return load_pattern(path)

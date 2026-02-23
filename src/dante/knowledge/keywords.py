"""Read/write .dante/knowledge/keywords.yaml and substring matching.

Keywords are a lightweight retrieval layer: when a user's query contains
a keyword as a substring (case-insensitive), the associated content is
returned. No embeddings, no vector search -- just fast string matching.

File format:
    revenue: "Revenue = SUM(amount) from orders. Excludes refunds."
    churn: "Use canceled_at IS NOT NULL to find churned customers."
"""

from __future__ import annotations

from pathlib import Path

import yaml

from dante.config import knowledge_dir


def _keywords_path(root: Path | None = None) -> Path:
    """Return the path to keywords.yaml, ensuring parent dirs exist."""
    return knowledge_dir(root) / "keywords.yaml"


def load(root: Path | None = None) -> dict[str, str]:
    """Load keywords as {keyword: content}. Returns empty dict if missing."""
    p = _keywords_path(root)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def save(keywords: dict[str, str], root: Path | None = None) -> None:
    """Overwrite keywords.yaml with the given {keyword: content} mapping."""
    p = _keywords_path(root)
    with open(p, "w", encoding="utf-8") as f:
        yaml.dump(
            keywords,
            f,
            default_flow_style=False,
            sort_keys=True,
            allow_unicode=True,
        )


def add(keyword: str, content: str, root: Path | None = None) -> None:
    """Add or update a single keyword trigger."""
    kw = load(root)
    kw[keyword] = content
    save(kw, root)


def remove(keyword: str, root: Path | None = None) -> bool:
    """Remove a keyword. Returns True if it existed, False otherwise."""
    kw = load(root)
    if keyword not in kw:
        return False
    del kw[keyword]
    save(kw, root)
    return True


def list_keywords(root: Path | None = None) -> list[dict[str, str]]:
    """Return a list of {keyword, content} dicts, sorted by keyword."""
    kw = load(root)
    return [{"keyword": k, "content": v} for k, v in sorted(kw.items())]


def match(query: str, root: Path | None = None) -> list[dict[str, str]]:
    """Return all keywords whose key appears as a substring in *query*.

    Matching is case-insensitive. Returns a list of
    {keyword, content} dicts for every matching keyword.
    """
    kw = load(root)
    query_lower = query.lower()
    results = []
    for keyword, content in sorted(kw.items()):
        if keyword.lower() in query_lower:
            results.append({"keyword": keyword, "content": content})
    return results

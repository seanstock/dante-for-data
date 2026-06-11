"""Read/write .dante/knowledge/keywords.yaml and substring matching.

Keywords are a lightweight retrieval layer: when a user's query contains
a keyword as a substring (case-insensitive), the associated content is
returned. No embeddings, no vector search -- just fast string matching.

When Dante Studio is configured (remote.enabled), keywords sync
bidirectionally: local keywords push up as org-scoped, and org keywords
from Studio are merged into match results.

File format:
    revenue: "Revenue = SUM(amount) from orders. Excludes refunds."
    churn: "Use canceled_at IS NOT NULL to find churned customers."
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from dante.config import knowledge_dir

logger = logging.getLogger(__name__)


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


def _remote(root: Path | None = None):
    """Return a configured remote client, or None for local mode."""
    try:
        from dante.remote import _get_remote_client

        return _get_remote_client(root)
    except Exception:
        logger.debug("Failed to resolve remote client", exc_info=True)
        return None


def add(keyword: str, content: str, root: Path | None = None) -> None:
    """Add or update a single keyword trigger.

    In remote mode the keyword is created in Studio (org-scoped) only.
    In local mode it is written to keywords.yaml only. No dual-write.
    """
    remote = _remote(root)
    if remote is not None:
        remote.create_keyword(keyword, content)
        return

    kw = load(root)
    kw[keyword] = content
    save(kw, root)


def remove(keyword: str, root: Path | None = None) -> bool:
    """Remove a keyword. Returns True if it existed, False otherwise.

    In remote mode, deletes the org keyword from Studio (so Studio-only
    keywords are deletable). In local mode, removes it from keywords.yaml.
    """
    remote = _remote(root)
    if remote is not None:
        for rkw in remote.list_keywords(scope="org"):
            if rkw.get("keyword") == keyword:
                return remote.delete_keyword(rkw["id"])
        return False

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

    Matching is case-insensitive. In remote mode, matches against Studio's
    org keywords. In local mode, matches against keywords.yaml.

    Returns a list of {keyword, content} dicts for every matching keyword.
    """
    remote = _remote(root)
    if remote is not None:
        kw = {}
        try:
            for rkw in remote.list_keywords(scope="org"):
                name = rkw.get("keyword", "")
                if name:
                    kw[name] = rkw.get("content", "")
        except Exception:
            logger.debug("Failed to fetch remote keywords", exc_info=True)
    else:
        kw = load(root)

    query_lower = query.lower()
    results = []
    for keyword, content in sorted(kw.items()):
        if keyword.lower() in query_lower:
            results.append({"keyword": keyword, "content": content})
    return results

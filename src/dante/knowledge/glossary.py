"""Read and write .dante/knowledge/terms.yaml.

The glossary is a flat mapping of term -> definition, always loaded into
the LLM context via CLAUDE.md @import. It gives the model canonical
definitions for domain-specific jargon (ARR, churn, cohort, etc.).

File format:
    ARR: "Annual Recurring Revenue. MRR * 12."
    churn: "A customer is churned if no active subscription for 30+ days."
"""

from __future__ import annotations

from pathlib import Path

import yaml

from dante.config import knowledge_dir


def _terms_path(root: Path | None = None) -> Path:
    """Return the path to terms.yaml, ensuring parent dirs exist."""
    return knowledge_dir(root) / "terms.yaml"


def load(root: Path | None = None) -> dict[str, str]:
    """Load the glossary as {term: definition}. Returns empty dict if missing."""
    p = _terms_path(root)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def save(terms: dict[str, str], root: Path | None = None) -> None:
    """Overwrite terms.yaml with the given {term: definition} mapping."""
    p = _terms_path(root)
    with open(p, "w", encoding="utf-8") as f:
        yaml.dump(
            terms,
            f,
            default_flow_style=False,
            sort_keys=True,
            allow_unicode=True,
        )


def define(term: str, definition: str, root: Path | None = None) -> None:
    """Add or update a single glossary term."""
    terms = load(root)
    terms[term] = definition
    save(terms, root)


def undefine(term: str, root: Path | None = None) -> bool:
    """Remove a glossary term. Returns True if it existed, False otherwise."""
    terms = load(root)
    if term not in terms:
        return False
    del terms[term]
    save(terms, root)
    return True


def list_terms(root: Path | None = None) -> list[dict[str, str]]:
    """Return a list of {term, definition} dicts, sorted by term."""
    terms = load(root)
    return [{"term": k, "definition": v} for k, v in sorted(terms.items())]

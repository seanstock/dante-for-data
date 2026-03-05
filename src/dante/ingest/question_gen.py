"""Heuristic-based conversion of chart titles to natural-language questions.

Converts BI platform chart/widget titles into questions suitable for
embedding generation, anchored with dashboard context.
"""

import re

_AGGREGATION_WORDS = {
    "total", "count", "number", "avg", "average", "sum",
    "min", "max", "mean", "median",
}

_VERB_WORDS = {
    "show", "list", "display", "compare", "breakdown",
    "analyze", "track", "find", "view", "explore",
}

_TIME_WORDS = {
    "daily", "weekly", "monthly", "yearly", "annual",
    "quarterly", "ytd", "mtd", "wtd",
}

_RATE_PATTERNS = {"rate", "ratio", "percent", "pct", "proportion"}

_FAKE_PLURAL_ENDINGS = {"ss", "us", "is", "ous"}


def _clean_title(title: str) -> str:
    title = title.strip()
    title = re.sub(r"\s+", " ", title)
    return title


def _is_plural(word: str) -> bool:
    lower = word.lower()
    if not lower.endswith("s"):
        return False
    if lower.endswith("ies"):
        return True
    for ending in _FAKE_PLURAL_ENDINGS:
        if lower.endswith(ending):
            return False
    return True


def generate_question(entity_title: str, dashboard_title: str) -> str:
    """Convert a chart title into a natural-language question.

    Rules (evaluated in order, first match wins):
    1. Title ends with '?' → use as-is, append dashboard context if missing
    2. Title starts with # or aggregation word → "What is the {title}"
    3. Title starts with a verb → "Can you {title}"
    4. Title starts with "top" or "bottom" → "What are the {title}"
    5. Title starts with a time word → "What is the {title}"
    6. Title contains a rate/percentage word → "What is the {title}"
    7. Title contains "by" (breakdown pattern) → "What is the {title}"
    8. Title's last word looks plural → "What are the {title}"
    9. Default → "What is the {title}"

    Always appends "from {Dashboard Title}" for context anchoring.
    """
    title = _clean_title(entity_title)
    dashboard = _clean_title(dashboard_title)

    if not title:
        title = "Untitled chart"

    suffix = f" from {dashboard}" if dashboard else ""

    # Rule 1: Already a question
    if title.endswith("?"):
        if dashboard and dashboard.lower() not in title.lower():
            return f"{title[:-1]}{suffix}?"
        return title

    first_word = title.split()[0].lower().rstrip("#")

    # Rule 2: Starts with # or aggregation word
    if title.startswith("#") or first_word in _AGGREGATION_WORDS:
        return f"What is the {title}{suffix}?"

    # Rule 3: Starts with a verb
    if first_word in _VERB_WORDS:
        return f"Can you {title[0].lower()}{title[1:]}{suffix}?"

    # Rule 4: Starts with "top" or "bottom"
    if first_word in ("top", "bottom"):
        return f"What are the {title}{suffix}?"

    # Rule 5: Starts with a time word
    if first_word in _TIME_WORDS:
        return f"What is the {title}{suffix}?"

    # Rule 6: Contains rate/percentage words
    title_lower = title.lower()
    if any(pattern in title_lower for pattern in _RATE_PATTERNS):
        return f"What is the {title}{suffix}?"
    if "%" in title:
        return f"What is the {title}{suffix}?"

    # Rule 7: Contains "by" (breakdown pattern)
    if re.search(r"\bby\b", title_lower):
        return f"What is the {title}{suffix}?"

    # Rule 8: Last word looks plural
    last_word = title.split()[-1]
    if _is_plural(last_word):
        return f"What are the {title}{suffix}?"

    # Rule 9: Default
    return f"What is the {title}{suffix}?"

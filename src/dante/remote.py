"""Remote knowledge client for dante-ds.

Provides a thin HTTP client (stdlib only, no new dependencies) that
communicates with a hosted Dante knowledge API. When configured, the
search / save_pattern / define / undefine functions delegate to the
remote endpoint instead of the local SQLite / YAML stores.

Configuration (either location works; project config takes priority):

    # .dante/config.yaml  OR  ~/.dante/config.yaml
    remote:
      api_url: https://api.example.com
      api_key: sk-...
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from dante.config import global_dir, load_project_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal HTTP helper
# ---------------------------------------------------------------------------


def _http_request(
    url: str,
    *,
    method: str = "GET",
    body: dict | None = None,
    api_key: str = "",
) -> Any:
    """Make an HTTP request and return the parsed JSON response.

    Args:
        url: Full URL to request.
        method: HTTP verb (GET, POST, PATCH, DELETE, …).
        body: Optional dict to JSON-encode as the request body.
        api_key: Bearer token for the Authorization header.

    Returns:
        Parsed JSON response (dict, list, or None for empty bodies).

    Raises:
        ConnectionError: On HTTP errors or network failures.
    """
    data: bytes | None = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if not raw:
                return None
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        body_text = ""
        try:
            body_text = exc.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise ConnectionError(
            f"HTTP {exc.code} from {url}: {body_text}"
        ) from exc
    except urllib.error.URLError as exc:
        raise ConnectionError(f"Network error reaching {url}: {exc.reason}") from exc


# ---------------------------------------------------------------------------
# RemoteKnowledge client
# ---------------------------------------------------------------------------


class RemoteKnowledge:
    """Thin HTTP client for a hosted Dante knowledge API.

    Example::

        client = RemoteKnowledge("https://api.example.com", "sk-...")
        results = client.search("monthly churn rate")
    """

    def __init__(self, api_url: str, api_key: str) -> None:
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: dict | None = None,
    ) -> Any:
        url = f"{self.api_url}{path}"
        return _http_request(url, method=method, body=body, api_key=self.api_key)

    # ------------------------------------------------------------------
    # Patterns
    # ------------------------------------------------------------------

    def search(self, query: str, top_k: int = 10) -> list[dict]:
        """Search the remote knowledge base for patterns matching *query*.

        Sends ``source: "library"`` so the server can distinguish
        requests originating from the Python library vs. other clients.
        """
        payload = {"query": query, "top_k": top_k, "source": "library"}
        result = self._request("/knowledge/search", method="POST", body=payload)
        return result if isinstance(result, list) else []

    def save_pattern(
        self,
        question: str,
        sql: str,
        tables: list[str] | None = None,
        description: str = "",
    ) -> dict:
        """Save a SQL pattern to the remote knowledge base."""
        payload = {
            "question": question,
            "sql": sql,
            "tables": tables or [],
            "description": description,
        }
        result = self._request("/knowledge/patterns", method="POST", body=payload)
        return result if isinstance(result, dict) else {}

    def list_patterns(
        self,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List patterns stored in the remote knowledge base."""
        params = f"?limit={limit}&offset={offset}"
        if status is not None:
            params += f"&status={status}"
        result = self._request(f"/knowledge/patterns{params}", method="GET")
        return result if isinstance(result, list) else []

    def edit_pattern(self, pattern_id: str, **updates: Any) -> dict:
        """Partially update a remote pattern by its ID."""
        result = self._request(
            f"/knowledge/patterns/{pattern_id}",
            method="PATCH",
            body=updates,
        )
        return result if isinstance(result, dict) else {}

    def delete_pattern(self, pattern_id: str) -> bool:
        """Delete a remote pattern by its ID. Returns True on success."""
        try:
            self._request(
                f"/knowledge/patterns/{pattern_id}",
                method="DELETE",
            )
            return True
        except ConnectionError:
            return False

    # ------------------------------------------------------------------
    # Glossary
    # ------------------------------------------------------------------

    def define_term(self, term: str, definition: str) -> dict:
        """Add or update a glossary term in the remote knowledge base."""
        payload = {"term": term, "definition": definition}
        result = self._request("/knowledge/glossary", method="POST", body=payload)
        return result if isinstance(result, dict) else {}

    def list_terms(self, limit: int = 50, offset: int = 0) -> list[dict]:
        """List glossary terms from the remote knowledge base."""
        result = self._request(
            f"/knowledge/glossary?limit={limit}&offset={offset}", method="GET"
        )
        return result if isinstance(result, list) else []

    def undefine_term(self, term: str) -> bool:
        """Remove a glossary term from the remote knowledge base."""
        try:
            self._request(f"/knowledge/glossary/{term}", method="DELETE")
            return True
        except ConnectionError:
            return False

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return statistics from the remote knowledge base."""
        result = self._request("/knowledge/stats", method="GET")
        return result if isinstance(result, dict) else {}


# ---------------------------------------------------------------------------
# Helper: resolve remote client from config
# ---------------------------------------------------------------------------


def _load_global_config() -> dict:
    """Load ~/.dante/config.yaml if it exists."""
    path = global_dir() / "config.yaml"
    if not path.exists():
        return {}
    try:
        import yaml  # already a dependency of dante

        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as exc:
        logger.debug("Could not load global config: %s", exc)
        return {}


def _get_remote_client(root: Path | None = None) -> RemoteKnowledge | None:
    """Return a configured :class:`RemoteKnowledge` client, or *None*.

    Checks for ``remote.api_url`` / ``remote.api_key`` in:

    1. Project config (``.dante/config.yaml`` found via *root* or
       :func:`~dante.config.load_project_config`).
    2. Global config (``~/.dante/config.yaml``).

    Returns *None* if neither source has remote configuration.
    """
    for cfg in (load_project_config(root), _load_global_config()):
        remote = cfg.get("remote", {})
        api_url = remote.get("api_url", "").strip()
        api_key = remote.get("api_key", "").strip()
        if api_url and api_key:
            return RemoteKnowledge(api_url, api_key)
    return None

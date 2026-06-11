"""Redash ingestion (experimental).

Fetches dashboards and their underlying queries from the Redash API.
Redash is SQL-first: every query object has a `query` field with the
full SQL text.

Requires REDASH_URL and REDASH_API_KEY in ~/.dante/credentials.yaml
under the `redash` key.
"""

from __future__ import annotations

import logging

import requests

from dante.ingest import IngestionConfig, IngestionResult
from dante.ingest._common import make_embedding_id, get_credentials, embed_charts

logger = logging.getLogger(__name__)


def _make_id(dashboard_id: str, query_id: str) -> str:
    return make_embedding_id("redash", "rds", dashboard_id, query_id)


def _api_get(base_url: str, api_key: str, path: str) -> dict | list | None:
    try:
        resp = requests.get(
            f"{base_url.rstrip('/')}/api{path}",
            headers={"Authorization": f"Key {api_key}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Redash API request failed: %s", path, exc_info=True)
        return None


def _fetch_charts(base_url: str, api_key: str, limit: int) -> list[dict] | None:
    """List dashboards and collect the SQL from each widget's query.

    Returns None if the dashboard listing itself failed (so the caller can
    report an error instead of a clean empty run).
    """
    data = _api_get(base_url, api_key, "/dashboards")
    if data is None:
        return None
    if not data:
        return []

    dashboards = data.get("results", data) if isinstance(data, dict) else data
    if limit > 0:
        dashboards = dashboards[:limit]

    logger.info("Scanning %d Redash dashboards", len(dashboards))
    charts: list[dict] = []
    seen_queries: set[str] = set()

    for idx, dash in enumerate(dashboards):
        slug = dash.get("slug", "")
        dash_name = dash.get("name", f"Dashboard {slug}")

        detail = _api_get(base_url, api_key, f"/dashboards/{slug}")
        if not detail:
            continue

        for widget in detail.get("widgets", []):
            vis = widget.get("visualization")
            if not vis:
                continue

            query_obj = vis.get("query", {})
            sql = query_obj.get("query", "")
            q_id = str(query_obj.get("id", ""))
            q_name = query_obj.get("name", "") or vis.get("name", "")

            if not sql or len(sql) < 50 or not q_name:
                continue

            if q_id in seen_queries:
                continue
            seen_queries.add(q_id)

            charts.append(
                {
                    "dashboard_id": slug,
                    "dashboard_title": dash_name,
                    "element_id": q_id,
                    "element_title": q_name,
                    "sql": sql,
                }
            )

        if (idx + 1) % 10 == 0:
            logger.info(
                "  Processed %d/%d dashboards (%d queries)",
                idx + 1,
                len(dashboards),
                len(charts),
            )

    logger.info("Collected %d queries with SQL from Redash", len(charts))
    return charts


async def ingest_redash(config: IngestionConfig) -> IngestionResult:
    """Run the Redash ingestion pipeline (experimental)."""
    creds = get_credentials("redash", ["url", "api_key"])
    if not creds:
        logger.error(
            "Redash credentials not configured. Add 'redash' section with "
            "url and api_key to ~/.dante/credentials.yaml"
        )
        result = IngestionResult()
        result.errors += 1
        return result

    charts = _fetch_charts(creds["url"], creds["api_key"], config.dashboard_limit)
    if charts is None:
        logger.error("Redash dashboard listing failed — check URL/API key")
        result = IngestionResult()
        result.errors += 1
        return result
    if not charts:
        logger.info("No Redash queries found")
        return IngestionResult()

    return await embed_charts(
        charts, source="redash", config=config, make_id=_make_id
    )

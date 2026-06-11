"""Mode Analytics ingestion (experimental).

Fetches reports and their SQL queries from the Mode API.
Mode is SQL-first: every query object has a `raw_query` field
containing the full SQL text.

Requires MODE_TOKEN and MODE_SECRET in ~/.dante/credentials.yaml
under the `mode` key, plus a `workspace` field.
"""

from __future__ import annotations

import logging

import requests

from dante.ingest import IngestionConfig, IngestionResult
from dante.ingest._common import make_embedding_id, get_credentials, embed_charts

logger = logging.getLogger(__name__)

_API_BASE = "https://app.mode.com/api"


def _make_id(report_token: str, query_token: str) -> str:
    return make_embedding_id("mode", "mode", report_token, query_token)


def _api_get(session: requests.Session, path: str) -> dict | list | None:
    """Make an authenticated GET request to the Mode API."""
    try:
        resp = session.get(f"{_API_BASE}{path}", timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Mode API request failed: %s", path, exc_info=True)
        return None


def _fetch_charts(
    session: requests.Session, workspace: str, limit: int
) -> list[dict] | None:
    """List reports and extract query SQL from each.

    Returns None if the report listing itself failed.
    """
    data = _api_get(session, f"/{workspace}/reports")
    if data is None:
        return None
    if not data:
        return []

    reports = (
        data if isinstance(data, list) else data.get("_embedded", {}).get("reports", [])
    )
    if limit > 0:
        reports = reports[:limit]

    logger.info("Scanning %d Mode reports", len(reports))
    charts: list[dict] = []

    for idx, report in enumerate(reports):
        token = report.get("token", "")
        report_name = report.get("name", f"Report {token}")

        queries_data = _api_get(session, f"/{workspace}/reports/{token}/queries")
        if not queries_data:
            continue

        queries = (
            queries_data
            if isinstance(queries_data, list)
            else queries_data.get("_embedded", {}).get("queries", [])
        )

        for q in queries:
            sql = q.get("raw_query", "")
            q_name = q.get("name", "")
            q_token = q.get("token", "")

            if not sql or len(sql) < 50 or not q_name:
                continue

            charts.append(
                {
                    "dashboard_id": token,
                    "dashboard_title": report_name,
                    "element_id": q_token,
                    "element_title": q_name,
                    "sql": sql,
                }
            )

        if (idx + 1) % 10 == 0:
            logger.info(
                "  Processed %d/%d reports (%d queries)",
                idx + 1,
                len(reports),
                len(charts),
            )

    logger.info("Collected %d queries with SQL from Mode", len(charts))
    return charts


async def ingest_mode(config: IngestionConfig) -> IngestionResult:
    """Run the Mode ingestion pipeline (experimental)."""
    creds = get_credentials("mode", ["token", "secret", "workspace"])
    if not creds:
        logger.error(
            "Mode credentials not configured. Add 'mode' section with "
            "token, secret, and workspace to ~/.dante/credentials.yaml"
        )
        result = IngestionResult()
        result.errors += 1
        return result

    session = requests.Session()
    session.auth = (creds["token"], creds["secret"])
    session.headers["Accept"] = "application/json"

    charts = _fetch_charts(session, creds["workspace"], config.dashboard_limit)
    if charts is None:
        logger.error("Mode report listing failed — check token/workspace")
        result = IngestionResult()
        result.errors += 1
        return result
    if not charts:
        logger.info("No Mode queries found")
        return IngestionResult()

    return await embed_charts(
        charts, source="mode", config=config, make_id=_make_id
    )

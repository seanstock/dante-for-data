"""Sigma Computing ingestion (experimental).

Fetches workbooks and extracts the SQL for each element using
Sigma's dedicated query endpoints.

Requires SIGMA_HOST, SIGMA_CLIENT_ID, and SIGMA_CLIENT_SECRET
in ~/.dante/credentials.yaml under the `sigma` key.
"""

from __future__ import annotations

import logging

import requests

from dante.ingest import IngestionConfig, IngestionResult
from dante.ingest._common import make_embedding_id, get_credentials, embed_charts

logger = logging.getLogger(__name__)


def _make_id(workbook_id: str, element_id: str) -> str:
    return make_embedding_id("sigma", "sig", workbook_id, element_id)


def _get_access_token(creds: dict) -> str | None:
    """Obtain a bearer token via OAuth client credentials."""
    try:
        resp = requests.post(
            f"https://{creds['host']}/v2/auth/token",
            json={
                "grant_type": "client_credentials",
                "client_id": creds["client_id"],
                "client_secret": creds["client_secret"],
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("access_token")
    except Exception:
        logger.exception("Failed to authenticate with Sigma")
        return None


def _api_get(host: str, token: str, path: str) -> dict | list | None:
    try:
        resp = requests.get(
            f"https://{host}/v2{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Sigma API request failed: %s", path, exc_info=True)
        return None


def _fetch_charts(host: str, token: str, limit: int) -> list[dict] | None:
    """List workbooks, then fetch element-level SQL for each.

    Returns None if the workbook listing itself failed.
    """
    data = _api_get(host, token, "/workbooks")
    if data is None:
        return None
    if not data:
        return []

    workbooks = data.get("entries", data) if isinstance(data, dict) else data
    if limit > 0:
        workbooks = workbooks[:limit]

    logger.info("Scanning %d Sigma workbooks", len(workbooks))
    charts: list[dict] = []

    for idx, wb in enumerate(workbooks):
        wb_id = wb.get("workbookId", "")
        wb_name = wb.get("name", f"Workbook {wb_id}")
        if not wb_id:
            continue

        elements_data = _api_get(host, token, f"/workbooks/{wb_id}/elements")
        if not elements_data:
            continue

        elements = (
            elements_data.get("entries", elements_data)
            if isinstance(elements_data, dict)
            else elements_data
        )

        for elem in elements:
            elem_id = elem.get("elementId", "")
            elem_name = elem.get("name", "")
            if not elem_id or not elem_name:
                continue

            query_data = _api_get(
                host,
                token,
                f"/workbooks/{wb_id}/elements/{elem_id}/query",
            )
            if not query_data:
                continue

            sql = query_data.get("sql", "")
            if not sql or len(sql) < 50:
                continue

            charts.append(
                {
                    "dashboard_id": wb_id,
                    "dashboard_title": wb_name,
                    "element_id": elem_id,
                    "element_title": elem_name,
                    "sql": sql,
                }
            )

        if (idx + 1) % 10 == 0:
            logger.info(
                "  Processed %d/%d workbooks (%d elements)",
                idx + 1,
                len(workbooks),
                len(charts),
            )

    logger.info("Collected %d elements with SQL from Sigma", len(charts))
    return charts


async def ingest_sigma(config: IngestionConfig) -> IngestionResult:
    """Run the Sigma ingestion pipeline (experimental)."""
    creds = get_credentials("sigma", ["host", "client_id", "client_secret"])
    if not creds:
        logger.error(
            "Sigma credentials not configured. Add 'sigma' section with "
            "host, client_id, and client_secret to ~/.dante/credentials.yaml"
        )
        result = IngestionResult()
        result.errors += 1
        return result

    token = _get_access_token(creds)
    if not token:
        result = IngestionResult()
        result.errors += 1
        return result

    charts = _fetch_charts(creds["host"], token, config.dashboard_limit)
    if charts is None:
        logger.error("Sigma workbook listing failed — check credentials/host")
        result = IngestionResult()
        result.errors += 1
        return result
    if not charts:
        logger.info("No Sigma elements found")
        return IngestionResult()

    return await embed_charts(
        charts, source="sigma", config=config, make_id=_make_id
    )

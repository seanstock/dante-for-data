"""Apache Superset ingestion (experimental).

Fetches dashboards, lists their charts, and extracts SQL from
chart form_data or SQL Lab queries via the Superset REST API.

Requires SUPERSET_URL, SUPERSET_USERNAME, and SUPERSET_PASSWORD
in ~/.dante/credentials.yaml under the `superset` key.
"""

from __future__ import annotations

import json
import logging

import requests

from dante.ingest import IngestionConfig, IngestionResult
from dante.ingest._common import make_embedding_id, get_credentials, embed_charts

logger = logging.getLogger(__name__)


def _make_id(dashboard_id: str, chart_id: str) -> str:
    return make_embedding_id("superset", "ss", dashboard_id, chart_id)


def _authenticate(
    base_url: str, username: str, password: str
) -> requests.Session | None:
    """Authenticate with Superset and return a session with JWT headers."""
    session = requests.Session()
    try:
        resp = session.post(
            f"{base_url}/api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider": "db",
            },
            timeout=15,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            return None
        session.headers["Authorization"] = f"Bearer {token}"
        return session
    except Exception:
        logger.exception("Superset authentication failed")
        return None


def _api_get(
    session: requests.Session, base_url: str, path: str, params: dict | None = None
) -> dict | None:
    try:
        resp = session.get(
            f"{base_url}/api/v1{path}",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Superset API request failed: %s", path, exc_info=True)
        return None


def _fetch_charts(
    session: requests.Session, base_url: str, limit: int
) -> list[dict] | None:
    """List dashboards and extract SQL from each chart.

    Returns None if the dashboard listing itself failed.
    """
    data = _api_get(session, base_url, "/dashboard/", params={"page_size": 200})
    if data is None:
        return None
    if not data:
        return []

    dashboards = data.get("result", [])
    if len(dashboards) >= 200:
        logger.warning(
            "Superset returned a full page (200 dashboards) — the instance "
            "likely has more; results may be incomplete (pagination not yet "
            "implemented)."
        )
    if limit > 0:
        dashboards = dashboards[:limit]

    logger.info("Scanning %d Superset dashboards", len(dashboards))
    charts: list[dict] = []

    for idx, dash in enumerate(dashboards):
        dash_id = str(dash.get("id", ""))
        dash_title = dash.get("dashboard_title", f"Dashboard {dash_id}")

        detail = _api_get(session, base_url, f"/dashboard/{dash_id}/charts")
        if not detail:
            continue

        chart_list = detail.get("result", [])
        for chart in chart_list:
            chart_id = str(chart.get("id", ""))
            chart_name = chart.get("slice_name", "")
            if not chart_name:
                continue

            chart_detail = _api_get(session, base_url, f"/chart/{chart_id}")
            if not chart_detail:
                continue

            result_data = chart_detail.get("result", {})
            params_raw = result_data.get("params", "{}")

            sql = ""
            try:
                form_data = (
                    json.loads(params_raw)
                    if isinstance(params_raw, str)
                    else params_raw
                )
                sql = form_data.get("sql", "") or form_data.get("query", "")
            except Exception:
                pass

            if not sql:
                query_ctx = result_data.get("query_context", {})
                if isinstance(query_ctx, str):
                    try:
                        query_ctx = json.loads(query_ctx)
                    except Exception:
                        query_ctx = {}
                sql = query_ctx.get("query", "")

            if not sql or len(sql) < 50:
                continue

            charts.append(
                {
                    "dashboard_id": dash_id,
                    "dashboard_title": dash_title,
                    "element_id": chart_id,
                    "element_title": chart_name,
                    "sql": sql,
                }
            )

        if (idx + 1) % 10 == 0:
            logger.info(
                "  Processed %d/%d dashboards (%d charts)",
                idx + 1,
                len(dashboards),
                len(charts),
            )

    logger.info("Collected %d charts with SQL from Superset", len(charts))
    return charts


async def ingest_superset(config: IngestionConfig) -> IngestionResult:
    """Run the Superset ingestion pipeline (experimental)."""
    creds = get_credentials("superset", ["url", "username", "password"])
    if not creds:
        logger.error(
            "Superset credentials not configured. Add 'superset' section with "
            "url, username, and password to ~/.dante/credentials.yaml"
        )
        result = IngestionResult()
        result.errors += 1
        return result

    session = _authenticate(creds["url"], creds["username"], creds["password"])
    if not session:
        result = IngestionResult()
        result.errors += 1
        return result

    charts = _fetch_charts(session, creds["url"], config.dashboard_limit)
    if charts is None:
        logger.error("Superset dashboard listing failed — check URL/credentials")
        result = IngestionResult()
        result.errors += 1
        return result
    if not charts:
        logger.info("No Superset charts with SQL found")
        return IngestionResult()

    return await embed_charts(
        charts, source="superset", config=config, make_id=_make_id
    )

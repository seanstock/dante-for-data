"""Databricks Lakeview dashboard ingestion.

Connects to the Databricks workspace REST API to list dashboards,
fetch their serialized definitions, and extract SQL from datasets
and widget configurations.

Requires workspace_url and token in ~/.dante/credentials.yaml
under the `databricks` key.
"""

from __future__ import annotations

import json
import logging
import re

import requests

from dante.ingest import IngestionConfig, IngestionResult
from dante.ingest._common import make_embedding_id, get_credentials, embed_charts

logger = logging.getLogger(__name__)


def _make_id(dashboard_id: str, element_id: str) -> str:
    return make_embedding_id("databricks", "dbr", dashboard_id, element_id)


def _api_get(
    session: requests.Session, base_url: str, path: str, params: dict | None = None
) -> dict | None:
    try:
        resp = session.get(
            f"{base_url}/api/2.0{path}",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Databricks API request failed: %s", path, exc_info=True)
        return None


def _parse_dashboard_charts(
    dash_id: str, dash_name: str, serialized: str
) -> list[dict]:
    """Parse a serialized dashboard definition into chart records."""
    try:
        definition = json.loads(serialized)
    except (json.JSONDecodeError, TypeError):
        return []

    dataset_sql: dict[str, str] = {}
    for ds in definition.get("datasets", []):
        name = ds.get("name") or ds.get("displayName", "")
        lines = ds.get("queryLines", [])
        sql = (
            "\n".join(line.rstrip() for line in lines) if lines else ds.get("query", "")
        )
        if name and sql and len(sql) > 20:
            dataset_sql[name] = sql

    if not dataset_sql:
        return []

    charts: list[dict] = []
    for page in definition.get("pages", []):
        for item in page.get("layout", []):
            widget = item.get("widget", {})
            spec = widget.get("spec", {})

            if spec.get("widgetType", "").startswith("filter"):
                continue

            title = spec.get("frame", {}).get("title", "") or widget.get(
                "displayName", ""
            )
            if not title or title.lower() == "untitled":
                continue

            ds_name = ""
            for wq in widget.get("queries", []):
                ref = wq.get("query", {}).get("datasetName")
                if ref:
                    ds_name = ref
                    break

            sql = dataset_sql.get(ds_name, "")
            if not sql and dataset_sql:
                sql = next(iter(dataset_sql.values()))

            if sql and len(sql) > 20:
                charts.append(
                    {
                        "dashboard_id": dash_id,
                        "dashboard_title": dash_name,
                        "element_id": widget.get("name", title),
                        "element_title": title,
                        "sql": sql,
                    }
                )

    return charts


def _fetch_charts(
    session: requests.Session, base_url: str, limit: int
) -> list[dict] | None:
    """List dashboards and extract SQL from each.

    Returns None if the dashboard listing itself failed.
    """
    data = _api_get(
        session, base_url, "/lakeview/dashboards", params={"page_size": 200}
    )
    if data is None:
        return None
    if not data:
        return []

    dashboards = data.get("dashboards", [])
    if len(dashboards) >= 200:
        logger.warning(
            "Databricks returned a full page (200 dashboards) — the workspace "
            "likely has more; results may be incomplete (pagination not yet "
            "implemented)."
        )
    if limit > 0:
        dashboards = dashboards[:limit]

    logger.info("Scanning %d Databricks dashboards", len(dashboards))
    charts: list[dict] = []

    for idx, dash in enumerate(dashboards):
        dash_id = dash.get("dashboard_id", "")
        dash_name = dash.get("display_name", f"Dashboard {dash_id[:8]}")
        if not dash_id:
            continue

        detail = _api_get(session, base_url, f"/lakeview/dashboards/{dash_id}")
        if not detail:
            continue

        serialized = detail.get("serialized_dashboard", "")
        if serialized:
            new_charts = _parse_dashboard_charts(dash_id, dash_name, serialized)
            for c in new_charts:
                c["dashboard_num"] = idx + 1
                c["total_dashboards"] = len(dashboards)
            charts.extend(new_charts)

        if (idx + 1) % 10 == 0:
            logger.info(
                "  Processed %d/%d dashboards (%d charts)",
                idx + 1,
                len(dashboards),
                len(charts),
            )

    logger.info("Collected %d charts with SQL from Databricks", len(charts))
    return charts


async def ingest_databricks(config: IngestionConfig) -> IngestionResult:
    """Run the Databricks Lakeview ingestion pipeline."""
    creds = get_credentials("databricks", ["workspace_url", "token"])
    if not creds:
        logger.error(
            "Databricks credentials not configured. Add 'databricks' section "
            "with workspace_url and token to ~/.dante/credentials.yaml"
        )
        result = IngestionResult()
        result.errors += 1
        return result

    base_url = creds["workspace_url"].rstrip("/")
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {creds['token']}"

    charts = _fetch_charts(session, base_url, config.dashboard_limit)
    if charts is None:
        logger.error("Databricks dashboard listing failed — check workspace URL/token")
        result = IngestionResult()
        result.errors += 1
        return result
    if not charts:
        logger.info("No Databricks charts found")
        return IngestionResult()

    return await embed_charts(
        charts,
        source="databricks",
        config=config,
        make_id=_make_id,
        sql_transform=lambda s: re.sub(r"\n{2,}", "\n", s),
    )

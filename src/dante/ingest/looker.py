"""Looker dashboard ingestion for embedding generation.

Connects to Looker via the SDK, discovers actively-used dashboards
through System Activity, extracts chart SQL, and creates embeddings.
"""

from __future__ import annotations

import json
import logging

from dante.ingest import IngestionConfig, IngestionResult
from dante.ingest._common import make_embedding_id, embed_charts

logger = logging.getLogger(__name__)


def _make_id(dashboard_id: str, element_id: str) -> str:
    return make_embedding_id("looker", "lkr", dashboard_id, element_id)


def _init_sdk():
    """Initialize the Looker SDK using stored Dante credentials."""
    try:
        import looker_sdk
        from looker_sdk.sdk.api40 import methods as methods40  # noqa: F401
    except ImportError:
        logger.error("looker-sdk not installed. Run: pip install dante-ds[looker]")
        return None

    from dante.ingest._common import get_credentials

    creds = get_credentials("looker", ["base_url"])
    if creds is None:
        logger.error(
            "Looker credentials not configured. "
            "Run 'dante ui' to set base_url, client_id, and client_secret."
        )
        return None

    try:
        sdk = looker_sdk.init40(
            config_settings={
                "base_url": creds["base_url"],
                "client_id": creds.get("client_id", ""),
                "client_secret": creds.get("client_secret", ""),
            }
        )
        return sdk
    except Exception:
        logger.exception("Failed to initialize Looker SDK")
        return None


def _query_dashboard_usage(sdk, lookback_days: int, min_views: int) -> dict[str, int]:
    """Query Looker System Activity for per-dashboard view counts."""
    try:
        query = sdk.create_query(
            body={
                "model": "i__looker",
                "view": "history",
                "fields": ["dashboard.id", "history.query_run_count"],
                "filters": {
                    "history.created_date": f"{lookback_days} days",
                    "dashboard.id": "NOT NULL",
                },
                "sorts": ["history.query_run_count desc"],
                "limit": "5000",
            }
        )
        raw = sdk.run_query(query_id=query.id, result_format="json")
        rows = json.loads(raw)
    except Exception:
        logger.exception("Failed to query Looker System Activity")
        return {}

    usage: dict[str, int] = {}
    for row in rows:
        did = str(row.get("dashboard.id", ""))
        views = row.get("history.query_run_count", 0) or 0
        if did:
            usage[did] = usage.get(did, 0) + views

    return {k: v for k, v in usage.items() if v >= min_views}


def _fetch_charts(
    sdk, dashboard_ids: set[str], usage: dict[str, int], limit: int
) -> list[dict]:
    """Fetch chart elements and their generated SQL from dashboards."""
    if limit > 0:
        dashboard_ids = set(list(dashboard_ids)[:limit])

    logger.info("Scanning %d Looker dashboards for charts", len(dashboard_ids))
    charts: list[dict] = []

    for idx, did in enumerate(dashboard_ids):
        try:
            dash = sdk.dashboard(dashboard_id=did, fields="id,title,folder")
            elements = sdk.dashboard_dashboard_elements(
                dashboard_id=did,
                fields="id,title,query,result_maker",
            )

            for elem in elements:
                title = elem.title or ""
                if not title or title.lower() == "untitled":
                    continue

                qobj = elem.query or (
                    elem.result_maker.query if elem.result_maker else None
                )
                if not qobj:
                    continue

                try:
                    sql = sdk.run_query(query_id=qobj.id, result_format="sql")
                except Exception:
                    continue

                if not sql or len(sql) < 50:
                    continue

                charts.append(
                    {
                        "dashboard_id": did,
                        "dashboard_title": dash.title or f"Dashboard {did}",
                        "element_id": str(elem.id),
                        "element_title": title,
                        "sql": sql,
                        "view_count": usage.get(did, 0),
                    }
                )

            if (idx + 1) % 10 == 0:
                logger.info(
                    "  Processed %d/%d dashboards (%d charts)",
                    idx + 1,
                    len(dashboard_ids),
                    len(charts),
                )
        except Exception:
            logger.warning("Error on dashboard %s", did, exc_info=True)

    logger.info("Collected %d charts with SQL", len(charts))
    return charts


async def ingest_looker(config: IngestionConfig) -> IngestionResult:
    """Run the Looker ingestion pipeline."""
    sdk = _init_sdk()
    if sdk is None:
        result = IngestionResult()
        result.errors += 1
        return result

    usage = _query_dashboard_usage(sdk, config.lookback_days, config.min_views)
    if not usage:
        logger.info("No dashboards meet the usage threshold")
        return IngestionResult()

    logger.info("Found %d dashboards with >= %d views", len(usage), config.min_views)

    charts = _fetch_charts(sdk, set(usage.keys()), usage, config.dashboard_limit)
    if not charts:
        logger.info("No charts with SQL found")
        return IngestionResult()

    return await embed_charts(
        charts, source="looker", config=config, make_id=_make_id
    )

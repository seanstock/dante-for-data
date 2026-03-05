"""Databricks Lakeview dashboard ingestion.

Connects to the Databricks workspace REST API to list dashboards,
fetch their serialized definitions, and extract SQL from datasets
and widget configurations.

Requires workspace_url and token in ~/.dante/credentials.yaml
under the `databricks` key.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time

import requests

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


def _make_embedding_id(dashboard_id: str, element_id: str) -> str:
    raw = f"databricks:{dashboard_id}:{element_id}"
    return f"dbr-{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def _get_credentials() -> dict | None:
    from dante.config import load_global_credentials

    creds = load_global_credentials().get("databricks", {})
    if not creds.get("workspace_url") or not creds.get("token"):
        return None
    return creds


def _api_get(session: requests.Session, base_url: str, path: str,
             params: dict | None = None) -> dict | None:
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


def _parse_dashboard_charts(dash_id: str, dash_name: str,
                            serialized: str) -> list[dict]:
    """Parse a serialized dashboard definition into chart records."""
    try:
        definition = json.loads(serialized)
    except (json.JSONDecodeError, TypeError):
        return []

    # Build dataset name -> SQL lookup
    dataset_sql: dict[str, str] = {}
    for ds in definition.get("datasets", []):
        name = ds.get("name") or ds.get("displayName", "")
        lines = ds.get("queryLines", [])
        sql = "\n".join(l.rstrip() for l in lines) if lines else ds.get("query", "")
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

            # Resolve dataset reference
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
                charts.append({
                    "dashboard_id": dash_id,
                    "dashboard_title": dash_name,
                    "element_id": widget.get("name", title),
                    "element_title": title,
                    "sql": sql,
                })

    return charts


def _fetch_charts(session: requests.Session, base_url: str,
                  limit: int) -> list[dict]:
    """List dashboards and extract SQL from each."""
    data = _api_get(session, base_url, "/lakeview/dashboards",
                    params={"page_size": 200})
    if not data:
        return []

    dashboards = data.get("dashboards", [])
    if limit > 0:
        dashboards = dashboards[:limit]

    logger.info("Scanning %d Databricks dashboards", len(dashboards))
    charts: list[dict] = []

    for idx, dash in enumerate(dashboards):
        dash_id = dash.get("dashboard_id", "")
        dash_name = dash.get("display_name", f"Dashboard {dash_id[:8]}")
        if not dash_id:
            continue

        detail = _api_get(session, base_url,
                          f"/lakeview/dashboards/{dash_id}")
        if not detail:
            continue

        serialized = detail.get("serialized_dashboard", "")
        if serialized:
            charts.extend(_parse_dashboard_charts(dash_id, dash_name, serialized))

        if (idx + 1) % 10 == 0:
            logger.info("  Processed %d/%d dashboards (%d charts)",
                        idx + 1, len(dashboards), len(charts))

    logger.info("Collected %d charts with SQL from Databricks", len(charts))
    return charts


async def ingest_databricks(config: IngestionConfig) -> IngestionResult:
    """Run the Databricks Lakeview ingestion pipeline."""
    result = IngestionResult()

    creds = _get_credentials()
    if not creds:
        logger.error(
            "Databricks credentials not configured. Add 'databricks' section "
            "with workspace_url and token to ~/.dante/credentials.yaml"
        )
        result.errors += 1
        return result

    base_url = creds["workspace_url"].rstrip("/")
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {creds['token']}"

    charts = _fetch_charts(session, base_url, config.dashboard_limit)
    if not charts:
        logger.info("No Databricks charts found")
        return result

    from dante.config import knowledge_dir
    from dante.ingest.question_gen import generate_question
    from dante.ingest.sql_simplifier import simplify_sql
    from dante.knowledge.embeddings import init_db, upsert
    from dante.knowledge.vectorize import generate_embedding

    db_path = knowledge_dir() / "embeddings.db"
    conn = init_db(db_path)

    for idx, chart in enumerate(charts):
        title = chart["element_title"]
        emb_id = _make_embedding_id(chart["dashboard_id"], chart["element_id"])

        if config.dry_run:
            question = generate_question(title, chart["dashboard_title"])
            logger.info("[%d/%d] %s -> %s", idx + 1, len(charts), title, question)
            result.skipped += 1
            continue

        try:
            question = generate_question(title, chart["dashboard_title"])
            simplified = await simplify_sql(chart["sql"], title)
            simplified = re.sub(r'\n{2,}', '\n', simplified)
            embed_text = f"Question: {question}\nSQL Pattern:\n{simplified[:2000]}"
            vector = await generate_embedding(embed_text)

            upsert(
                conn=conn,
                id=emb_id,
                question=question,
                sql=simplified,
                source="databricks",
                dashboard=chart["dashboard_title"],
                description=title,
                embedding_vector=vector,
            )
            result.created += 1

        except Exception:
            logger.warning("Failed to process chart '%s'", title, exc_info=True)
            result.errors += 1

        if (idx + 1) % 10 == 0:
            time.sleep(0.5)

    conn.close()
    return result

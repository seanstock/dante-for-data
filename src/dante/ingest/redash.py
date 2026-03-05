"""Redash ingestion (experimental).

Fetches dashboards and their underlying queries from the Redash API.
Redash is SQL-first: every query object has a `query` field with the
full SQL text.

Requires REDASH_URL and REDASH_API_KEY in ~/.dante/credentials.yaml
under the `redash` key.
"""

from __future__ import annotations

import hashlib
import logging
import time

import requests

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


def _make_embedding_id(dashboard_id: str, query_id: str) -> str:
    raw = f"redash:{dashboard_id}:{query_id}"
    return f"rds-{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def _get_credentials() -> dict | None:
    from dante.config import load_global_credentials

    creds = load_global_credentials().get("redash", {})
    if not creds.get("url") or not creds.get("api_key"):
        return None
    return creds


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


def _fetch_charts(base_url: str, api_key: str, limit: int) -> list[dict]:
    """List dashboards and collect the SQL from each widget's query."""
    data = _api_get(base_url, api_key, "/dashboards")
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

            # Avoid duplicates when the same query appears on multiple dashboards
            if q_id in seen_queries:
                continue
            seen_queries.add(q_id)

            charts.append({
                "dashboard_id": slug,
                "dashboard_title": dash_name,
                "element_id": q_id,
                "element_title": q_name,
                "sql": sql,
            })

        if (idx + 1) % 10 == 0:
            logger.info("  Processed %d/%d dashboards (%d queries)",
                        idx + 1, len(dashboards), len(charts))

    logger.info("Collected %d queries with SQL from Redash", len(charts))
    return charts


async def ingest_redash(config: IngestionConfig) -> IngestionResult:
    """Run the Redash ingestion pipeline (experimental)."""
    result = IngestionResult()

    creds = _get_credentials()
    if not creds:
        logger.error(
            "Redash credentials not configured. Add 'redash' section with "
            "url and api_key to ~/.dante/credentials.yaml"
        )
        result.errors += 1
        return result

    charts = _fetch_charts(creds["url"], creds["api_key"], config.dashboard_limit)
    if not charts:
        logger.info("No Redash queries found")
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
            embed_text = f"Question: {question}\nSQL Pattern:\n{simplified[:2000]}"
            vector = await generate_embedding(embed_text)

            upsert(
                conn=conn,
                id=emb_id,
                question=question,
                sql=simplified,
                source="redash",
                dashboard=chart["dashboard_title"],
                description=title,
                embedding_vector=vector,
            )
            result.created += 1

        except Exception:
            logger.warning("Failed on query '%s'", title, exc_info=True)
            result.errors += 1

        if (idx + 1) % 10 == 0:
            time.sleep(0.5)

    conn.close()
    return result

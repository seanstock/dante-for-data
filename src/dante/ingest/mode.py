"""Mode Analytics ingestion (experimental).

Fetches reports and their SQL queries from the Mode API.
Mode is SQL-first: every query object has a `raw_query` field
containing the full SQL text.

Requires MODE_TOKEN and MODE_SECRET in ~/.dante/credentials.yaml
under the `mode` key, plus a `workspace` field.
"""

from __future__ import annotations

import hashlib
import logging
import time

import requests

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)

_API_BASE = "https://app.mode.com/api"


def _make_embedding_id(report_token: str, query_token: str) -> str:
    raw = f"mode:{report_token}:{query_token}"
    return f"mode-{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def _get_credentials() -> dict | None:
    """Load Mode credentials from Dante config."""
    from dante.config import load_global_credentials

    creds = load_global_credentials().get("mode", {})
    if not creds.get("token") or not creds.get("secret") or not creds.get("workspace"):
        return None
    return creds


def _api_get(session: requests.Session, path: str) -> dict | list | None:
    """Make an authenticated GET request to the Mode API."""
    try:
        resp = session.get(f"{_API_BASE}{path}", timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Mode API request failed: %s", path, exc_info=True)
        return None


def _fetch_charts(session: requests.Session, workspace: str,
                  limit: int) -> list[dict]:
    """List reports and extract query SQL from each."""
    data = _api_get(session, f"/{workspace}/reports")
    if not data:
        return []

    reports = data if isinstance(data, list) else data.get("_embedded", {}).get("reports", [])
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

        queries = (queries_data if isinstance(queries_data, list)
                   else queries_data.get("_embedded", {}).get("queries", []))

        for q in queries:
            sql = q.get("raw_query", "")
            q_name = q.get("name", "")
            q_token = q.get("token", "")

            if not sql or len(sql) < 50 or not q_name:
                continue

            charts.append({
                "dashboard_id": token,
                "dashboard_title": report_name,
                "element_id": q_token,
                "element_title": q_name,
                "sql": sql,
            })

        if (idx + 1) % 10 == 0:
            logger.info("  Processed %d/%d reports (%d queries)",
                        idx + 1, len(reports), len(charts))

    logger.info("Collected %d queries with SQL from Mode", len(charts))
    return charts


async def ingest_mode(config: IngestionConfig) -> IngestionResult:
    """Run the Mode ingestion pipeline (experimental)."""
    result = IngestionResult()

    creds = _get_credentials()
    if not creds:
        logger.error(
            "Mode credentials not configured. Add 'mode' section with "
            "token, secret, and workspace to ~/.dante/credentials.yaml"
        )
        result.errors += 1
        return result

    session = requests.Session()
    session.auth = (creds["token"], creds["secret"])
    session.headers["Accept"] = "application/json"

    charts = _fetch_charts(session, creds["workspace"], config.dashboard_limit)
    if not charts:
        logger.info("No Mode queries found")
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
                source="mode",
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

"""Sigma Computing ingestion (experimental).

Fetches workbooks and extracts the SQL for each element using
Sigma's dedicated query endpoints.

Requires SIGMA_HOST, SIGMA_CLIENT_ID, and SIGMA_CLIENT_SECRET
in ~/.dante/credentials.yaml under the `sigma` key.
"""

from __future__ import annotations

import hashlib
import logging
import time

import requests

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


def _make_embedding_id(workbook_id: str, element_id: str) -> str:
    raw = f"sigma:{workbook_id}:{element_id}"
    return f"sig-{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def _get_credentials() -> dict | None:
    from dante.config import load_global_credentials

    creds = load_global_credentials().get("sigma", {})
    if not creds.get("host") or not creds.get("client_id") or not creds.get("client_secret"):
        return None
    return creds


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


def _fetch_charts(host: str, token: str, limit: int) -> list[dict]:
    """List workbooks, then fetch element-level SQL for each."""
    data = _api_get(host, token, "/workbooks")
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

        # List elements in the workbook
        elements_data = _api_get(host, token, f"/workbooks/{wb_id}/elements")
        if not elements_data:
            continue

        elements = (elements_data.get("entries", elements_data)
                    if isinstance(elements_data, dict) else elements_data)

        for elem in elements:
            elem_id = elem.get("elementId", "")
            elem_name = elem.get("name", "")
            if not elem_id or not elem_name:
                continue

            # Fetch the SQL for this specific element
            query_data = _api_get(
                host, token,
                f"/workbooks/{wb_id}/elements/{elem_id}/query",
            )
            if not query_data:
                continue

            sql = query_data.get("sql", "")
            if not sql or len(sql) < 50:
                continue

            charts.append({
                "dashboard_id": wb_id,
                "dashboard_title": wb_name,
                "element_id": elem_id,
                "element_title": elem_name,
                "sql": sql,
            })

        if (idx + 1) % 10 == 0:
            logger.info("  Processed %d/%d workbooks (%d elements)",
                        idx + 1, len(workbooks), len(charts))

    logger.info("Collected %d elements with SQL from Sigma", len(charts))
    return charts


async def ingest_sigma(config: IngestionConfig) -> IngestionResult:
    """Run the Sigma ingestion pipeline (experimental)."""
    result = IngestionResult()

    creds = _get_credentials()
    if not creds:
        logger.error(
            "Sigma credentials not configured. Add 'sigma' section with "
            "host, client_id, and client_secret to ~/.dante/credentials.yaml"
        )
        result.errors += 1
        return result

    token = _get_access_token(creds)
    if not token:
        result.errors += 1
        return result

    charts = _fetch_charts(creds["host"], token, config.dashboard_limit)
    if not charts:
        logger.info("No Sigma elements found")
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
                source="sigma",
                dashboard=chart["dashboard_title"],
                description=title,
                embedding_vector=vector,
            )
            result.created += 1

        except Exception:
            logger.warning("Failed on element '%s'", title, exc_info=True)
            result.errors += 1

        if (idx + 1) % 10 == 0:
            time.sleep(0.5)

    conn.close()
    return result

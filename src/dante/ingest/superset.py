"""Apache Superset ingestion (experimental).

Fetches dashboards, lists their charts, and extracts SQL from
chart form_data or SQL Lab queries via the Superset REST API.

Requires SUPERSET_URL, SUPERSET_USERNAME, and SUPERSET_PASSWORD
in ~/.dante/credentials.yaml under the `superset` key.
"""

from __future__ import annotations

import hashlib
import logging
import time

import requests

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


def _make_embedding_id(dashboard_id: str, chart_id: str) -> str:
    raw = f"superset:{dashboard_id}:{chart_id}"
    return f"ss-{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def _get_credentials() -> dict | None:
    from dante.config import load_global_credentials

    creds = load_global_credentials().get("superset", {})
    if not creds.get("url") or not creds.get("username") or not creds.get("password"):
        return None
    return creds


def _authenticate(base_url: str, username: str, password: str) -> requests.Session | None:
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


def _api_get(session: requests.Session, base_url: str, path: str,
             params: dict | None = None) -> dict | None:
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


def _fetch_charts(session: requests.Session, base_url: str,
                  limit: int) -> list[dict]:
    """List dashboards and extract SQL from each chart."""
    data = _api_get(session, base_url, "/dashboard/",
                    params={"page_size": 200})
    if not data:
        return []

    dashboards = data.get("result", [])
    if limit > 0:
        dashboards = dashboards[:limit]

    logger.info("Scanning %d Superset dashboards", len(dashboards))
    charts: list[dict] = []

    for idx, dash in enumerate(dashboards):
        dash_id = str(dash.get("id", ""))
        dash_title = dash.get("dashboard_title", f"Dashboard {dash_id}")

        # Get charts on this dashboard
        detail = _api_get(session, base_url, f"/dashboard/{dash_id}/charts")
        if not detail:
            continue

        chart_list = detail.get("result", [])
        for chart in chart_list:
            chart_id = str(chart.get("id", ""))
            chart_name = chart.get("slice_name", "")
            if not chart_name:
                continue

            # Fetch full chart detail to get form_data
            chart_detail = _api_get(session, base_url, f"/chart/{chart_id}")
            if not chart_detail:
                continue

            result_data = chart_detail.get("result", {})
            params_raw = result_data.get("params", "{}")

            # Extract SQL: check for direct SQL in form_data
            sql = ""
            try:
                import json
                form_data = json.loads(params_raw) if isinstance(params_raw, str) else params_raw
                sql = form_data.get("sql", "") or form_data.get("query", "")
            except Exception:
                pass

            # Also check the datasource query for SQL Lab charts
            if not sql:
                query_ctx = result_data.get("query_context", {})
                if isinstance(query_ctx, str):
                    try:
                        import json
                        query_ctx = json.loads(query_ctx)
                    except Exception:
                        query_ctx = {}
                sql = query_ctx.get("query", "")

            if not sql or len(sql) < 50:
                continue

            charts.append({
                "dashboard_id": dash_id,
                "dashboard_title": dash_title,
                "element_id": chart_id,
                "element_title": chart_name,
                "sql": sql,
            })

        if (idx + 1) % 10 == 0:
            logger.info("  Processed %d/%d dashboards (%d charts)",
                        idx + 1, len(dashboards), len(charts))

    logger.info("Collected %d charts with SQL from Superset", len(charts))
    return charts


async def ingest_superset(config: IngestionConfig) -> IngestionResult:
    """Run the Superset ingestion pipeline (experimental)."""
    result = IngestionResult()

    creds = _get_credentials()
    if not creds:
        logger.error(
            "Superset credentials not configured. Add 'superset' section with "
            "url, username, and password to ~/.dante/credentials.yaml"
        )
        result.errors += 1
        return result

    session = _authenticate(creds["url"], creds["username"], creds["password"])
    if not session:
        result.errors += 1
        return result

    charts = _fetch_charts(session, creds["url"], config.dashboard_limit)
    if not charts:
        logger.info("No Superset charts with SQL found")
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
                source="superset",
                dashboard=chart["dashboard_title"],
                description=title,
                embedding_vector=vector,
            )
            result.created += 1

        except Exception:
            logger.warning("Failed on chart '%s'", title, exc_info=True)
            result.errors += 1

        if (idx + 1) % 10 == 0:
            time.sleep(0.5)

    conn.close()
    return result

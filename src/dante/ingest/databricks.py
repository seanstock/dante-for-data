"""Databricks Lakeview dashboard ingestion via the Databricks CLI.

Lists Lakeview dashboards, parses serialized dashboard JSON to extract
dataset queries and widget titles, then generates embeddings.
"""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import time

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


def _make_embedding_id(dashboard_id: str, element_id: str) -> str:
    """Deterministic embedding ID for a Databricks chart."""
    raw = f"databricks:{dashboard_id}:{element_id}"
    return f"dbr-{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def _run_cli(*args: str, timeout: int = 60) -> dict | list | None:
    """Run a databricks CLI command and parse JSON output."""
    cmd = ["databricks", *args, "--output", "json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        logger.error(
            "'databricks' CLI not found. "
            "Install with: pip install databricks-cli"
        )
        return None
    except subprocess.TimeoutExpired:
        logger.warning("CLI command timed out: %s", " ".join(cmd))
        return None

    if proc.returncode != 0:
        logger.warning("CLI error: %s", proc.stderr.strip())
        return None

    try:
        return json.loads(proc.stdout) if proc.stdout else None
    except json.JSONDecodeError:
        return None


def _extract_charts_from_dashboard(dash_id: str, dash_name: str) -> list[dict]:
    """Fetch a single dashboard and extract widget titles + SQL from it."""
    data = _run_cli("lakeview", "get", dash_id, timeout=30)
    if not data or not isinstance(data, dict):
        return []

    serialized = data.get("serialized_dashboard")
    if not serialized:
        return []

    try:
        definition = json.loads(serialized)
    except (json.JSONDecodeError, TypeError):
        return []

    # Build a lookup of dataset name -> SQL query
    dataset_sql: dict[str, str] = {}
    for ds in definition.get("datasets", []):
        name = ds.get("name") or ds.get("displayName", "")
        lines = ds.get("queryLines", [])
        sql = "\n".join(lines) if lines else ds.get("query", "")
        if name and sql and len(sql) > 20:
            dataset_sql[name] = sql

    if not dataset_sql:
        return []

    # Walk pages -> layout items -> widgets
    charts: list[dict] = []
    for page in definition.get("pages", []):
        for item in page.get("layout", []):
            widget = item.get("widget", {})
            spec = widget.get("spec", {})

            # Skip filter widgets
            widget_type = spec.get("widgetType", "")
            if widget_type.startswith("filter"):
                continue

            # Title lives in spec.frame.title
            title = spec.get("frame", {}).get("title", "") or widget.get(
                "displayName", ""
            )
            if not title or title.lower() == "untitled":
                continue

            # Resolve which dataset this widget references
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


def _fetch_all_charts(limit: int) -> list[dict]:
    """List Lakeview dashboards and extract charts from each."""
    listing = _run_cli("lakeview", "list")
    if not listing or not isinstance(listing, list):
        return []

    total = len(listing)
    if limit > 0:
        listing = listing[:limit]

    logger.info(
        "Found %d Lakeview dashboards, processing %d", total, len(listing)
    )

    all_charts: list[dict] = []
    for idx, dash in enumerate(listing):
        did = dash.get("dashboard_id", "")
        name = dash.get("display_name", f"Dashboard {did[:8]}" if did else "Unknown")
        if not did:
            continue

        all_charts.extend(_extract_charts_from_dashboard(did, name))

        if (idx + 1) % 10 == 0:
            logger.info(
                "  Processed %d/%d dashboards (%d charts)",
                idx + 1, len(listing), len(all_charts),
            )

    logger.info("Collected %d charts with SQL from Databricks", len(all_charts))
    return all_charts


async def ingest_databricks(config: IngestionConfig) -> IngestionResult:
    """Run the full Databricks Lakeview ingestion pipeline."""
    result = IngestionResult()

    charts = _fetch_all_charts(config.dashboard_limit)
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

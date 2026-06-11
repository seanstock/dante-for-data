"""Shared ingestion utilities — deduplicates the embed loop across connectors."""

from __future__ import annotations

import asyncio
import hashlib
import logging

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)

# Abort a run after this many consecutive chart failures — a systemic problem
# (bad API key, network outage) fails every chart identically, so there is no
# point grinding through the whole list.
_MAX_CONSECUTIVE_FAILURES = 5


def make_embedding_id(platform: str, prefix: str, id_a: str, id_b: str) -> str:
    """Deterministic embedding ID for any platform chart.

    Args:
        platform: Source identifier used in the hash (e.g. "looker", "mode").
        prefix: Short prefix for the ID (e.g. "lkr", "mode", "ss").
        id_a: First component (dashboard/report ID).
        id_b: Second component (element/query ID).
    """
    raw = f"{platform}:{id_a}:{id_b}"
    return f"{prefix}-{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def get_credentials(platform: str, required_keys: list[str]) -> dict | None:
    """Load credentials for *platform* from ~/.dante/credentials.yaml.

    Returns the credential dict if all *required_keys* are present,
    otherwise returns None.
    """
    from dante.config import load_global_credentials

    creds = load_global_credentials().get(platform, {})
    if not all(creds.get(k) for k in required_keys):
        return None
    return creds


async def embed_charts(
    charts: list[dict],
    *,
    source: str,
    config: IngestionConfig,
    make_id: callable,
    sql_transform: callable | None = None,
) -> IngestionResult:
    """Process a list of chart dicts: generate questions, simplify SQL,
    create embeddings, and upsert into the local index.

    Each chart dict must have keys:
        dashboard_id, dashboard_title, element_id, element_title, sql

    Args:
        charts: List of chart records from a connector's _fetch_charts.
        source: Source label for the embedding (e.g. "looker").
        config: Ingestion configuration (for dry_run, progress_callback).
        make_id: Callable(dashboard_id, element_id) -> embedding ID string.
        sql_transform: Optional callable(simplified_sql) -> str for
            post-processing (e.g. collapsing blank lines).
    """
    from dante.config import knowledge_dir
    from dante.ingest.question_gen import generate_question
    from dante.ingest.sql_simplifier import simplify_sql
    from dante.knowledge.embeddings import init_db, upsert
    from dante.knowledge.vectorize import generate_embedding

    result = IngestionResult()
    db_path = knowledge_dir() / "embeddings.db"
    conn = init_db(db_path)

    consecutive_failures = 0
    try:
        for idx, chart in enumerate(charts):
            title = chart["element_title"]
            emb_id = make_id(chart["dashboard_id"], chart["element_id"])

            if config.dry_run:
                question = generate_question(title, chart["dashboard_title"])
                logger.info("[%d/%d] %s -> %s", idx + 1, len(charts), title, question)
                result.skipped += 1
                continue

            try:
                question = generate_question(title, chart["dashboard_title"])
                simplified = await simplify_sql(chart["sql"], title)
                if sql_transform:
                    simplified = sql_transform(simplified)
                embed_text = f"Question: {question}\nSQL Pattern:\n{simplified[:2000]}"
                vector = await generate_embedding(embed_text)

                upsert(
                    conn=conn,
                    id=emb_id,
                    question=question,
                    sql=simplified,
                    source=source,
                    dashboard=chart["dashboard_title"],
                    description=title,
                    embedding_vector=vector,
                )
                result.created += 1
                consecutive_failures = 0

            except Exception as e:
                logger.warning("Failed on chart '%s'", title, exc_info=True)
                result.errors += 1
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    logger.error(
                        "Aborting ingestion after %d consecutive failures; last "
                        "error: %s. Check credentials/connectivity.",
                        consecutive_failures, e,
                    )
                    result.aborted = True
                    break

            if config.progress_callback:
                config.progress_callback(
                    f"{chart['dashboard_title']} | {title}",
                    chart.get("dashboard_num", idx + 1),
                    chart.get("total_dashboards", len(charts)),
                )

            if (idx + 1) % 10 == 0:
                # Gentle rate-limit — await, don't block the event loop.
                await asyncio.sleep(0.5)
    finally:
        conn.close()

    return result

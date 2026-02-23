"""Looker SDK integration for dashboard/chart ingestion."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


async def ingest_looker(config: IngestionConfig) -> IngestionResult:
    """Ingest embeddings from Looker dashboards.

    Connects via Looker SDK, scans dashboards with min_views threshold,
    extracts chart titles and underlying SQL, generates questions,
    simplifies SQL, creates embeddings, and upserts into the local index.
    """
    result = IngestionResult()

    try:
        import looker_sdk
    except ImportError:
        logger.error("looker-sdk not installed. Run: pip install dante-lib[looker]")
        result.errors += 1
        return result

    from dante.config import load_global_credentials, project_dir
    from dante.ingest.question_gen import generate_question
    from dante.ingest.sql_simplifier import simplify_sql

    creds = load_global_credentials()
    looker_creds = creds.get("looker", {})

    if not looker_creds.get("base_url"):
        logger.error("Looker credentials not configured. Run 'dante ui' to set up.")
        result.errors += 1
        return result

    try:
        sdk = looker_sdk.init40(config_settings={
            "base_url": looker_creds["base_url"],
            "client_id": looker_creds.get("client_id", ""),
            "client_secret": looker_creds.get("client_secret", ""),
        })

        # Get dashboards
        dashboards = sdk.all_dashboards(fields="id,title,view_count,last_accessed_at")

        # Filter by min_views and lookback_days
        cutoff = datetime.now(timezone.utc) - timedelta(days=config.lookback_days)
        filtered = []
        for d in dashboards:
            if d.view_count and d.view_count >= config.min_views:
                if d.last_accessed_at and d.last_accessed_at >= cutoff:
                    filtered.append(d)

        if config.dashboard_limit > 0:
            filtered = filtered[:config.dashboard_limit]

        logger.info("Found %d Looker dashboards matching filters", len(filtered))

        for dashboard in filtered:
            try:
                full_dash = sdk.dashboard(dashboard.id, fields="dashboard_elements")
                for element in (full_dash.dashboard_elements or []):
                    if not element.title:
                        continue

                    # Get the SQL from the element's query
                    sql = ""
                    if element.query_id:
                        try:
                            query = sdk.query(element.query_id)
                            sql = query.sql or ""
                        except Exception:
                            pass

                    if not sql:
                        result.skipped += 1
                        continue

                    # Generate question from title
                    question = generate_question(element.title, dashboard.title or "")

                    # Simplify SQL
                    simplified = await simplify_sql(sql, element.title)

                    # Compute deterministic ID
                    det_id = _compute_id("looker", str(dashboard.id), str(element.id))

                    if config.dry_run:
                        logger.info("Would ingest: %s → %s", element.title, question)
                        result.skipped += 1
                        continue

                    # Generate embedding and upsert
                    from dante.knowledge.vectorize import generate_embedding
                    from dante.knowledge.embeddings import upsert, init_db

                    db_path = project_dir() / "embeddings.db"
                    init_db(db_path)

                    embed_text = f"Question: {question}\nSQL Pattern: {simplified}"
                    embedding = await generate_embedding(embed_text)

                    was_update = upsert(
                        db_path=db_path,
                        id=det_id,
                        question=question,
                        sql=simplified,
                        source="looker",
                        dashboard=dashboard.title or "",
                        description=element.title,
                        embedding=embedding,
                    )

                    if was_update:
                        result.updated += 1
                    else:
                        result.created += 1

            except Exception as e:
                logger.warning("Error processing dashboard %s: %s", dashboard.id, e)
                result.errors += 1

    except Exception as e:
        logger.error("Looker ingestion failed: %s", e)
        result.errors += 1

    return result


def _compute_id(source: str, dashboard_id: str, entity_id: str) -> str:
    """Compute a deterministic ID for an embedding."""
    raw = f"{source}:{dashboard_id}:{entity_id}"
    hash_hex = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"lkr-{hash_hex}"

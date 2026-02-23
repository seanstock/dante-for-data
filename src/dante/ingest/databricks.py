"""Databricks Lakeview dashboard ingestion."""

from __future__ import annotations

import hashlib
import logging

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


async def ingest_databricks(config: IngestionConfig) -> IngestionResult:
    """Ingest embeddings from Databricks Lakeview dashboards.

    TODO: Full implementation requires Databricks workspace API access.
    Currently a placeholder that logs the intent.
    """
    result = IngestionResult()
    logger.info("Databricks ingestion not yet fully implemented")
    logger.info("Will scan Lakeview dashboards for chart titles and SQL")
    return result


def _compute_id(dashboard_id: str, entity_id: str) -> str:
    raw = f"databricks:{dashboard_id}:{entity_id}"
    hash_hex = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"dbr-{hash_hex}"

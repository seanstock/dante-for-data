"""Placeholder ingestion for platforms with planned support.

These platforms have APIs capable of SQL extraction but
integrations have not been built yet.
"""

from __future__ import annotations

import logging

from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)

_PLANNED = {
    "metabase": "Metabase (native SQL via dataset_query, MBQL conversion via /api/dataset/native)",
    "hex": "Hex (SQL cell source code via project cell listings)",
    "thoughtspot": "ThoughtSpot (generated SQL via dedicated answer/liveboard endpoints)",
    "tableau": "Tableau (custom SQL via Metadata API / GraphQL)",
}


async def ingest_coming_soon(source: str, config: IngestionConfig) -> IngestionResult:
    """Log that a source is planned but not yet available."""
    result = IngestionResult()
    desc = _PLANNED.get(source, source)
    logger.info(
        "%s ingestion is coming soon. "
        "If you'd like to contribute, see the existing scrapers for the pattern.",
        desc,
    )
    return result

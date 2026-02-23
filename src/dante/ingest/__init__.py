"""Embedding ingestion pipeline.

Ingests chart titles + SQL from BI platforms, generates questions,
simplifies SQL, creates embeddings, and upserts into the local index.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IngestionConfig:
    """Configuration for an ingestion run."""
    sources: list[str] = field(default_factory=lambda: ["all"])
    min_views: int = 10
    lookback_days: int = 90
    dashboard_limit: int = 0  # 0 = unlimited
    skip_existing: bool = False
    dry_run: bool = False


@dataclass
class IngestionResult:
    """Results from an ingestion run."""
    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0

    def to_dict(self) -> dict:
        return {
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
        }


async def run(config: IngestionConfig) -> IngestionResult:
    """Run the full ingestion pipeline.

    Args:
        config: Ingestion configuration.

    Returns:
        IngestionResult with counts.
    """
    result = IngestionResult()
    sources = config.sources

    if "all" in sources:
        sources = ["looker", "databricks", "warehouse"]

    for source in sources:
        if source == "looker":
            from dante.ingest.looker import ingest_looker
            r = await ingest_looker(config)
            _merge_results(result, r)
        elif source == "databricks":
            from dante.ingest.databricks import ingest_databricks
            r = await ingest_databricks(config)
            _merge_results(result, r)
        elif source == "warehouse":
            from dante.ingest.warehouse import ingest_warehouse
            r = await ingest_warehouse(config)
            _merge_results(result, r)

    return result


def _merge_results(target: IngestionResult, source: IngestionResult):
    target.created += source.created
    target.updated += source.updated
    target.skipped += source.skipped
    target.errors += source.errors

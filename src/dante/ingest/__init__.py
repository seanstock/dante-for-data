"""Embedding ingestion pipeline.

Ingests chart titles + SQL from BI platforms, generates questions,
simplifies SQL, creates embeddings, and upserts into the local index.

Supported sources:
  - looker        Looker dashboards via SDK
  - databricks    Databricks Lakeview via CLI
  - warehouse     Direct schema introspection
  - mode          Mode Analytics (experimental)
  - redash        Redash (experimental)
  - sigma         Sigma Computing (experimental)
  - superset      Apache Superset (experimental)
  - metabase      Coming soon
  - hex           Coming soon
  - thoughtspot   Coming soon
  - tableau       Coming soon
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_COMING_SOON = {"metabase", "hex", "thoughtspot", "tableau"}

_CORE_SOURCES = {"looker", "databricks", "warehouse"}
_EXPERIMENTAL_SOURCES = {"mode", "redash", "sigma", "superset"}
_ALL_SOURCES = _CORE_SOURCES | _EXPERIMENTAL_SOURCES


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
    sources = list(config.sources)

    if "all" in sources:
        sources = sorted(_CORE_SOURCES)

    for source in sources:
        if source in _COMING_SOON:
            from dante.ingest.coming_soon import ingest_coming_soon
            r = await ingest_coming_soon(source, config)
            _merge_results(result, r)

        elif source == "looker":
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

        elif source == "mode":
            from dante.ingest.mode import ingest_mode
            r = await ingest_mode(config)
            _merge_results(result, r)

        elif source == "redash":
            from dante.ingest.redash import ingest_redash
            r = await ingest_redash(config)
            _merge_results(result, r)

        elif source == "sigma":
            from dante.ingest.sigma import ingest_sigma
            r = await ingest_sigma(config)
            _merge_results(result, r)

        elif source == "superset":
            from dante.ingest.superset import ingest_superset
            r = await ingest_superset(config)
            _merge_results(result, r)

        else:
            logger.warning("Unknown ingestion source: %s", source)

    return result


def _merge_results(target: IngestionResult, source: IngestionResult):
    target.created += source.created
    target.updated += source.updated
    target.skipped += source.skipped
    target.errors += source.errors

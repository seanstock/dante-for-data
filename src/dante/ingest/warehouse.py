"""Warehouse schema metadata ingestion.

Introspects the connected database and creates embeddings for
table/column metadata — providing schema-level context for queries.
"""

from __future__ import annotations

import hashlib
import logging

from sqlalchemy import inspect

from dante.config import project_dir
from dante.connect import connect
from dante.ingest import IngestionConfig, IngestionResult

logger = logging.getLogger(__name__)


async def ingest_warehouse(config: IngestionConfig) -> IngestionResult:
    """Ingest schema metadata from the connected database.

    Creates one embedding per table: "What data is in {table}?"
    with a description of its columns.
    """
    result = IngestionResult()

    try:
        engine = connect()
    except Exception as e:
        logger.error("Cannot connect to database for schema ingestion: %s", e)
        result.errors += 1
        return result

    try:
        insp = inspect(engine)
        table_names = insp.get_table_names()
        logger.info("Found %d tables for schema ingestion", len(table_names))

        for table_name in table_names:
            try:
                columns = insp.get_columns(table_name)
                col_descriptions = []
                for col in columns:
                    nullable = "nullable" if col.get("nullable", True) else "not null"
                    col_descriptions.append(f"  - {col['name']} ({col['type']}, {nullable})")

                col_text = "\n".join(col_descriptions)
                question = f"What data is in the {table_name} table?"
                description = f"Table: {table_name}\nColumns:\n{col_text}"

                det_id = _compute_id(table_name)

                if config.dry_run:
                    logger.info("Would ingest schema for table: %s (%d columns)", table_name, len(columns))
                    result.skipped += 1
                    continue

                from dante.knowledge.vectorize import generate_embedding
                from dante.knowledge.embeddings import upsert, init_db

                db_path = project_dir() / "embeddings.db"
                init_db(db_path)

                embed_text = f"Question: {question}\nSchema: {description}"
                embedding = await generate_embedding(embed_text)

                was_update = upsert(
                    db_path=db_path,
                    id=det_id,
                    question=question,
                    sql="",
                    source="warehouse",
                    dashboard="",
                    description=description,
                    embedding=embedding,
                )

                if was_update:
                    result.updated += 1
                else:
                    result.created += 1

            except Exception as e:
                logger.warning("Error processing table %s: %s", table_name, e)
                result.errors += 1

    except Exception as e:
        logger.error("Warehouse schema ingestion failed: %s", e)
        result.errors += 1

    return result


def _compute_id(table_name: str) -> str:
    raw = f"warehouse:schema:{table_name}"
    hash_hex = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"wh-{hash_hex}"

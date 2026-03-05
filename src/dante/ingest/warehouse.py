"""Warehouse schema metadata ingestion.

Introspects the connected database and creates embeddings for
table/column metadata — providing schema-level context for queries.
"""

from __future__ import annotations

import hashlib
import logging

from sqlalchemy import inspect

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

    from dante.config import knowledge_dir
    from dante.knowledge.embeddings import init_db, upsert
    from dante.knowledge.vectorize import generate_embedding

    try:
        insp = inspect(engine)
        table_names = insp.get_table_names()
        logger.info("Found %d tables for schema ingestion", len(table_names))

        db_path = knowledge_dir() / "embeddings.db"
        conn = init_db(db_path)

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

                emb_id = _compute_id(table_name)

                if config.dry_run:
                    logger.info("Would ingest schema for table: %s (%d columns)", table_name, len(columns))
                    result.skipped += 1
                    continue

                embed_text = f"Question: {question}\nSchema: {description}"
                vector = await generate_embedding(embed_text)

                upsert(
                    conn=conn,
                    id=emb_id,
                    question=question,
                    sql="",
                    source="warehouse",
                    dashboard="",
                    description=description,
                    embedding_vector=vector,
                )
                result.created += 1

            except Exception as e:
                logger.warning("Error processing table %s: %s", table_name, e)
                result.errors += 1

        conn.close()

    except Exception as e:
        logger.error("Warehouse schema ingestion failed: %s", e)
        result.errors += 1

    return result


def _compute_id(table_name: str) -> str:
    raw = f"warehouse:schema:{table_name}"
    hash_hex = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"wh-{hash_hex}"

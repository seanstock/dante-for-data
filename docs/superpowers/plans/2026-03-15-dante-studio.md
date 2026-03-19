# Dante Studio Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fork the existing Dante web app into Dante Studio with a knowledge management layer — pattern lifecycle, glossary, change history with revert, usage analytics, API key auth for the Python library.

**Architecture:** Extend the existing FastAPI + React + Postgres stack. Add new API router (`/api/knowledge/`), extend the `embeddings` table with status/lifecycle columns, add glossary/history/search-log/api-key tables, add admin knowledge pages to the React frontend, and update the existing `dante-for-data` library's remote client to match the new API paths.

**Tech Stack:** FastAPI, SQLAlchemy 2.0 (async), Alembic, Postgres + pgvector, React 18 + TypeScript + CSS Modules, Vite

**Spec:** `docs/superpowers/specs/2026-03-15-dante-cloud-design.md`

---

## File Structure

### Backend (dante-studio) — new files

| File | Responsibility |
|------|---------------|
| `alembic/versions/019_add_knowledge_lifecycle.py` | Migration: add status/use_count/last_used to embeddings |
| `alembic/versions/020_add_knowledge_tables.py` | Migration: glossary, history, search_log, api_keys tables |
| `app/domain/glossary.py` | GlossaryTerm ORM model |
| `app/domain/knowledge_history.py` | KnowledgeHistory ORM model |
| `app/domain/search_log.py` | SearchLog ORM model |
| `app/domain/api_keys.py` | ApiKey ORM model |
| `app/auth/api_key.py` | API key generation, hashing, verification |
| `app/services/knowledge_history.py` | Snapshot helpers + audit log writer |
| `app/api/knowledge.py` | Knowledge router: patterns, glossary, search, history, analytics, embeddings |
| `app/api/api_keys.py` | API key CRUD endpoints |
| `tests/test_api_key_auth.py` | API key auth unit tests |
| `tests/test_knowledge_history.py` | History service unit tests |
| `tests/test_knowledge_integration.py` | End-to-end knowledge API test |

### Backend (dante-studio) — existing files assumed from fork

These files must exist in the forked `../dante` codebase. Verify before starting:

| File | What we need from it |
|------|---------------------|
| `app/auth/permissions.py` | `AdminUser` dependency (uses `RequireRole("admin")`) |
| `app/vectorstore/similarity.py` | `search_similar_embeddings()`, `embed_and_store()` |
| `app/services/embedding_merger.py` | `find_merge_candidates()` |
| `app/auth/session.py` | `verify_session_token()` |

### Backend (dante-studio) — modified files

| File | Change |
|------|--------|
| `app/domain/knowledge.py` | Add status, use_count, last_used columns to Embedding model |
| `app/domain/__init__.py` | Register new models |
| `app/dependencies.py` | Add Bearer token (API key) auth fallback |
| `app/main.py` | Register knowledge + api_keys routers |
| `app/agents/context_builder.py` | Filter deprecated patterns from chat injection |

### Frontend (dante-studio) — new files

| File | Responsibility |
|------|---------------|
| `src/pages/KnowledgeDashboardPage.tsx` | Tabbed knowledge admin (overview, patterns, glossary, history, tools) |
| `src/pages/KnowledgeDashboardPage.module.css` | Styles for knowledge dashboard |
| `src/components/ApiKeyManager.tsx` | API key list/create/revoke component |
| `src/components/ApiKeyManager.module.css` | Styles for API key manager |

### Frontend (dante-studio) — modified files

| File | Change |
|------|--------|
| `src/api/client.ts` | Add `knowledge` and `apiKeys` API namespaces |
| `src/pages/ChatPage.tsx` | Add "Knowledge" sidebar tab + viewMode |
| `src/pages/SettingsPage.tsx` | Embed ApiKeyManager in settings |

### Library (dante-for-data) — modified files

| File | Change |
|------|--------|
| `src/dante/remote.py` | Change `/knowledge/terms` → `/knowledge/glossary` (3 methods) |
| `tests/test_remote.py` | Update assertions for new glossary paths |

---

## Chunk 1: Project Setup + Database Migrations

### Task 1: Fork dante to dante-studio

**Files:**
- Create: `c:/Users/seang/dante-studio/` (copy of `c:/Users/seang/dante/`)

- [ ] **Step 1: Copy the project**

```bash
robocopy c:/Users/seang/dante c:/Users/seang/dante-studio /E /XD .git node_modules .venv __pycache__ .pytest_cache
```

- [ ] **Step 2: Clean git history (fresh start)**

```bash
cd c:/Users/seang/dante-studio
rm -rf .git
git init
git add -A
git commit -m "Initial commit: fork dante web app as dante-studio"
```

- [ ] **Step 3: Verify it runs**

```bash
cd c:/Users/seang/dante-studio
docker compose up -d postgres
cd backend && pip install -e . && alembic upgrade head
cd ../frontend && npm install
```

Expected: Postgres starts, migrations run, frontend deps install.

---

### Task 2: Alembic migration — extend embeddings table

**Files:**
- Create: `c:/Users/seang/dante-studio/backend/alembic/versions/019_add_knowledge_lifecycle.py`
- Modify: `c:/Users/seang/dante-studio/backend/app/domain/knowledge.py` (Embedding model)

- [ ] **Step 1: Create the migration file**

```bash
cd c:/Users/seang/dante-studio/backend
alembic revision --autogenerate -m "add knowledge lifecycle columns to embeddings"
```

Then edit the generated file to contain:

```python
"""add knowledge lifecycle columns to embeddings"""
from alembic import op
import sqlalchemy as sa

def upgrade():
    op.add_column('embeddings', sa.Column('status', sa.Text(), nullable=False, server_default='validated'))
    op.add_column('embeddings', sa.Column('use_count', sa.Integer(), nullable=True, server_default='0'))
    op.add_column('embeddings', sa.Column('last_used', sa.DateTime(timezone=True), nullable=True))

    # Migrate is_hidden → status
    op.execute("UPDATE embeddings SET status = 'deprecated' WHERE is_hidden = true")
    op.execute("UPDATE embeddings SET status = 'validated' WHERE is_hidden = false OR is_hidden IS NULL")

    # Add check constraint
    op.execute("""
        ALTER TABLE embeddings ADD CONSTRAINT ck_embeddings_status
        CHECK (status IN ('draft', 'validated', 'promoted', 'deprecated'))
    """)

    # Index for status filtering
    op.create_index('ix_embeddings_status', 'embeddings', ['status'])

def downgrade():
    op.drop_index('ix_embeddings_status')
    op.execute("ALTER TABLE embeddings DROP CONSTRAINT ck_embeddings_status")
    op.drop_column('embeddings', 'last_used')
    op.drop_column('embeddings', 'use_count')
    op.drop_column('embeddings', 'status')
```

- [ ] **Step 2: Update the Embedding model**

In `backend/app/domain/knowledge.py`, add after `view_count`:

```python
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="validated",
    )
    use_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, server_default="0",
    )
    last_used: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
```

- [ ] **Step 3: Run migration**

```bash
cd c:/Users/seang/dante-studio/backend
alembic upgrade head
```

Expected: Migration applies cleanly.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: add status/use_count/last_used to embeddings table"
```

---

### Task 3: Alembic migration — new tables (glossary, history, search_log, api_keys)

**Files:**
- Create: `c:/Users/seang/dante-studio/backend/alembic/versions/020_add_knowledge_tables.py`
- Create: `c:/Users/seang/dante-studio/backend/app/domain/glossary.py`
- Create: `c:/Users/seang/dante-studio/backend/app/domain/knowledge_history.py`
- Create: `c:/Users/seang/dante-studio/backend/app/domain/search_log.py`
- Create: `c:/Users/seang/dante-studio/backend/app/domain/api_keys.py`
- Modify: `c:/Users/seang/dante-studio/backend/app/domain/__init__.py`

- [ ] **Step 1: Create domain models**

`backend/app/domain/glossary.py`:
```python
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.storage.db import Base


class GlossaryTerm(Base):
    __tablename__ = "knowledge_glossary"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    term: Mapped[str] = mapped_column(Text, nullable=False)
    definition: Mapped[str] = mapped_column(Text, nullable=False)
    author_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True,
    )
```

`backend/app/domain/knowledge_history.py`:
```python
from datetime import datetime, timezone

from sqlalchemy import String, Text, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.storage.db import Base


class KnowledgeHistory(Base):
    __tablename__ = "knowledge_history"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[str] = mapped_column(String(36), nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    before_state: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    after_state: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    user_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
    )
```

`backend/app/domain/search_log.py`:
```python
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Text, Integer, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.storage.db import Base


class SearchLog(Base):
    __tablename__ = "knowledge_search_log"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    result_count: Mapped[int] = mapped_column(Integer, nullable=False)
    top_result_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    source: Mapped[str] = mapped_column(String(20), server_default="web")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
    )
```

`backend/app/domain/api_keys.py`:
```python
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.storage.db import Base


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    key_hash: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    last_used: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
    )
```

- [ ] **Step 2: Register models in `__init__.py`**

Add to `backend/app/domain/__init__.py`:
```python
from app.domain.glossary import GlossaryTerm
from app.domain.knowledge_history import KnowledgeHistory
from app.domain.search_log import SearchLog
from app.domain.api_keys import ApiKey
```

- [ ] **Step 3: Create the migration**

```bash
cd c:/Users/seang/dante-studio/backend
alembic revision --autogenerate -m "add glossary, history, search_log, api_keys tables"
```

Verify the generated migration includes all 4 tables and these indexes. Add any that are missing:
```python
    op.create_index('ix_knowledge_glossary_term_lower', 'knowledge_glossary',
                     [sa.text('lower(term)')], unique=True)
    op.create_index('ix_knowledge_history_entity', 'knowledge_history',
                     ['entity_type', 'entity_id'])
    op.create_index('ix_knowledge_history_created', 'knowledge_history', ['created_at'])
    op.create_index('ix_knowledge_search_log_created', 'knowledge_search_log', ['created_at'])
    op.create_index('ix_api_keys_key_hash', 'api_keys', ['key_hash'])
```

- [ ] **Step 4: Run migration**

```bash
alembic upgrade head
```

Expected: All tables created.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: add glossary, history, search_log, api_keys tables"
```

---

## Chunk 2: API Key Auth + Knowledge API

### Task 4: API key authentication middleware

**Files:**
- Create: `c:/Users/seang/dante-studio/backend/app/auth/api_key.py`
- Create: `c:/Users/seang/dante-studio/backend/tests/test_api_key_auth.py`
- Modify: `c:/Users/seang/dante-studio/backend/app/dependencies.py`

- [ ] **Step 1: Write failing test**

`backend/tests/test_api_key_auth.py`:
```python
import pytest
from app.auth.api_key import verify_api_key, hash_api_key, generate_api_key


def test_hash_api_key_deterministic():
    key = "dk_test123"
    assert hash_api_key(key) == hash_api_key(key)


def test_hash_api_key_different_keys():
    assert hash_api_key("dk_a") != hash_api_key("dk_b")


def test_generate_api_key_has_prefix():
    key = generate_api_key()
    assert key.startswith("dk_")
    assert len(key) > 20


def test_generate_api_key_unique():
    keys = {generate_api_key() for _ in range(10)}
    assert len(keys) == 10
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd c:/Users/seang/dante-studio/backend
pytest tests/test_api_key_auth.py -v
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement api_key module**

`backend/app/auth/api_key.py`:
```python
"""API key authentication for library clients."""

import hashlib
import secrets
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.api_keys import ApiKey


def generate_api_key() -> str:
    """Generate a new API key with dk_ prefix."""
    return f"dk_{secrets.token_urlsafe(32)}"


def hash_api_key(key: str) -> str:
    """Hash an API key for storage. Uses SHA-256."""
    return hashlib.sha256(key.encode()).hexdigest()


async def verify_api_key(db: AsyncSession, key: str) -> str | None:
    """Verify an API key and return the user_id, or None if invalid.

    Also updates last_used timestamp.
    """
    key_hash = hash_api_key(key)
    result = await db.execute(
        select(ApiKey).where(
            ApiKey.key_hash == key_hash,
            ApiKey.revoked_at.is_(None),
        )
    )
    api_key = result.scalar_one_or_none()
    if api_key is None:
        return None

    await db.execute(
        update(ApiKey).where(ApiKey.id == api_key.id).values(
            last_used=datetime.now(timezone.utc)
        )
    )
    await db.commit()
    return api_key.user_id
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_api_key_auth.py -v
```

Expected: 4 PASS.

- [ ] **Step 5: Update dependencies.py to accept API keys**

Modify `backend/app/dependencies.py` — add `authorization` parameter to `get_current_user_id` and `get_optional_user_id`:

```python
from typing import Annotated, Optional
from fastapi import Cookie, Depends, Header, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.storage.db import get_session
from app.auth.session import verify_session_token
from app.auth.api_key import verify_api_key

DbSession = Annotated[AsyncSession, Depends(get_session)]


async def get_current_user_id(
    db: DbSession,
    session_token: Annotated[Optional[str], Cookie()] = None,
    token: Annotated[Optional[str], Query()] = None,
    authorization: Annotated[Optional[str], Header()] = None,
) -> str:
    # 1. Try API key from Authorization header
    if authorization and authorization.startswith("Bearer dk_"):
        api_key = authorization.removeprefix("Bearer ")
        user_id = await verify_api_key(db, api_key)
        if user_id:
            return user_id
        raise HTTPException(status_code=401, detail="Invalid API key")

    # 2. Try session token (preserve existing call signature exactly)
    raw_token = session_token or token
    if not raw_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user_id = await verify_session_token(raw_token, db)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid session")
    return user_id


async def get_optional_user_id(
    db: AsyncSession = Depends(get_session),
    session_token: Annotated[Optional[str], Cookie()] = None,
    token: Annotated[Optional[str], Query()] = None,
    authorization: Annotated[Optional[str], Header()] = None,
) -> Optional[str]:
    # 1. Try API key
    if authorization and authorization.startswith("Bearer dk_"):
        api_key = authorization.removeprefix("Bearer ")
        user_id = await verify_api_key(db, api_key)
        return user_id  # None if invalid (non-throwing)

    # 2. Try session token
    raw_token = session_token or token
    if not raw_token:
        return None
    return await verify_session_token(raw_token, db)


CurrentUserId = Annotated[str, Depends(get_current_user_id)]
OptionalUserId = Annotated[Optional[str], Depends(get_optional_user_id)]
```

**NOTE:** Check the existing `verify_session_token` signature — it may take `(token, db)` or just `(token)`. Preserve the existing call exactly.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: add API key authentication for library clients"
```

---

### Task 5: Knowledge history service

**Files:**
- Create: `c:/Users/seang/dante-studio/backend/app/services/knowledge_history.py`
- Create: `c:/Users/seang/dante-studio/backend/tests/test_knowledge_history.py`

- [ ] **Step 1: Write failing test**

`backend/tests/test_knowledge_history.py`:
```python
import pytest
from app.services.knowledge_history import snapshot_embedding, snapshot_term


def test_snapshot_embedding_captures_fields():
    class FakeEmbedding:
        id = "abc"
        question = "What is churn?"
        sql = "SELECT count(*) FROM users"
        description = "Monthly churn"
        status = "validated"
        source = "manual"

    snap = snapshot_embedding(FakeEmbedding())
    assert snap["question"] == "What is churn?"
    assert snap["status"] == "validated"
    assert "id" in snap


def test_snapshot_term_captures_fields():
    class FakeTerm:
        id = "xyz"
        term = "ARR"
        definition = "Annual Recurring Revenue"

    snap = snapshot_term(FakeTerm())
    assert snap["term"] == "ARR"
    assert snap["definition"] == "Annual Recurring Revenue"
```

- [ ] **Step 2: Run test — expected FAIL**

```bash
pytest tests/test_knowledge_history.py -v
```

- [ ] **Step 3: Implement**

`backend/app/services/knowledge_history.py`:
```python
"""Knowledge change history — audit log for patterns and glossary."""

import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.knowledge_history import KnowledgeHistory


def snapshot_embedding(emb) -> dict:
    """Capture current state of an embedding as a JSON-serializable dict."""
    return {
        "id": emb.id,
        "question": emb.question,
        "sql": emb.sql,
        "description": emb.description,
        "status": getattr(emb, "status", "validated"),
        "source": emb.source,
    }


def snapshot_term(term) -> dict:
    """Capture current state of a glossary term."""
    return {
        "id": term.id,
        "term": term.term,
        "definition": term.definition,
    }


async def log_change(
    db: AsyncSession,
    entity_type: str,
    entity_id: str,
    action: str,
    user_id: str | None,
    before_state: dict | None = None,
    after_state: dict | None = None,
) -> KnowledgeHistory:
    """Write an audit log entry."""
    entry = KnowledgeHistory(
        id=str(uuid.uuid4()),
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        before_state=before_state,
        after_state=after_state,
        user_id=user_id,
        created_at=datetime.now(timezone.utc),
    )
    db.add(entry)
    await db.flush()
    return entry
```

- [ ] **Step 4: Run test — expected PASS**

```bash
pytest tests/test_knowledge_history.py -v
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: add knowledge history service for audit logging"
```

---

### Task 6: Knowledge API router

**Files:**
- Create: `c:/Users/seang/dante-studio/backend/app/api/knowledge.py`
- Modify: `c:/Users/seang/dante-studio/backend/app/main.py`

- [ ] **Step 1: Create the router with all endpoints**

`backend/app/api/knowledge.py`:
```python
"""Knowledge management API — patterns, glossary, history, analytics."""

import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, func, desc, update

from app.dependencies import DbSession, CurrentUserId
from app.auth.permissions import AdminUser
from app.domain.knowledge import Embedding
from app.domain.glossary import GlossaryTerm
from app.domain.knowledge_history import KnowledgeHistory
from app.domain.search_log import SearchLog
from app.services.knowledge_history import (
    log_change, snapshot_embedding, snapshot_term,
)
from app.vectorstore.similarity import search_similar_embeddings


router = APIRouter(prefix="/api/knowledge", tags=["knowledge"])


# ── Pydantic schemas ──────────────────────────────────────────────

class PatternCreate(BaseModel):
    question: str
    sql: str
    tables: list[str] = []
    description: str = ""

class PatternUpdate(BaseModel):
    question: str | None = None
    sql: str | None = None
    tables: list[str] | None = None
    description: str | None = None

class StatusUpdate(BaseModel):
    status: str  # validated, promoted, deprecated

class TermCreate(BaseModel):
    term: str
    definition: str

class TermUpdate(BaseModel):
    definition: str

class SearchRequest(BaseModel):
    query: str
    top_k: int = 10
    source: str = "web"  # "web" or "library" — set by client


# ── Patterns ──────────────────────────────────────────────────────

@router.post("/patterns")
async def create_pattern(body: PatternCreate, db: DbSession, user_id: CurrentUserId):
    emb = Embedding(
        id=str(uuid.uuid4()),
        user_id=user_id,
        question=body.question,
        sql=body.sql,
        description=body.description,
        source="manual",
        status="validated",
    )
    db.add(emb)
    await db.flush()

    # Generate embedding vector for searchability
    try:
        from app.vectorstore.similarity import embed_and_store
        await embed_and_store(db, emb)
    except Exception:
        pass  # Pattern saved without vector; admin can sync later

    await log_change(db, "pattern", emb.id, "create", user_id,
                     after_state=snapshot_embedding(emb))
    await db.commit()
    return {"id": emb.id, "status": emb.status}


@router.get("/patterns")
async def list_patterns(
    db: DbSession,
    user_id: CurrentUserId,
    status: Optional[str] = None,
    offset: int = 0,
    limit: int = Query(default=50, le=200),
):
    q = select(Embedding).where(Embedding.status != "deprecated")
    if status:
        q = select(Embedding).where(Embedding.status == status)
    q = q.order_by(desc(Embedding.created_at)).offset(offset).limit(limit)
    result = await db.execute(q)
    rows = result.scalars().all()
    return [
        {
            "id": r.id, "question": r.question, "sql": r.sql,
            "description": r.description, "status": r.status,
            "use_count": r.use_count or 0, "source": r.source,
            "author_id": r.user_id, "created_at": str(r.created_at),
        }
        for r in rows
    ]


@router.get("/patterns/{pattern_id}")
async def get_pattern(pattern_id: str, db: DbSession, user_id: CurrentUserId):
    result = await db.execute(select(Embedding).where(Embedding.id == pattern_id))
    emb = result.scalar_one_or_none()
    if not emb:
        raise HTTPException(404, "Pattern not found")
    return {
        "id": emb.id, "question": emb.question, "sql": emb.sql,
        "description": emb.description, "status": emb.status,
        "use_count": emb.use_count or 0, "source": emb.source,
        "author_id": emb.user_id, "created_at": str(emb.created_at),
    }


@router.patch("/patterns/{pattern_id}")
async def update_pattern(
    pattern_id: str, body: PatternUpdate, db: DbSession, user_id: CurrentUserId,
):
    result = await db.execute(select(Embedding).where(Embedding.id == pattern_id))
    emb = result.scalar_one_or_none()
    if not emb:
        raise HTTPException(404, "Pattern not found")

    before = snapshot_embedding(emb)
    if body.question is not None:
        emb.question = body.question
    if body.sql is not None:
        emb.sql = body.sql
    if body.description is not None:
        emb.description = body.description
    after = snapshot_embedding(emb)

    # Regenerate vector if question or SQL changed
    if body.question is not None or body.sql is not None:
        try:
            from app.vectorstore.similarity import embed_and_store
            await embed_and_store(db, emb)
        except Exception:
            pass

    await log_change(db, "pattern", emb.id, "update", user_id,
                     before_state=before, after_state=after)
    await db.commit()
    return {"id": emb.id, "status": "updated"}


@router.delete("/patterns/{pattern_id}")
async def delete_pattern(
    pattern_id: str, db: DbSession, user_id: CurrentUserId, _admin: AdminUser,
):
    result = await db.execute(select(Embedding).where(Embedding.id == pattern_id))
    emb = result.scalar_one_or_none()
    if not emb:
        raise HTTPException(404, "Pattern not found")

    before = snapshot_embedding(emb)
    await log_change(db, "pattern", emb.id, "delete", user_id, before_state=before)
    await db.delete(emb)
    await db.commit()
    return {"status": "deleted"}


@router.patch("/patterns/{pattern_id}/status")
async def update_pattern_status(
    pattern_id: str, body: StatusUpdate, db: DbSession,
    user_id: CurrentUserId, _admin: AdminUser,
):
    if body.status not in ("draft", "validated", "promoted", "deprecated"):
        raise HTTPException(400, "Invalid status")

    result = await db.execute(select(Embedding).where(Embedding.id == pattern_id))
    emb = result.scalar_one_or_none()
    if not emb:
        raise HTTPException(404, "Pattern not found")

    before = snapshot_embedding(emb)
    emb.status = body.status
    after = snapshot_embedding(emb)
    action = {"deprecated": "deprecate", "promoted": "promote"}.get(body.status, "update")
    await log_change(db, "pattern", emb.id, action, user_id,
                     before_state=before, after_state=after)
    await db.commit()
    return {"id": emb.id, "status": emb.status}


# ── Search ────────────────────────────────────────────────────────

@router.post("/search")
async def search_patterns(body: SearchRequest, db: DbSession, user_id: CurrentUserId):
    results = await search_similar_embeddings(db, body.query, top_k=body.top_k)

    # Log the search for gap analysis
    search_entry = SearchLog(
        id=str(uuid.uuid4()),
        query=body.query,
        result_count=len(results),
        top_result_id=results[0]["id"] if results else None,
        user_id=user_id,
        source=body.source,
    )
    db.add(search_entry)

    # Increment use_count on returned patterns
    for r in results:
        await db.execute(
            update(Embedding)
            .where(Embedding.id == r["id"])
            .values(
                use_count=func.coalesce(Embedding.use_count, 0) + 1,
                last_used=datetime.now(timezone.utc),
            )
        )
    await db.commit()
    return results


# ── Glossary ──────────────────────────────────────────────────────

@router.post("/glossary")
async def define_term(body: TermCreate, db: DbSession, user_id: CurrentUserId):
    term = GlossaryTerm(
        id=str(uuid.uuid4()),
        term=body.term,
        definition=body.definition,
        author_id=user_id,
    )
    db.add(term)
    await db.flush()
    await log_change(db, "term", term.id, "create", user_id,
                     after_state=snapshot_term(term))
    await db.commit()
    return {"id": term.id, "term": term.term}


@router.get("/glossary")
async def list_terms(
    db: DbSession, user_id: CurrentUserId,
    offset: int = 0, limit: int = Query(default=50, le=200),
):
    q = select(GlossaryTerm).order_by(GlossaryTerm.term).offset(offset).limit(limit)
    result = await db.execute(q)
    rows = result.scalars().all()
    return [
        {"id": r.id, "term": r.term, "definition": r.definition,
         "author_id": r.author_id, "updated_at": str(r.updated_at or r.created_at)}
        for r in rows
    ]


@router.put("/glossary/{term_name}")
async def update_term(
    term_name: str, body: TermUpdate, db: DbSession, user_id: CurrentUserId,
):
    result = await db.execute(
        select(GlossaryTerm).where(func.lower(GlossaryTerm.term) == term_name.lower())
    )
    term = result.scalar_one_or_none()
    if not term:
        raise HTTPException(404, "Term not found")

    before = snapshot_term(term)
    term.definition = body.definition
    after = snapshot_term(term)
    await log_change(db, "term", term.id, "update", user_id,
                     before_state=before, after_state=after)
    await db.commit()
    return {"id": term.id, "status": "updated"}


@router.delete("/glossary/{term_name}")
async def delete_term(
    term_name: str, db: DbSession, user_id: CurrentUserId, _admin: AdminUser,
):
    result = await db.execute(
        select(GlossaryTerm).where(func.lower(GlossaryTerm.term) == term_name.lower())
    )
    term = result.scalar_one_or_none()
    if not term:
        raise HTTPException(404, "Term not found")

    before = snapshot_term(term)
    await log_change(db, "term", term.id, "delete", user_id, before_state=before)
    await db.delete(term)
    await db.commit()
    return {"status": "deleted"}


# ── History ───────────────────────────────────────────────────────

@router.get("/history")
async def list_history(
    db: DbSession, user_id: CurrentUserId,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    offset: int = 0, limit: int = Query(default=50, le=200),
):
    q = select(KnowledgeHistory)
    if entity_type:
        q = q.where(KnowledgeHistory.entity_type == entity_type)
    if entity_id:
        q = q.where(KnowledgeHistory.entity_id == entity_id)
    q = q.order_by(desc(KnowledgeHistory.created_at)).offset(offset).limit(limit)
    result = await db.execute(q)
    rows = result.scalars().all()
    return [
        {
            "id": r.id, "entity_type": r.entity_type, "entity_id": r.entity_id,
            "action": r.action, "before_state": r.before_state,
            "after_state": r.after_state, "user_id": r.user_id,
            "created_at": str(r.created_at),
        }
        for r in rows
    ]


@router.post("/history/{history_id}/revert")
async def revert_change(
    history_id: str, db: DbSession, user_id: CurrentUserId, _admin: AdminUser,
):
    result = await db.execute(
        select(KnowledgeHistory).where(KnowledgeHistory.id == history_id)
    )
    entry = result.scalar_one_or_none()
    if not entry or not entry.before_state:
        raise HTTPException(400, "Cannot revert — no previous state")

    if entry.entity_type == "pattern":
        emb_result = await db.execute(
            select(Embedding).where(Embedding.id == entry.entity_id)
        )
        emb = emb_result.scalar_one_or_none()
        if not emb:
            raise HTTPException(404, "Pattern not found")

        before_revert = snapshot_embedding(emb)
        emb.question = entry.before_state.get("question", emb.question)
        emb.sql = entry.before_state.get("sql", emb.sql)
        emb.description = entry.before_state.get("description", emb.description)
        emb.status = entry.before_state.get("status", emb.status)
        after_revert = snapshot_embedding(emb)

        await log_change(db, "pattern", emb.id, "revert", user_id,
                         before_state=before_revert, after_state=after_revert)

    elif entry.entity_type == "term":
        term_result = await db.execute(
            select(GlossaryTerm).where(GlossaryTerm.id == entry.entity_id)
        )
        term = term_result.scalar_one_or_none()
        if not term:
            raise HTTPException(404, "Term not found")

        before_revert = snapshot_term(term)
        term.term = entry.before_state.get("term", term.term)
        term.definition = entry.before_state.get("definition", term.definition)
        after_revert = snapshot_term(term)

        await log_change(db, "term", term.id, "revert", user_id,
                         before_state=before_revert, after_state=after_revert)

    await db.commit()
    return {"status": "reverted", "entity_type": entry.entity_type}


# ── Analytics (admin only) ────────────────────────────────────────

@router.get("/analytics/patterns")
async def pattern_analytics(db: DbSession, user_id: CurrentUserId, _admin: AdminUser):
    # Most used
    most_q = (
        select(Embedding)
        .where(Embedding.status != "deprecated")
        .order_by(desc(Embedding.use_count))
        .limit(20)
    )
    most_result = await db.execute(most_q)
    most_used = [
        {"id": r.id, "question": r.question, "use_count": r.use_count or 0}
        for r in most_result.scalars().all()
    ]

    # Least used (non-zero)
    least_q = (
        select(Embedding)
        .where(Embedding.status != "deprecated", Embedding.use_count > 0)
        .order_by(Embedding.use_count)
        .limit(20)
    )
    least_result = await db.execute(least_q)
    least_used = [
        {"id": r.id, "question": r.question, "use_count": r.use_count or 0}
        for r in least_result.scalars().all()
    ]

    # Counts by status
    count_q = select(Embedding.status, func.count()).group_by(Embedding.status)
    count_result = await db.execute(count_q)
    counts = {row[0]: row[1] for row in count_result.all()}

    return {"most_used": most_used, "least_used": least_used, "counts_by_status": counts}


@router.get("/analytics/gaps")
async def gap_analytics(
    db: DbSession, user_id: CurrentUserId, _admin: AdminUser,
    limit: int = Query(default=20, le=100),
):
    q = (
        select(SearchLog.query, func.count().label("count"))
        .where(SearchLog.result_count == 0)
        .group_by(SearchLog.query)
        .order_by(desc("count"))
        .limit(limit)
    )
    result = await db.execute(q)
    return [{"query": row[0], "count": row[1]} for row in result.all()]


@router.get("/analytics/activity")
async def activity_analytics(db: DbSession, user_id: CurrentUserId, _admin: AdminUser):
    q = (
        select(
            func.date_trunc("day", KnowledgeHistory.created_at).label("day"),
            func.count().label("count"),
        )
        .group_by("day")
        .order_by(desc("day"))
        .limit(30)
    )
    result = await db.execute(q)
    return [{"day": str(row[0]), "count": row[1]} for row in result.all()]


# ── Knowledge stats (public, used by library) ─────────────────────

@router.get("/stats")
async def knowledge_stats(db: DbSession, user_id: CurrentUserId):
    pattern_count = await db.execute(
        select(func.count()).select_from(Embedding).where(Embedding.status != "deprecated")
    )
    term_count = await db.execute(
        select(func.count()).select_from(GlossaryTerm)
    )
    return {
        "pattern_count": pattern_count.scalar() or 0,
        "term_count": term_count.scalar() or 0,
    }


# ── Embedding Tools (admin only) ─────────────────────────────────

@router.post("/embeddings/sync")
async def sync_embeddings(db: DbSession, user_id: CurrentUserId, _admin: AdminUser):
    """Re-embed all patterns that are missing vectors."""
    from app.vectorstore.similarity import embed_and_store
    result = await db.execute(
        select(Embedding).where(
            Embedding.vector.is_(None), Embedding.status != "deprecated"
        )
    )
    patterns = result.scalars().all()
    count = 0
    for p in patterns:
        try:
            await embed_and_store(db, p)
            count += 1
        except Exception:
            pass
    await db.commit()
    return {"synced": count, "total_missing": len(patterns)}


@router.post("/embeddings/merge")
async def merge_embeddings(db: DbSession, user_id: CurrentUserId, _admin: AdminUser):
    """Find and return merge candidates. Wraps existing merger service."""
    from app.services.embedding_merger import find_merge_candidates
    candidates = await find_merge_candidates(db)
    return [
        {
            "id_a": c.embedding_a_id, "question_a": c.question_a,
            "id_b": c.embedding_b_id, "question_b": c.question_b,
            "similarity": c.similarity,
        }
        for c in candidates
    ]
```

- [ ] **Step 2: Register router in main.py**

Add to `backend/app/main.py` after the existing router imports:
```python
from app.api.knowledge import router as knowledge_router
```

And in the router registration block:
```python
app.include_router(knowledge_router)
```

- [ ] **Step 3: Verify server starts**

```bash
cd c:/Users/seang/dante-studio/backend
uvicorn app.main:app --port 8001
```

Expected: Server starts, `/docs` shows new `/api/knowledge/*` endpoints.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: add knowledge API router (patterns, glossary, history, analytics)"
```

---

### Task 7: API key management endpoints

**Files:**
- Create: `c:/Users/seang/dante-studio/backend/app/api/api_keys.py`
- Modify: `c:/Users/seang/dante-studio/backend/app/main.py`

- [ ] **Step 1: Create the router**

`backend/app/api/api_keys.py`:
```python
"""API key management endpoints."""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import select

from app.dependencies import DbSession, CurrentUserId
from app.domain.api_keys import ApiKey
from app.auth.api_key import generate_api_key, hash_api_key


router = APIRouter(prefix="/api/auth/api-keys", tags=["api-keys"])


class ApiKeyCreate(BaseModel):
    name: str = "default"


@router.post("")
async def create_api_key(body: ApiKeyCreate, db: DbSession, user_id: CurrentUserId):
    raw_key = generate_api_key()
    api_key = ApiKey(
        id=str(uuid.uuid4()),
        user_id=user_id,
        key_hash=hash_api_key(raw_key),
        name=body.name,
        created_at=datetime.now(timezone.utc),
    )
    db.add(api_key)
    await db.commit()
    # Return the raw key ONCE — it cannot be retrieved again
    return {"id": api_key.id, "key": raw_key, "name": body.name}


@router.get("")
async def list_api_keys(db: DbSession, user_id: CurrentUserId):
    result = await db.execute(
        select(ApiKey)
        .where(ApiKey.user_id == user_id, ApiKey.revoked_at.is_(None))
        .order_by(ApiKey.created_at)
    )
    keys = result.scalars().all()
    return [
        {
            "id": k.id, "name": k.name,
            "last_used": str(k.last_used) if k.last_used else None,
            "created_at": str(k.created_at),
        }
        for k in keys
    ]


@router.delete("/{key_id}")
async def revoke_api_key(key_id: str, db: DbSession, user_id: CurrentUserId):
    result = await db.execute(
        select(ApiKey).where(ApiKey.id == key_id, ApiKey.user_id == user_id)
    )
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(404, "API key not found")
    key.revoked_at = datetime.now(timezone.utc)
    await db.commit()
    return {"status": "revoked"}
```

- [ ] **Step 2: Register in main.py**

```python
from app.api.api_keys import router as api_keys_router
# ...
app.include_router(api_keys_router)
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: add API key management endpoints"
```

---

## Chunk 3: Frontend — Admin Knowledge Pages

### Task 8: Frontend API client + Knowledge Dashboard page

**Files:**
- Modify: `c:/Users/seang/dante-studio/frontend/src/api/client.ts`
- Create: `c:/Users/seang/dante-studio/frontend/src/pages/KnowledgeDashboardPage.tsx`
- Create: `c:/Users/seang/dante-studio/frontend/src/pages/KnowledgeDashboardPage.module.css`
- Modify: `c:/Users/seang/dante-studio/frontend/src/pages/ChatPage.tsx`

- [ ] **Step 1: Add knowledge + apiKeys API methods to client.ts**

Add these new namespaces to the `api` object, after the existing sections:

```typescript
knowledge: {
    listPatterns: (params?: { status?: string; offset?: number; limit?: number }) =>
        request(`/api/knowledge/patterns?${new URLSearchParams(
            Object.entries(params || {}).filter(([, v]) => v != null).map(([k, v]) => [k, String(v)])
        )}`),
    getPattern: (id: string) => request(`/api/knowledge/patterns/${id}`),
    createPattern: (data: { question: string; sql: string; tables?: string[]; description?: string }) =>
        request('/api/knowledge/patterns', { method: 'POST', body: JSON.stringify(data) }),
    updatePattern: (id: string, data: Record<string, unknown>) =>
        request(`/api/knowledge/patterns/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
    deletePattern: (id: string) =>
        request(`/api/knowledge/patterns/${id}`, { method: 'DELETE' }),
    updateStatus: (id: string, status: string) =>
        request(`/api/knowledge/patterns/${id}/status`, {
            method: 'PATCH', body: JSON.stringify({ status }),
        }),
    search: (query: string, topK?: number) =>
        request('/api/knowledge/search', {
            method: 'POST', body: JSON.stringify({ query, top_k: topK || 10 }),
        }),
    listTerms: (params?: { offset?: number; limit?: number }) =>
        request(`/api/knowledge/glossary?${new URLSearchParams(
            Object.entries(params || {}).filter(([, v]) => v != null).map(([k, v]) => [k, String(v)])
        )}`),
    defineTerm: (term: string, definition: string) =>
        request('/api/knowledge/glossary', {
            method: 'POST', body: JSON.stringify({ term, definition }),
        }),
    updateTerm: (term: string, definition: string) =>
        request(`/api/knowledge/glossary/${encodeURIComponent(term)}`, {
            method: 'PUT', body: JSON.stringify({ definition }),
        }),
    deleteTerm: (term: string) =>
        request(`/api/knowledge/glossary/${encodeURIComponent(term)}`, { method: 'DELETE' }),
    history: (params?: { entity_type?: string; entity_id?: string; offset?: number; limit?: number }) =>
        request(`/api/knowledge/history?${new URLSearchParams(
            Object.entries(params || {}).filter(([, v]) => v != null).map(([k, v]) => [k, String(v)])
        )}`),
    revert: (historyId: string) =>
        request(`/api/knowledge/history/${historyId}/revert`, { method: 'POST' }),
    analyticsPatterns: () => request('/api/knowledge/analytics/patterns'),
    analyticsGaps: () => request('/api/knowledge/analytics/gaps'),
    analyticsActivity: () => request('/api/knowledge/analytics/activity'),
    stats: () => request('/api/knowledge/stats'),
    syncEmbeddings: () =>
        request('/api/knowledge/embeddings/sync', { method: 'POST' }),
    mergeCandidates: () =>
        request('/api/knowledge/embeddings/merge', { method: 'POST' }),
},
apiKeys: {
    list: () => request('/api/auth/api-keys'),
    create: (name: string) =>
        request('/api/auth/api-keys', {
            method: 'POST', body: JSON.stringify({ name }),
        }),
    revoke: (id: string) =>
        request(`/api/auth/api-keys/${id}`, { method: 'DELETE' }),
},
```

- [ ] **Step 2: Create the Knowledge Dashboard page**

`frontend/src/pages/KnowledgeDashboardPage.tsx`:
```tsx
import { useState, useEffect, useCallback } from 'react'
import { api } from '../api/client'
import { Spinner } from '../components/Spinner'
import { useToast } from '../components/ToastProvider'
import styles from './KnowledgeDashboardPage.module.css'

type Tab = 'overview' | 'patterns' | 'glossary' | 'history' | 'tools'

interface Pattern {
    id: string; question: string; sql: string; description: string
    status: string; use_count: number; source: string; author_id: string
    created_at: string
}

interface Term {
    id: string; term: string; definition: string
    author_id: string; updated_at: string
}

interface HistoryEntry {
    id: string; entity_type: string; entity_id: string; action: string
    before_state: Record<string, unknown> | null
    after_state: Record<string, unknown> | null
    user_id: string; created_at: string
}

interface KnowledgeDashboardPageProps {
    onClose: () => void
    userRole: string
}

export function KnowledgeDashboardPage({ onClose, userRole }: KnowledgeDashboardPageProps) {
    const [activeTab, setActiveTab] = useState<Tab>('overview')
    const { addToast } = useToast()
    const isAdmin = userRole === 'admin'

    return (
        <div className={styles.container}>
            <div className={styles.header}>
                <h2 className={styles.headerTitle}>Knowledge Base</h2>
                <button onClick={onClose} className={styles.closeButton}>&times;</button>
            </div>

            <div className={styles.tabs}>
                {(['overview', 'patterns', 'glossary', 'history', 'tools'] as Tab[]).map(tab => (
                    <button
                        key={tab}
                        className={`${styles.tab} ${activeTab === tab ? styles.tabActive : ''}`}
                        onClick={() => setActiveTab(tab)}
                    >
                        {tab.charAt(0).toUpperCase() + tab.slice(1)}
                    </button>
                ))}
            </div>

            <div className={styles.tabContent}>
                {activeTab === 'overview' && <OverviewTab addToast={addToast} isAdmin={isAdmin} />}
                {activeTab === 'patterns' && <PatternsTab addToast={addToast} isAdmin={isAdmin} />}
                {activeTab === 'glossary' && <GlossaryTab addToast={addToast} isAdmin={isAdmin} />}
                {activeTab === 'history' && <HistoryTab addToast={addToast} isAdmin={isAdmin} />}
                {activeTab === 'tools' && <ToolsTab addToast={addToast} isAdmin={isAdmin} />}
            </div>
        </div>
    )
}


// ── Overview Tab ──────────────────────────────────────────────────

function OverviewTab({ addToast, isAdmin }: { addToast: (msg: string) => void; isAdmin: boolean }) {
    const [analytics, setAnalytics] = useState<{
        most_used: { id: string; question: string; use_count: number }[]
        least_used: { id: string; question: string; use_count: number }[]
        counts_by_status: Record<string, number>
    } | null>(null)
    const [gaps, setGaps] = useState<{ query: string; count: number }[]>([])
    const [loading, setLoading] = useState(true)

    useEffect(() => {
        if (!isAdmin) { setLoading(false); return }
        Promise.all([
            api.knowledge.analyticsPatterns().catch(() => null),
            api.knowledge.analyticsGaps().catch(() => []),
        ]).then(([a, g]) => {
            setAnalytics(a)
            setGaps(g as { query: string; count: number }[])
        }).catch(() => addToast('Failed to load analytics'))
          .finally(() => setLoading(false))
    }, [addToast, isAdmin])

    if (!isAdmin) {
        return <p className={styles.emptyText}>Admin access required to view analytics.</p>
    }

    if (loading) {
        return <div className={styles.loading}><Spinner size={16} borderWidth={2} color="var(--accent)" /> Loading...</div>
    }

    return (
        <div className={styles.overviewGrid}>
            {/* Status counts */}
            <div className={styles.card}>
                <h3 className={styles.cardTitle}>Patterns by Status</h3>
                {analytics?.counts_by_status ? (
                    <div className={styles.statusGrid}>
                        {Object.entries(analytics.counts_by_status).map(([status, count]) => (
                            <div key={status} className={styles.statusItem}>
                                <span className={`${styles.badge} ${styles[`badge_${status}`] || ''}`}>{status}</span>
                                <span className={styles.statusCount}>{count}</span>
                            </div>
                        ))}
                    </div>
                ) : <p className={styles.emptyText}>No data</p>}
            </div>

            {/* Most used */}
            <div className={styles.card}>
                <h3 className={styles.cardTitle}>Top Patterns</h3>
                {analytics?.most_used?.length ? (
                    <ul className={styles.rankList}>
                        {analytics.most_used.slice(0, 10).map((p, i) => (
                            <li key={p.id} className={styles.rankItem}>
                                <span className={styles.rankNum}>{i + 1}.</span>
                                <span className={styles.rankQuestion}>{p.question}</span>
                                <span className={styles.rankCount}>{p.use_count} uses</span>
                            </li>
                        ))}
                    </ul>
                ) : <p className={styles.emptyText}>No usage data yet</p>}
            </div>

            {/* Knowledge gaps */}
            <div className={styles.card}>
                <h3 className={styles.cardTitle}>Knowledge Gaps</h3>
                <p className={styles.cardDescription}>Searches with zero results</p>
                {gaps.length ? (
                    <ul className={styles.rankList}>
                        {gaps.slice(0, 10).map((g, i) => (
                            <li key={i} className={styles.rankItem}>
                                <span className={styles.rankQuestion}>{g.query}</span>
                                <span className={styles.rankCount}>{g.count}x</span>
                            </li>
                        ))}
                    </ul>
                ) : <p className={styles.emptyText}>No gaps found</p>}
            </div>
        </div>
    )
}


// ── Patterns Tab ─────────────────────────────────────────────────

function PatternsTab({ addToast, isAdmin }: { addToast: (msg: string) => void; isAdmin: boolean }) {
    const [patterns, setPatterns] = useState<Pattern[]>([])
    const [loading, setLoading] = useState(true)
    const [statusFilter, setStatusFilter] = useState<string>('')
    const [editingId, setEditingId] = useState<string | null>(null)
    const [editQuestion, setEditQuestion] = useState('')
    const [editSql, setEditSql] = useState('')
    const [editDescription, setEditDescription] = useState('')

    const loadPatterns = useCallback(() => {
        setLoading(true)
        api.knowledge.listPatterns(statusFilter ? { status: statusFilter } : undefined)
            .then((res: Pattern[]) => setPatterns(res))
            .catch(() => addToast('Failed to load patterns'))
            .finally(() => setLoading(false))
    }, [addToast, statusFilter])

    useEffect(() => { loadPatterns() }, [loadPatterns])

    const handleStatusChange = async (id: string, status: string) => {
        try {
            await api.knowledge.updateStatus(id, status)
            addToast(`Pattern ${status}`)
            loadPatterns()
        } catch { addToast('Failed to update status') }
    }

    const handleDelete = async (id: string) => {
        try {
            await api.knowledge.deletePattern(id)
            addToast('Pattern deleted')
            loadPatterns()
        } catch { addToast('Failed to delete pattern') }
    }

    const handleSaveEdit = async () => {
        if (!editingId) return
        try {
            await api.knowledge.updatePattern(editingId, {
                question: editQuestion, sql: editSql, description: editDescription,
            })
            addToast('Pattern updated')
            setEditingId(null)
            loadPatterns()
        } catch { addToast('Failed to update pattern') }
    }

    const startEdit = (p: Pattern) => {
        setEditingId(p.id)
        setEditQuestion(p.question)
        setEditSql(p.sql)
        setEditDescription(p.description)
    }

    if (loading) {
        return <div className={styles.loading}><Spinner size={16} borderWidth={2} color="var(--accent)" /> Loading...</div>
    }

    return (
        <div>
            {/* Filter bar */}
            <div className={styles.filterBar}>
                <select
                    value={statusFilter}
                    onChange={e => setStatusFilter(e.target.value)}
                    className={styles.select}
                >
                    <option value="">All statuses</option>
                    <option value="draft">Draft</option>
                    <option value="validated">Validated</option>
                    <option value="promoted">Promoted</option>
                    <option value="deprecated">Deprecated</option>
                </select>
                <span className={styles.resultCount}>{patterns.length} patterns</span>
            </div>

            {/* Edit modal */}
            {editingId && (
                <div className={styles.editPanel}>
                    <h4 className={styles.editTitle}>Edit Pattern</h4>
                    <input value={editQuestion} onChange={e => setEditQuestion(e.target.value)}
                        placeholder="Question" className={styles.input} />
                    <textarea value={editSql} onChange={e => setEditSql(e.target.value)}
                        placeholder="SQL" rows={4} className={styles.textarea} />
                    <input value={editDescription} onChange={e => setEditDescription(e.target.value)}
                        placeholder="Description" className={styles.input} />
                    <div className={styles.editActions}>
                        <button onClick={handleSaveEdit} className={styles.primaryButton}>Save</button>
                        <button onClick={() => setEditingId(null)} className={styles.secondaryButton}>Cancel</button>
                    </div>
                </div>
            )}

            {/* Pattern table */}
            <table className={styles.table}>
                <thead>
                    <tr>
                        <th>Question</th>
                        <th>Status</th>
                        <th>Uses</th>
                        <th>Source</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {patterns.map(p => (
                        <tr key={p.id}>
                            <td className={styles.questionCell}>
                                <span className={styles.question}>{p.question}</span>
                                {p.sql && <code className={styles.sqlPreview}>{p.sql.slice(0, 80)}...</code>}
                            </td>
                            <td><span className={`${styles.badge} ${styles[`badge_${p.status}`] || ''}`}>{p.status}</span></td>
                            <td>{p.use_count}</td>
                            <td>{p.source}</td>
                            <td className={styles.actions}>
                                <button onClick={() => startEdit(p)} className={styles.iconBtn} title="Edit">Edit</button>
                                {isAdmin && p.status !== 'deprecated' && (
                                    <button onClick={() => handleStatusChange(p.id, 'deprecated')} className={styles.iconBtn} title="Deprecate">Dep</button>
                                )}
                                {isAdmin && p.status !== 'promoted' && p.status !== 'deprecated' && (
                                    <button onClick={() => handleStatusChange(p.id, 'promoted')} className={styles.iconBtn} title="Promote">Pro</button>
                                )}
                                {isAdmin && (
                                    <button onClick={() => handleDelete(p.id)} className={styles.iconBtnDanger} title="Delete">Del</button>
                                )}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>

            {!patterns.length && <p className={styles.emptyText}>No patterns found</p>}
        </div>
    )
}


// ── Glossary Tab ─────────────────────────────────────────────────

function GlossaryTab({ addToast, isAdmin }: { addToast: (msg: string) => void; isAdmin: boolean }) {
    const [terms, setTerms] = useState<Term[]>([])
    const [loading, setLoading] = useState(true)
    const [newTerm, setNewTerm] = useState('')
    const [newDef, setNewDef] = useState('')
    const [editingTerm, setEditingTerm] = useState<string | null>(null)
    const [editDef, setEditDef] = useState('')

    const loadTerms = useCallback(() => {
        setLoading(true)
        api.knowledge.listTerms()
            .then((res: Term[]) => setTerms(res))
            .catch(() => addToast('Failed to load glossary'))
            .finally(() => setLoading(false))
    }, [addToast])

    useEffect(() => { loadTerms() }, [loadTerms])

    const handleAdd = async () => {
        if (!newTerm.trim() || !newDef.trim()) return
        try {
            await api.knowledge.defineTerm(newTerm.trim(), newDef.trim())
            setNewTerm('')
            setNewDef('')
            addToast('Term defined')
            loadTerms()
        } catch { addToast('Failed to define term') }
    }

    const handleUpdate = async (term: string) => {
        try {
            await api.knowledge.updateTerm(term, editDef)
            setEditingTerm(null)
            addToast('Term updated')
            loadTerms()
        } catch { addToast('Failed to update term') }
    }

    const handleDelete = async (term: string) => {
        try {
            await api.knowledge.deleteTerm(term)
            addToast('Term deleted')
            loadTerms()
        } catch { addToast('Failed to delete term') }
    }

    if (loading) {
        return <div className={styles.loading}><Spinner size={16} borderWidth={2} color="var(--accent)" /> Loading...</div>
    }

    return (
        <div>
            {/* Add new term */}
            <div className={styles.addRow}>
                <input value={newTerm} onChange={e => setNewTerm(e.target.value)}
                    placeholder="Term" className={styles.input} />
                <input value={newDef} onChange={e => setNewDef(e.target.value)}
                    placeholder="Definition" className={styles.inputWide} />
                <button onClick={handleAdd} className={styles.primaryButton}
                    disabled={!newTerm.trim() || !newDef.trim()}>Add</button>
            </div>

            {/* Terms table */}
            <table className={styles.table}>
                <thead>
                    <tr><th>Term</th><th>Definition</th><th>Actions</th></tr>
                </thead>
                <tbody>
                    {terms.map(t => (
                        <tr key={t.id}>
                            <td className={styles.termCell}>{t.term}</td>
                            <td>
                                {editingTerm === t.term ? (
                                    <div className={styles.inlineEdit}>
                                        <input value={editDef} onChange={e => setEditDef(e.target.value)}
                                            className={styles.input} autoFocus />
                                        <button onClick={() => handleUpdate(t.term)} className={styles.primaryButton}>Save</button>
                                        <button onClick={() => setEditingTerm(null)} className={styles.secondaryButton}>Cancel</button>
                                    </div>
                                ) : (
                                    <span>{t.definition}</span>
                                )}
                            </td>
                            <td className={styles.actions}>
                                <button onClick={() => { setEditingTerm(t.term); setEditDef(t.definition) }}
                                    className={styles.iconBtn} title="Edit">Edit</button>
                                {isAdmin && (
                                    <button onClick={() => handleDelete(t.term)}
                                        className={styles.iconBtnDanger} title="Delete">Del</button>
                                )}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>

            {!terms.length && <p className={styles.emptyText}>No glossary terms yet</p>}
        </div>
    )
}


// ── History Tab ──────────────────────────────────────────────────

function HistoryTab({ addToast, isAdmin }: { addToast: (msg: string) => void; isAdmin: boolean }) {
    const [entries, setEntries] = useState<HistoryEntry[]>([])
    const [loading, setLoading] = useState(true)

    const loadHistory = useCallback(() => {
        setLoading(true)
        api.knowledge.history()
            .then((res: HistoryEntry[]) => setEntries(res))
            .catch(() => addToast('Failed to load history'))
            .finally(() => setLoading(false))
    }, [addToast])

    useEffect(() => { loadHistory() }, [loadHistory])

    const handleRevert = async (id: string) => {
        try {
            await api.knowledge.revert(id)
            addToast('Change reverted')
            loadHistory()
        } catch { addToast('Failed to revert') }
    }

    if (loading) {
        return <div className={styles.loading}><Spinner size={16} borderWidth={2} color="var(--accent)" /> Loading...</div>
    }

    return (
        <div>
            <div className={styles.timeline}>
                {entries.map(e => (
                    <div key={e.id} className={styles.timelineItem}>
                        <div className={styles.timelineHeader}>
                            <span className={`${styles.badge} ${styles[`badge_${e.action}`] || ''}`}>{e.action}</span>
                            <span className={styles.timelineType}>{e.entity_type}</span>
                            <span className={styles.timelineDate}>
                                {new Date(e.created_at).toLocaleString()}
                            </span>
                        </div>
                        {e.after_state && (
                            <div className={styles.timelineBody}>
                                {(e.after_state as Record<string, unknown>).question
                                    ? String((e.after_state as Record<string, unknown>).question)
                                    : (e.after_state as Record<string, unknown>).term
                                        ? String((e.after_state as Record<string, unknown>).term)
                                        : JSON.stringify(e.after_state).slice(0, 120)}
                            </div>
                        )}
                        {isAdmin && e.before_state && (
                            <button onClick={() => handleRevert(e.id)} className={styles.revertButton}>
                                Revert
                            </button>
                        )}
                    </div>
                ))}
            </div>

            {!entries.length && <p className={styles.emptyText}>No history yet</p>}
        </div>
    )
}


// ── Tools Tab ────────────────────────────────────────────────────

function ToolsTab({ addToast, isAdmin }: { addToast: (msg: string) => void; isAdmin: boolean }) {
    const [syncing, setSyncing] = useState(false)
    const [merging, setMerging] = useState(false)
    const [syncResult, setSyncResult] = useState<{ synced: number; total_missing: number } | null>(null)
    const [mergeCandidates, setMergeCandidates] = useState<
        { id_a: string; question_a: string; id_b: string; question_b: string; similarity: number }[]
    >([])

    const handleSync = async () => {
        setSyncing(true)
        try {
            const result = await api.knowledge.syncEmbeddings()
            setSyncResult(result as { synced: number; total_missing: number })
            addToast(`Synced ${(result as { synced: number }).synced} embeddings`)
        } catch { addToast('Failed to sync embeddings') }
        finally { setSyncing(false) }
    }

    const handleMerge = async () => {
        setMerging(true)
        try {
            const result = await api.knowledge.mergeCandidates()
            setMergeCandidates(result as typeof mergeCandidates)
            addToast(`Found ${(result as unknown[]).length} merge candidates`)
        } catch { addToast('Failed to find merge candidates') }
        finally { setMerging(false) }
    }

    if (!isAdmin) {
        return <p className={styles.emptyText}>Admin access required for embedding tools.</p>
    }

    return (
        <div className={styles.toolsGrid}>
            {/* Sync embeddings */}
            <div className={styles.card}>
                <h3 className={styles.cardTitle}>Sync Embeddings</h3>
                <p className={styles.cardDescription}>
                    Re-generate vectors for patterns that are missing them.
                </p>
                <button onClick={handleSync} disabled={syncing} className={styles.primaryButton}>
                    {syncing ? 'Syncing...' : 'Sync Now'}
                </button>
                {syncResult && (
                    <p className={styles.resultText}>
                        Synced {syncResult.synced} of {syncResult.total_missing} missing embeddings
                    </p>
                )}
            </div>

            {/* Merge duplicates */}
            <div className={styles.card}>
                <h3 className={styles.cardTitle}>Find Duplicates</h3>
                <p className={styles.cardDescription}>
                    Identify patterns with high similarity that may be duplicates.
                </p>
                <button onClick={handleMerge} disabled={merging} className={styles.primaryButton}>
                    {merging ? 'Scanning...' : 'Find Candidates'}
                </button>
                {mergeCandidates.length > 0 && (
                    <ul className={styles.mergeList}>
                        {mergeCandidates.map((c, i) => (
                            <li key={i} className={styles.mergeItem}>
                                <div>{c.question_a}</div>
                                <div className={styles.mergeSimilarity}>{(c.similarity * 100).toFixed(1)}% similar</div>
                                <div>{c.question_b}</div>
                            </li>
                        ))}
                    </ul>
                )}
            </div>
        </div>
    )
}
```

- [ ] **Step 3: Create the CSS module**

`frontend/src/pages/KnowledgeDashboardPage.module.css`:
```css
.container {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: var(--bg-primary);
    color: var(--text-primary);
}

.header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem 1.5rem 0.5rem;
    border-bottom: 1px solid var(--bg-tertiary);
}

.headerTitle {
    font-size: 1.125rem;
    font-weight: 700;
    margin: 0;
}

.closeButton {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-size: 1.25rem;
    cursor: pointer;
    padding: 0.25rem 0.5rem;
}

.closeButton:hover { color: var(--text-primary); }

/* Tabs */
.tabs {
    display: flex;
    gap: 0.25rem;
    padding: 0.75rem 1.5rem 0;
    border-bottom: 1px solid var(--bg-tertiary);
}

.tab {
    background: none;
    border: none;
    color: var(--text-secondary);
    padding: 0.5rem 0.75rem;
    font-size: 0.8125rem;
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: color 0.15s, border-color 0.15s;
}

.tab:hover { color: var(--text-primary); }

.tabActive {
    color: var(--text-primary);
    border-bottom-color: var(--accent);
}

.tabContent {
    flex: 1;
    overflow-y: auto;
    padding: 1rem 1.5rem;
}

/* Loading */
.loading {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 2rem 0;
    color: var(--text-secondary);
    font-size: 0.8125rem;
}

/* Overview grid */
.overviewGrid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 1rem;
}

/* Cards */
.card {
    background: var(--bg-secondary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 8px;
    padding: 1rem;
}

.cardTitle {
    font-size: 0.875rem;
    font-weight: 600;
    margin: 0 0 0.25rem;
}

.cardDescription {
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin: 0 0 0.75rem;
}

/* Status badges */
.badge {
    display: inline-block;
    padding: 0.125rem 0.5rem;
    border-radius: 9999px;
    font-size: 0.6875rem;
    font-weight: 600;
    text-transform: uppercase;
    background: var(--bg-tertiary);
    color: var(--text-secondary);
}

.badge_validated { background: #1a3a2a; color: #4ade80; }
.badge_promoted { background: #1a2a3a; color: #60a5fa; }
.badge_deprecated { background: #3a1a1a; color: #f87171; }
.badge_draft { background: #2a2a1a; color: #fbbf24; }
.badge_create { background: #1a3a2a; color: #4ade80; }
.badge_update { background: #1a2a3a; color: #60a5fa; }
.badge_delete { background: #3a1a1a; color: #f87171; }
.badge_revert { background: #2a1a3a; color: #c084fc; }
.badge_deprecate { background: #3a1a1a; color: #f87171; }
.badge_promote { background: #1a2a3a; color: #60a5fa; }

/* Status grid */
.statusGrid {
    display: flex;
    gap: 1rem;
    flex-wrap: wrap;
}

.statusItem {
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.statusCount {
    font-size: 1.25rem;
    font-weight: 700;
}

/* Rank list */
.rankList {
    list-style: none;
    padding: 0;
    margin: 0;
}

.rankItem {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.375rem 0;
    font-size: 0.8125rem;
    border-bottom: 1px solid var(--bg-tertiary);
}

.rankNum {
    color: var(--text-secondary);
    min-width: 1.5rem;
}

.rankQuestion {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.rankCount {
    color: var(--text-secondary);
    font-size: 0.75rem;
    white-space: nowrap;
}

/* Filter bar */
.filterBar {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
}

.select {
    background: var(--bg-secondary);
    border: 1px solid var(--bg-tertiary);
    color: var(--text-primary);
    padding: 0.375rem 0.5rem;
    border-radius: 0.375rem;
    font-size: 0.8125rem;
}

.resultCount {
    font-size: 0.75rem;
    color: var(--text-secondary);
}

/* Table */
.table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.8125rem;
}

.table th {
    text-align: left;
    padding: 0.5rem 0.75rem;
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 0.75rem;
    text-transform: uppercase;
    border-bottom: 1px solid var(--bg-tertiary);
}

.table td {
    padding: 0.5rem 0.75rem;
    border-bottom: 1px solid var(--bg-tertiary);
    vertical-align: top;
}

.table tbody tr:hover { background: var(--bg-tertiary); }

.questionCell { max-width: 400px; }

.question {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.sqlPreview {
    display: block;
    font-size: 0.6875rem;
    color: var(--text-secondary);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    margin-top: 0.125rem;
}

.termCell { font-weight: 600; white-space: nowrap; }

.actions {
    display: flex;
    gap: 0.375rem;
    white-space: nowrap;
}

.iconBtn {
    background: none;
    border: 1px solid var(--bg-tertiary);
    color: var(--text-secondary);
    padding: 0.125rem 0.375rem;
    border-radius: 0.25rem;
    cursor: pointer;
    font-size: 0.6875rem;
    transition: color 0.15s;
}

.iconBtn:hover { color: var(--text-primary); border-color: var(--text-secondary); }

.iconBtnDanger {
    composes: iconBtn;
}

.iconBtnDanger:hover { color: var(--error, #f87171); border-color: var(--error, #f87171); }

/* Buttons */
.primaryButton {
    background: var(--accent);
    color: #fff;
    border: none;
    padding: 0.375rem 0.75rem;
    border-radius: 0.375rem;
    font-size: 0.8125rem;
    cursor: pointer;
    transition: opacity 0.15s;
}

.primaryButton:hover { opacity: 0.9; }
.primaryButton:disabled { opacity: 0.5; cursor: not-allowed; }

.secondaryButton {
    background: var(--bg-tertiary);
    color: var(--text-primary);
    border: none;
    padding: 0.375rem 0.75rem;
    border-radius: 0.375rem;
    font-size: 0.8125rem;
    cursor: pointer;
}

.secondaryButton:hover { opacity: 0.8; }

/* Inputs */
.input {
    background: var(--bg-secondary);
    border: 1px solid var(--bg-tertiary);
    color: var(--text-primary);
    padding: 0.375rem 0.5rem;
    border-radius: 0.375rem;
    font-size: 0.8125rem;
    font-family: inherit;
}

.input:focus { outline: none; border-color: var(--accent); }

.inputWide { composes: input; flex: 1; }

.textarea {
    composes: input;
    resize: vertical;
    font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: 0.75rem;
}

/* Add row */
.addRow {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
    align-items: center;
}

/* Inline edit */
.inlineEdit {
    display: flex;
    gap: 0.375rem;
    align-items: center;
}

/* Edit panel */
.editPanel {
    background: var(--bg-secondary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 8px;
    padding: 1rem;
    margin-bottom: 0.75rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
}

.editTitle {
    font-size: 0.875rem;
    font-weight: 600;
    margin: 0;
}

.editActions {
    display: flex;
    gap: 0.5rem;
    margin-top: 0.25rem;
}

/* Timeline */
.timeline {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
}

.timelineItem {
    background: var(--bg-secondary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 8px;
    padding: 0.75rem;
}

.timelineHeader {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.375rem;
}

.timelineType {
    font-size: 0.75rem;
    color: var(--text-secondary);
}

.timelineDate {
    margin-left: auto;
    font-size: 0.6875rem;
    color: var(--text-secondary);
}

.timelineBody {
    font-size: 0.8125rem;
    color: var(--text-secondary);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.revertButton {
    background: none;
    border: 1px solid var(--bg-tertiary);
    color: var(--text-secondary);
    padding: 0.125rem 0.5rem;
    border-radius: 0.25rem;
    cursor: pointer;
    font-size: 0.6875rem;
    margin-top: 0.375rem;
    transition: color 0.15s;
}

.revertButton:hover { color: #c084fc; border-color: #c084fc; }

/* Tools grid */
.toolsGrid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 1rem;
}

.resultText {
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin-top: 0.5rem;
}

.mergeList {
    list-style: none;
    padding: 0;
    margin: 0.75rem 0 0;
}

.mergeItem {
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--bg-tertiary);
    font-size: 0.8125rem;
}

.mergeSimilarity {
    font-size: 0.6875rem;
    color: var(--accent);
    margin: 0.125rem 0;
}

.emptyText {
    color: var(--text-secondary);
    font-size: 0.8125rem;
    padding: 1rem 0;
}
```

- [ ] **Step 4: Add Knowledge view to ChatPage**

In `frontend/src/pages/ChatPage.tsx`:

1. Add `'knowledge'` to the `ViewMode` type
2. Add a "Knowledge" sidebar tab button
3. Add the conditional render:

```tsx
{viewMode === 'knowledge' && (
    <KnowledgeDashboardPage
        onClose={() => setViewMode('chat')}
        userRole={user.role}
    />
)}
```

4. Import:
```tsx
import { KnowledgeDashboardPage } from './KnowledgeDashboardPage'
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: add knowledge dashboard page with patterns, glossary, history, analytics"
```

---

### Task 9: API Key management UI

**Files:**
- Create: `c:/Users/seang/dante-studio/frontend/src/components/ApiKeyManager.tsx`
- Create: `c:/Users/seang/dante-studio/frontend/src/components/ApiKeyManager.module.css`
- Modify: `c:/Users/seang/dante-studio/frontend/src/pages/SettingsPage.tsx`

- [ ] **Step 1: Create ApiKeyManager component**

`frontend/src/components/ApiKeyManager.tsx`:
```tsx
import { useState, useEffect, useCallback } from 'react'
import { api } from '../api/client'
import { useToast } from './ToastProvider'
import styles from './ApiKeyManager.module.css'

interface ApiKeyEntry {
    id: string
    name: string
    last_used: string | null
    created_at: string
}

export function ApiKeyManager() {
    const [keys, setKeys] = useState<ApiKeyEntry[]>([])
    const [newKeyName, setNewKeyName] = useState('')
    const [createdKey, setCreatedKey] = useState<string | null>(null)
    const [creating, setCreating] = useState(false)
    const [copied, setCopied] = useState(false)
    const { addToast } = useToast()

    const loadKeys = useCallback(() => {
        api.apiKeys.list()
            .then((res: ApiKeyEntry[]) => setKeys(res))
            .catch(() => addToast('Failed to load API keys'))
    }, [addToast])

    useEffect(() => { loadKeys() }, [loadKeys])

    const handleCreate = async () => {
        setCreating(true)
        try {
            const result = await api.apiKeys.create(newKeyName.trim() || 'default') as {
                id: string; key: string; name: string
            }
            setCreatedKey(result.key)
            setNewKeyName('')
            setCopied(false)
            loadKeys()
        } catch {
            addToast('Failed to create API key')
        } finally {
            setCreating(false)
        }
    }

    const handleRevoke = async (id: string) => {
        try {
            await api.apiKeys.revoke(id)
            addToast('API key revoked')
            loadKeys()
        } catch {
            addToast('Failed to revoke key')
        }
    }

    const handleCopy = () => {
        if (createdKey) {
            navigator.clipboard.writeText(createdKey)
            setCopied(true)
            addToast('Copied to clipboard')
        }
    }

    return (
        <div className={styles.container}>
            <h3 className={styles.title}>API Keys</h3>
            <p className={styles.description}>
                API keys let the dante-for-data Python library authenticate with this server.
                Keys are shown once on creation and cannot be retrieved later.
            </p>

            {/* Create new key */}
            <div className={styles.createRow}>
                <input
                    value={newKeyName}
                    onChange={e => setNewKeyName(e.target.value)}
                    placeholder="Key name (e.g. laptop, notebook server)"
                    className={styles.input}
                />
                <button onClick={handleCreate} disabled={creating} className={styles.primaryButton}>
                    {creating ? 'Creating...' : 'Create Key'}
                </button>
            </div>

            {/* Newly created key banner */}
            {createdKey && (
                <div className={styles.keyBanner}>
                    <p className={styles.keyBannerTitle}>Your new API key (copy it now — it won't be shown again):</p>
                    <div className={styles.keyRow}>
                        <code className={styles.keyValue}>{createdKey}</code>
                        <button onClick={handleCopy} className={styles.copyButton}>
                            {copied ? 'Copied' : 'Copy'}
                        </button>
                    </div>
                    <p className={styles.keyBannerHint}>
                        Add to ~/.dante/config.yaml: <code>remote: {'{'} api_url: ..., api_key: {createdKey.slice(0, 8)}... {'}'}</code>
                    </p>
                    <button onClick={() => setCreatedKey(null)} className={styles.dismissButton}>Dismiss</button>
                </div>
            )}

            {/* Existing keys */}
            {keys.length > 0 ? (
                <table className={styles.table}>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Last Used</th>
                            <th>Created</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>
                        {keys.map(k => (
                            <tr key={k.id}>
                                <td>{k.name || 'default'}</td>
                                <td className={styles.dateCell}>
                                    {k.last_used ? new Date(k.last_used).toLocaleDateString() : 'Never'}
                                </td>
                                <td className={styles.dateCell}>
                                    {new Date(k.created_at).toLocaleDateString()}
                                </td>
                                <td>
                                    <button onClick={() => handleRevoke(k.id)} className={styles.revokeButton}>
                                        Revoke
                                    </button>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            ) : (
                <p className={styles.emptyText}>No API keys yet</p>
            )}
        </div>
    )
}
```

- [ ] **Step 2: Create the CSS module**

`frontend/src/components/ApiKeyManager.module.css`:
```css
.container { margin-top: 0.5rem; }

.title {
    font-size: 0.875rem;
    font-weight: 600;
    margin: 0 0 0.25rem;
}

.description {
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin: 0 0 0.75rem;
    line-height: 1.4;
}

.createRow {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
}

.input {
    background: var(--bg-secondary);
    border: 1px solid var(--bg-tertiary);
    color: var(--text-primary);
    padding: 0.375rem 0.5rem;
    border-radius: 0.375rem;
    font-size: 0.8125rem;
    font-family: inherit;
    flex: 1;
}

.input:focus { outline: none; border-color: var(--accent); }

.primaryButton {
    background: var(--accent);
    color: #fff;
    border: none;
    padding: 0.375rem 0.75rem;
    border-radius: 0.375rem;
    font-size: 0.8125rem;
    cursor: pointer;
    white-space: nowrap;
}

.primaryButton:disabled { opacity: 0.5; cursor: not-allowed; }

/* Key banner */
.keyBanner {
    background: #1a2a1a;
    border: 1px solid #2a4a2a;
    border-radius: 8px;
    padding: 0.75rem;
    margin-bottom: 0.75rem;
}

.keyBannerTitle {
    font-size: 0.75rem;
    font-weight: 600;
    color: #4ade80;
    margin: 0 0 0.375rem;
}

.keyRow {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.375rem;
}

.keyValue {
    background: var(--bg-primary);
    padding: 0.375rem 0.5rem;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    flex: 1;
    overflow-x: auto;
    white-space: nowrap;
}

.copyButton {
    background: var(--bg-tertiary);
    color: var(--text-primary);
    border: none;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    cursor: pointer;
}

.keyBannerHint {
    font-size: 0.6875rem;
    color: var(--text-secondary);
    margin: 0;
}

.keyBannerHint code {
    font-size: 0.6875rem;
}

.dismissButton {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-size: 0.6875rem;
    cursor: pointer;
    padding: 0;
    margin-top: 0.375rem;
}

.dismissButton:hover { color: var(--text-primary); }

/* Table */
.table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.8125rem;
}

.table th {
    text-align: left;
    padding: 0.375rem 0.5rem;
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 0.75rem;
    border-bottom: 1px solid var(--bg-tertiary);
}

.table td {
    padding: 0.375rem 0.5rem;
    border-bottom: 1px solid var(--bg-tertiary);
}

.dateCell {
    color: var(--text-secondary);
    font-size: 0.75rem;
}

.revokeButton {
    background: none;
    border: 1px solid var(--bg-tertiary);
    color: var(--text-secondary);
    padding: 0.125rem 0.375rem;
    border-radius: 0.25rem;
    cursor: pointer;
    font-size: 0.6875rem;
}

.revokeButton:hover { color: var(--error, #f87171); border-color: var(--error, #f87171); }

.emptyText {
    color: var(--text-secondary);
    font-size: 0.8125rem;
}
```

- [ ] **Step 3: Add to Settings page**

In `frontend/src/pages/SettingsPage.tsx`, in the knowledge section (or add a new `'api-keys'` section), add:

```tsx
import { ApiKeyManager } from '../components/ApiKeyManager'

// Inside the knowledge section content:
<ApiKeyManager />
```

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: add API key management UI in settings"
```

---

## Chunk 4: Library Remote Client Update

### Task 10: Update remote.py glossary paths + edit method (dante-for-data)

The `remote.py` client already exists at `c:/Users/seang/dante-for-data/src/dante/remote.py` with full test coverage. Two changes needed:

1. The spec uses `/knowledge/glossary` for glossary endpoints, but the library uses `/knowledge/terms`.
2. The library's `edit_pattern` already uses `PATCH` which now matches the backend (changed from spec's `PUT`).

**URL convention:** The library sends paths like `/knowledge/search`. The backend router is at `/api/knowledge/`. So the user's `api_url` config must end with `/api`, e.g. `api_url: https://dante.mycompany.com/api`. This is already the convention — the library prepends `self.api_url` + path.

**Files:**
- Modify: `c:/Users/seang/dante-for-data/src/dante/remote.py` (lines 185-199)
- Modify: `c:/Users/seang/dante-for-data/tests/test_remote.py` (lines 347-421)

- [ ] **Step 1: Update remote.py glossary paths**

In `src/dante/remote.py`, change the three glossary methods:

```python
    # Line 188: Change "/knowledge/terms" → "/knowledge/glossary"
    def define_term(self, term: str, definition: str) -> dict:
        """Add or update a glossary term in the remote knowledge base."""
        payload = {"term": term, "definition": definition}
        result = self._request("/knowledge/glossary", method="POST", body=payload)
        return result if isinstance(result, dict) else {}

    # Line 193: Change "/knowledge/terms" → "/knowledge/glossary"
    def list_terms(self, limit: int = 50, offset: int = 0) -> list[dict]:
        """List glossary terms from the remote knowledge base."""
        result = self._request(
            f"/knowledge/glossary?limit={limit}&offset={offset}", method="GET"
        )
        return result if isinstance(result, list) else []

    # Line 199: Change "/knowledge/terms" → "/knowledge/glossary"
    def undefine_term(self, term: str) -> bool:
        """Remove a glossary term from the remote knowledge base."""
        try:
            self._request(f"/knowledge/glossary/{term}", method="DELETE")
            return True
        except ConnectionError:
            return False
```

- [ ] **Step 2: Update test assertions**

In `tests/test_remote.py`, update the URL assertions:

```python
    # test_define_term_calls_correct_url (line 354):
    #   Change "https://api.example.com/knowledge/terms"
    #   →      "https://api.example.com/knowledge/glossary"

    # test_list_terms_calls_correct_url (line 376):
    #   Change "https://api.example.com/knowledge/terms?limit=50&offset=0"
    #   →      "https://api.example.com/knowledge/glossary?limit=50&offset=0"

    # test_list_terms_with_custom_pagination (line 384):
    #   No URL assertion to change — just verifies params in URL

    # test_undefine_term_calls_correct_url (line 401):
    #   Change "https://api.example.com/knowledge/terms/ARR"
    #   →      "https://api.example.com/knowledge/glossary/ARR"
```

- [ ] **Step 3: Run tests**

```bash
cd c:/Users/seang/dante-for-data
pytest tests/test_remote.py -v
```

Expected: All 20+ tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/dante/remote.py tests/test_remote.py
git commit -m "feat: update remote client glossary paths from /terms to /glossary"
```

---

## Chunk 5: Context Builder + Integration Tests

### Task 11: Update chat context builder to respect status

**Files:**
- Modify: `c:/Users/seang/dante-studio/backend/app/agents/context_builder.py`

- [ ] **Step 1: Filter deprecated patterns from context injection**

In the function that gathers semantic context (likely `_gather_semantic_context` or similar), add a filter after results are returned:

```python
results = [r for r in results if r.get("status", "validated") != "deprecated"]
```

Or modify the vectorstore query in `similarity.py` to add `WHERE status != 'deprecated'` if it doesn't already filter by `is_hidden`.

- [ ] **Step 2: Commit**

```bash
cd c:/Users/seang/dante-studio
git add -A && git commit -m "fix: exclude deprecated patterns from chat context injection"
```

---

### Task 12: Integration test

**Files:**
- Create: `c:/Users/seang/dante-studio/backend/tests/test_knowledge_integration.py`

- [ ] **Step 1: Write integration test**

Test the full flow:
1. Create a pattern via POST `/api/knowledge/patterns`
2. Search for it via POST `/api/knowledge/search`
3. Update it via PUT `/api/knowledge/patterns/:id`
4. Check history has 2 entries (create + update)
5. Deprecate it (PATCH `/api/knowledge/patterns/:id/status`)
6. Verify it's excluded from default listing
7. Revert the deprecation (POST `/api/knowledge/history/:id/revert`)
8. Verify it's back in listing

Also test glossary flow:
1. Define a term
2. Update it
3. Verify history
4. Delete it (admin)

- [ ] **Step 2: Run full test suite**

```bash
cd c:/Users/seang/dante-studio/backend
pytest tests/ -v
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test: add knowledge API integration tests"
```

---

## Summary

| Chunk | Tasks | What it delivers |
|-------|-------|-----------------|
| 1 | 1-3 | Fork + database schema ready |
| 2 | 4-7 | Full backend API with admin enforcement (patterns, glossary, history, analytics, API keys) |
| 3 | 8-9 | Frontend knowledge dashboard + API key management UI |
| 4 | 10 | Library remote client aligned with new glossary paths |
| 5 | 11-12 | Chat integration, integration tests |

### Key changes from v1 of this plan

1. **Admin role enforcement is inline** — `_admin: AdminUser` parameter on delete, deprecate, promote, revert, and all analytics endpoints. No TODO comments.
2. **Task 10 is a 3-line delta** on the existing `remote.py` instead of a full rewrite. The existing client is well-tested with 35+ tests.
3. **Tasks 8-9 have full React + CSS code** following the codebase's exact patterns: CSS Modules, `useToast`, `useCallback` for data loading, `api` namespace, camelCase class names, `var(--*)` theming.
4. **Embedding vector generation** is wired into `create_pattern` and `update_pattern` from the start.
5. **`/knowledge/stats` endpoint** added for the library's `stats()` method.
6. **Backend `update_pattern`** uses `PATCH` (not `PUT`) to match the existing library client.
7. **Overview tab** guards analytics calls behind `isAdmin` to avoid 403 errors for non-admin users.

### Known test gaps (acceptable for v1, address in follow-up)

- No dedicated unit tests for `app/api/api_keys.py` endpoints (tested indirectly via integration)
- No test for the `dependencies.py` API key auth fallback (Bearer token → user_id resolution)
- No frontend component tests (spec mentions them, but deferred to stabilization phase)

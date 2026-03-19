# Dante Cloud — Centralized Knowledge Service Design

**Date:** 2026-03-15
**Status:** Draft
**Author:** Sean + Claude

## Problem

A data science team's SQL knowledge is trapped in individual analysts' heads and local files. When one analyst figures out the right way to calculate churn, that knowledge doesn't spread. New hires start from zero. The team's collective knowledge grows linearly with headcount instead of compounding.

## Solution

A centralized knowledge service built on top of the existing Dante web application (`../dante`). One Postgres database. Two clients (web app + Python library) that talk to the same API. Full change history with revert capability.

## Business Model

- `dante-for-data` Python library: free, open source, works fully offline with local storage
- Dante Cloud (this product): paid SaaS, per-customer deployment, adds shared team knowledge

When an analyst configures `api_url` + `api_key`, their library calls the central API instead of local storage. When not configured, everything works locally as today.

## Architecture

```
┌──────────────┐     ┌──────────────┐
│  Web App     │     │  Python Lib  │
│  (React)     │     │  (dante-ds)  │
└──────┬───────┘     └──────┬───────┘
       │                    │
       │  session auth      │  API key auth
       │                    │
       ▼                    ▼
┌──────────────────────────────────┐
│  FastAPI Backend                 │
│  (auth, business logic, history) │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  Postgres + pgvector             │
│  (patterns, glossary, embeddings,│
│   change history)                │
└──────────────────────────────────┘
```

### Key Decisions

- **Single-tenant:** One deployment per customer. No org_id, no multi-tenancy plumbing. Matches the existing `../dante` architecture.
- **One central DB:** No sync, no local cache, no distributed systems. The library is just an HTTP client. If latency ever matters, add caching later.
- **Two auth modes:** Web app uses existing session/cookie auth. Library uses API key auth (new, simple bearer token).
- **Offline fallback:** Library without `api_url` configured works locally as it does today (free version). The two modes don't mix — you're either local or remote.
- **Fork, not feature flag:** `../dante` is copied to a new `dante-studio` project and the two diverge. The original `../dante` stays untouched as a reference. Dante Cloud is the product going forward.

## Deployment Model

Each customer gets their own deployment:
- Their own Postgres instance (with pgvector)
- Their own FastAPI backend
- Their own React frontend
- Deployed via Docker Compose (existing setup), later Kubernetes or managed platform

## User Roles

Two roles (already exist in `../dante`). Note: the existing backend allows any user to delete embeddings — we are tightening this so only admins can delete/deprecate.

| Capability | Analyst | Admin |
|-----------|---------|-------|
| Search patterns | Yes | Yes |
| Save/edit patterns | Yes | Yes |
| Delete/deprecate patterns | No | Yes |
| Bulk merge/sync embeddings | No | Yes |
| Edit glossary definitions | Yes | Yes |
| Delete glossary terms | No | Yes |
| View own usage | Yes | Yes |
| View org-wide usage analytics | No | Yes |
| Revert changes | No | Yes |

## Relationship to Existing Embeddings Table

The existing `../dante` backend has an `embeddings` table (model: `Embedding` in `domain/knowledge.py`) that stores question, SQL, description, vector, source, metadata, and management flags (`is_hidden`, `is_locked`, `view_count`). It also has a full API (`/api/embeddings`) with CRUD, search, lock/unlock, hide/show, and regenerate operations.

**We are NOT creating a separate `knowledge_patterns` table.** Instead, we extend the existing `embeddings` table with:
- `status` column: `draft`, `validated`, `promoted`, `deprecated` (replaces the boolean `is_hidden`)
- `use_count` column: tracks how many times a pattern was returned in search results
- `last_used` column: timestamp of last search hit

The existing `embeddings` table already has `source` tracking (`manual`, `feedback`, `dashboard`, `query_history`, `looker`, `databricks`), `view_count`, and vector storage. The knowledge API endpoints are a new router that wraps the existing embeddings infrastructure with the added history/status/analytics layer.

The existing `notes` and `keywords` tables remain as-is — they are used by the chat context system and are separate from the pattern/glossary knowledge base. They are NOT exposed via the library's remote client in v1.

## Components

### 1. Knowledge API Endpoints (Backend)

New router at `/api/knowledge/` (matches existing convention — no version prefix, consistent with `/api/embeddings`, `/api/notes`, etc.). All endpoints accept both session auth and API key auth.

All list endpoints support pagination via `?offset=0&limit=50` parameters.

```
# Patterns (wraps existing embeddings with status/history layer)
POST   /api/knowledge/patterns              Create pattern
GET    /api/knowledge/patterns              List patterns (filters: status, author, table; pagination)
GET    /api/knowledge/patterns/:id          Get single pattern
PUT    /api/knowledge/patterns/:id          Update pattern
DELETE /api/knowledge/patterns/:id          Delete pattern (admin only)
PATCH  /api/knowledge/patterns/:id/status   Deprecate/promote/restore (admin only)

# Search (semantic + keyword, logs every query for gap analysis)
POST   /api/knowledge/search               Search patterns

# Glossary (new table, new concept for the backend)
POST   /api/knowledge/glossary             Define term
GET    /api/knowledge/glossary             List terms (pagination)
PUT    /api/knowledge/glossary/:term       Update term
DELETE /api/knowledge/glossary/:term       Delete term (admin only)

# Embeddings management (wraps existing merger service)
POST   /api/knowledge/embeddings/sync      Re-embed all patterns (admin only)
POST   /api/knowledge/embeddings/merge     Merge duplicate patterns (admin only)

# History
GET    /api/knowledge/history              List changes (filters: entity, user, date; pagination)
POST   /api/knowledge/history/:id/revert   Revert to previous state (admin only)

# Usage analytics (admin only)
GET    /api/knowledge/analytics/patterns   Most/least used patterns
GET    /api/knowledge/analytics/gaps       Zero-result searches
GET    /api/knowledge/analytics/activity   Team activity over time

# API keys (for library auth — managed in web app settings)
POST   /api/auth/api-keys                  Generate API key
GET    /api/auth/api-keys                  List user's API keys
DELETE /api/auth/api-keys/:id             Revoke API key
```

### 2. Database Changes

Modifications to existing tables + new tables. All via Alembic migrations.

**Modifications to existing `embeddings` table:**

```sql
-- Add status lifecycle (replaces is_hidden boolean)
-- Note: this applies to the CLOUD BACKEND's embeddings table (Postgres),
-- not the library's local SQLite table.
ALTER TABLE embeddings ADD COLUMN status TEXT NOT NULL DEFAULT 'validated'
    CHECK (status IN ('draft', 'validated', 'promoted', 'deprecated'));
ALTER TABLE embeddings ADD COLUMN use_count INT DEFAULT 0;
ALTER TABLE embeddings ADD COLUMN last_used TIMESTAMPTZ;

-- Migrate existing data: is_hidden=true → status='deprecated'
UPDATE embeddings SET status = 'deprecated' WHERE is_hidden = true;
UPDATE embeddings SET status = 'validated' WHERE is_hidden = false OR is_hidden IS NULL;
```

**New tables:**

```sql
-- Glossary (new — backend had notes and keywords but no glossary)
CREATE TABLE knowledge_glossary (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    term        TEXT NOT NULL,          -- case-insensitive unique via index below
    definition  TEXT NOT NULL,
    author_id   VARCHAR(36) REFERENCES users(id),
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Change history (audit log for all knowledge mutations)
CREATE TABLE knowledge_history (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type   TEXT NOT NULL,       -- 'pattern' or 'term'
    entity_id     UUID NOT NULL,
    action        TEXT NOT NULL,
        -- create, update, delete, deprecate, promote, restore, revert
    before_state  JSONB,               -- null on create
    after_state   JSONB,               -- null on delete
    user_id       VARCHAR(36) REFERENCES users(id),
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- Search log (for gap analysis and usage analytics)
CREATE TABLE knowledge_search_log (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query         TEXT NOT NULL,
    result_count  INT NOT NULL,
    top_result_id UUID,                -- FK to embeddings if any
    user_id       VARCHAR(36) REFERENCES users(id),
    source        TEXT DEFAULT 'web',  -- 'web' or 'library'
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- API keys (for library authentication)
CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     VARCHAR(36) REFERENCES users(id),
    key_hash    TEXT NOT NULL,          -- bcrypt hash of the key
    name        TEXT,                   -- friendly name ("laptop", "notebook server")
    revoked_at  TIMESTAMPTZ,           -- null = active, set = revoked (soft delete)
    last_used   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- Indexes
CREATE UNIQUE INDEX ON knowledge_glossary (lower(term));
CREATE INDEX ON embeddings (status);
CREATE INDEX ON knowledge_history (entity_type, entity_id);
CREATE INDEX ON knowledge_history (created_at);
CREATE INDEX ON knowledge_search_log (created_at);
CREATE INDEX ON api_keys (key_hash);
```

Note: The existing `embeddings` table already has a pgvector column with indexing managed by the vectorstore module. No changes needed there.

### 3. Admin Knowledge Pages (Frontend)

New pages in the existing React frontend. Accessible to all authenticated users, but destructive actions (delete, deprecate, revert, merge) are gated to admin role via API checks.

**Knowledge Dashboard** (`/knowledge`)
- Pattern count by status (validated, promoted, deprecated)
- Top 10 most-used patterns
- Top 10 zero-result searches (knowledge gaps)
- Recent activity feed

**Pattern Manager** (`/knowledge/patterns`)
- Table of all patterns: question, SQL preview, status, use count, author, last used
- Sort by: usage, recency, status
- Filter by: status, author, table name
- Actions: edit, deprecate, promote, delete, view history

**Pattern Detail** (`/knowledge/patterns/:id`)
- Full question, SQL, tables, description
- Edit form
- Change history timeline with revert buttons (admin only)
- Usage chart over time

**Glossary Manager** (`/knowledge/glossary`)
- Table of all terms: term, definition, author, last updated
- Inline edit
- Delete with confirmation (admin only)
- Change history per term

**Embedding Tools** (`/knowledge/embeddings`) (admin only)
- Re-sync all embeddings (regenerate vectors)
- Merge duplicate patterns (reuses existing `embedding_merger.py` service)
- Embedding model info and stats

### 4. Library Changes (`dante-for-data`)

New module: `src/dante/remote.py` — thin HTTP client, no new dependencies (uses `urllib.request` from stdlib).

```python
class RemoteKnowledge:
    """HTTP client for Dante Cloud knowledge API."""

    def __init__(self, api_url: str, api_key: str): ...

    # Patterns
    def search(self, query: str, top_k: int = 10) -> list[dict]: ...
    def save_pattern(self, question, sql, tables, description) -> dict: ...
    def list_patterns(self, status=None, limit=50, offset=0) -> list[dict]: ...
    def edit_pattern(self, pattern_id, **updates) -> dict: ...
    def delete_pattern(self, pattern_id) -> bool: ...

    # Glossary
    def define_term(self, term, definition) -> dict: ...
    def list_terms(self, limit=50, offset=0) -> list[dict]: ...
    def undefine_term(self, term) -> bool: ...

    # Stats
    def stats(self) -> dict: ...
```

Each existing knowledge function gets a remote/local switch:

```python
def search(query, top_k=10, root=None):
    remote = _get_remote_client()  # returns None if not configured
    if remote:
        return remote.search(query, top_k)
    return _local_search(query, top_k, root)
```

Configuration in `~/.dante/config.yaml` (global, applies to all projects):
```yaml
remote:
  api_url: https://dante.mycompany.com
  api_key: dk_abc123...
```

**Local-to-remote migration:** When a user configures remote for the first time, local patterns are NOT automatically uploaded. They can use `dante.knowledge.upload_local()` as an explicit one-time migration if desired. This is a convenience function, not automatic.

### 5. API Key Authentication

New auth middleware that accepts bearer tokens alongside existing session auth:

```python
# Backend: check for API key in Authorization header
# Authorization: Bearer dk_abc123...
#
# 1. Hash the provided key
# 2. Look up in api_keys table
# 3. Resolve to user_id
# 4. Inject user into request (same as session auth does)
# 5. Update last_used timestamp
#
# If no API key header, fall through to existing session auth
```

API keys are generated in the web app settings page. Users can have multiple keys. Keys can be revoked.

**Rate limiting:** API key requests share the existing per-user rate limits (daily token budget, embedding rate limiter). No separate limits needed — the API key resolves to a user, and that user's limits apply.

### 6. Chat Integration

The existing chat agent's context builder (`agents/context_builder.py`) already pulls from the `embeddings` table to inject relevant SQL patterns into the LLM prompt. Since we are extending the existing `embeddings` table (not creating a new one), **the chat feature works with the new status/lifecycle fields automatically.** The only change: filter out `status = 'deprecated'` patterns from context injection, same as the current `is_hidden` filter.

## What We're NOT Building (v1)

- Multi-tenancy / org_id — single-tenant, one deployment per customer
- Team management / team-scoped visibility — all knowledge is org-wide
- Real-time push notifications when knowledge changes
- Local caching / sync in the library — just direct API calls
- Pattern auto-deprecation based on schema drift
- Slack/Teams integrations
- Usage-weighted search ranking (use_count is tracked but doesn't affect ranking yet)
- Remote access to notes/keywords from the library — these stay local or web-app-only

## Migration Path from Existing `../dante`

1. Copy `../dante` to new `dante-studio` project (fork — the two diverge from here)
2. Add Alembic migrations: extend `embeddings` table, add new tables
3. Migrate existing data: `is_hidden` → `status`, backfill defaults
4. Add `/api/knowledge/` router (new FastAPI router)
5. Add `/api/auth/api-keys` endpoints
6. Add API key auth middleware to `dependencies.py`
7. Add admin knowledge pages to React frontend
8. Add `remote.py` to `dante-for-data` library
9. Existing features (chat, SQL, notebooks, Data Apps) remain untouched

## Testing Strategy

- Backend: pytest for all new API endpoints, history logging, revert logic, role-based access control
- Backend: test API key auth alongside session auth
- Frontend: component tests for admin pages
- Library: test `RemoteKnowledge` client with mocked HTTP responses
- Library: test local/remote switching based on config
- Integration: end-to-end test of library → API → database → search round-trip

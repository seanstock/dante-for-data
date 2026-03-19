# Dante Studio

Dante is an AI-powered analytics platform that lets users query data warehouses, generate visualizations, and build shareable Data Apps through a conversational chat interface. It combines a FastAPI backend with a React frontend, backed by PostgreSQL with pgvector for semantic search.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.11+, FastAPI, SQLAlchemy 2.0 (async), Uvicorn |
| Frontend | React 18, TypeScript, Vite |
| Database | PostgreSQL 16 + pgvector |
| AI | Claude (via claude-agent-sdk), OpenAI embeddings (text-embedding-3-large) |
| Tools Protocol | MCP (Model Context Protocol) via fastmcp |
| Code Editor | CodeMirror 6 |
| Charts | Plotly.js |
| Auth | Google OAuth, dev-mode login |
| Deployment | Docker Compose |

---

## Architecture Overview

### Dual Database Design

- **App DB** (`dante`): Users, conversations, knowledge base, credentials, data apps
- **Warehouse DB** (`dante_warehouse`): Read-only customer/business data for analytics

Separate async connection pools keep user operations from blocking analytics queries.

### Agent System

The core AI is a Claude agent (claude-agent-sdk) with streaming tool use over WebSocket. The agent has access to tools exposed via an internal MCP server:

| Tool | Purpose |
|------|---------|
| `execute_sql` | Read-only SQL against the warehouse |
| `list_tables` | Browse available tables with row counts |
| `describe_table` | Schema, sample values, foreign keys |
| `execute_python` | Run Python in a sandboxed subprocess |
| `create_chart` | Generate Plotly visualizations |
| `search_knowledge_base` | Semantic search over embeddings |
| `create_data_app` | Create data app from template |
| `update_data_app` | Edit data app HTML/CSS/JS |
| `create_notebook` | Create Jupyter-style notebook |
| `execute_notebook_cell` | Run cell in IPython kernel |

Optional external MCP servers provide Databricks, Looker, Google, and Statsig integrations.

### Advanced Cognition (Experimental)

A multi-agent orchestrator can coordinate Data Engineer, Data Scientist, and Analyst roles for complex analytical tasks. Located in `backend/app/agents/advanced_cognition/`.

---

## Backend Structure

```
backend/
├── app/
│   ├── main.py                  # FastAPI entrypoint
│   ├── config.py                # Pydantic settings
│   ├── dependencies.py          # DI for FastAPI
│   ├── api/                     # HTTP + WebSocket endpoints
│   │   ├── chat.py              # Chat streaming + agent orchestration
│   │   ├── data_apps.py         # Data App CRUD + rendering
│   │   ├── conversations.py     # Conversation management
│   │   ├── embeddings.py        # Embedding management
│   │   ├── feedback.py          # User feedback → embeddings
│   │   ├── keywords.py          # Knowledge keywords
│   │   ├── notes.py             # Knowledge notes
│   │   ├── notebooks.py         # Jupyter notebook management
│   │   ├── python.py            # Direct Python execution
│   │   ├── auth.py              # Authentication
│   │   ├── users.py, admin.py   # User management
│   │   ├── credentials.py       # Encrypted credential storage
│   │   ├── usage.py             # Token usage tracking
│   │   └── ws.py                # WebSocket manager
│   ├── agents/                  # Claude Agent SDK integration
│   │   ├── base.py              # ChatAgent class + stream events
│   │   ├── context.py           # DanteContext runtime state
│   │   ├── context_builder.py   # System prompt assembly
│   │   ├── tool_server.py       # MCP tool wrappers
│   │   ├── mcp_manager.py       # MCP server registry
│   │   ├── model_factory.py     # Model selection + thinking config
│   │   └── advanced_cognition/  # Multi-agent orchestrator
│   ├── tools/                   # Agent-callable tool implementations
│   ├── domain/                  # SQLAlchemy ORM models
│   ├── storage/                 # DB connectivity (app + warehouse)
│   ├── vectorstore/             # OpenAI embedding client + pgvector similarity
│   ├── services/                # Business logic (rendering, embeddings, etc.)
│   ├── sandbox/                 # Python code execution (subprocess or Docker)
│   ├── middleware/              # Rate limiting
│   ├── jobs/                    # Background tasks (refresh, cleanup)
│   └── auth/                    # Session + RBAC
├── alembic/                     # 5 database migrations
├── mcp_servers/                 # External MCP (Databricks)
└── tests/
```

---

## Frontend Structure

```
frontend/src/
├── App.tsx                      # Root (auth check + routing)
├── api/
│   ├── client.ts                # HTTP client (fetch wrapper)
│   └── types.ts                 # TypeScript interfaces
├── components/
│   ├── ChatWindow.tsx           # Main chat interface
│   ├── ChatInput.tsx            # Message input
│   ├── ChatMessage.tsx          # Message rendering (markdown, tool calls)
│   ├── DataAppEditor.tsx        # 4-pane editor (explorer, code, preview, chat)
│   ├── DataAppViewer.tsx        # Read-only data app render
│   ├── NotebookEditor.tsx       # Jupyter-style notebook
│   ├── KnowledgePanel.tsx       # Notes + keywords management
│   ├── EmbeddingGenerator.tsx   # Bulk embedding UI
│   ├── ExecutionLog.tsx         # Tool call log
│   ├── PlotlyChart.tsx          # Chart wrapper
│   └── ...
├── pages/
│   ├── ChatPage.tsx             # Main layout (sidebar + content area)
│   ├── LoginPage.tsx            # Auth
│   ├── SettingsPage.tsx         # User settings
│   └── AdminUsersPage.tsx       # Admin panel
├── hooks/
│   ├── useChatStream.ts         # WebSocket streaming
│   ├── useWebSocket.ts          # Generic WS hook
│   ├── useExecutionLog.ts       # Tool execution tracking
│   └── useJobPolling.ts         # Async job polling
└── constants/
    ├── dataAppTemplates.ts      # Template definitions + CSS/JS
    └── toolLabels.ts            # Tool display names
```

### Key UI Features

- **Sidebar tabs**: Conversations, Data Apps, Notebooks, Execution Log
- **Streaming chat**: Real-time token display, inline tool calls, thinking indicators
- **Data App Editor**: 4-pane layout (table explorer, HTML/CSS/JS code, live preview, AI chat assistant)
- **Notebook Editor**: Code + markdown cells, per-notebook IPython kernel, execution outputs
- **Knowledge Panel**: Notes, keywords (with file attachments), embedding browser
- **No Redux**: State collocated with components via React hooks

---

## Database Schema

### Core Tables

| Table | Purpose |
|-------|---------|
| `users` | Accounts (email, role, auth provider, token budget) |
| `conversations` | Chat threads per user |
| `messages` | Messages with role, content, tool_calls JSON |
| `sessions` | Auth sessions (7-day expiry) |

### Knowledge Base

| Table | Purpose |
|-------|---------|
| `notes` | Short text (org or personal scope) |
| `keywords` | Trigger-based context injection |
| `keyword_files` | Attached files for keywords |
| `embeddings` | pgvector 1536-dim vectors with SQL patterns |

### Data Apps

| Table | Purpose |
|-------|---------|
| `data_apps` | HTML/CSS/JS + computed values, slug for sharing |
| `data_app_queries` | SQL or Python queries bound to template slots |
| `data_app_snapshots` | Immutable snapshots of rendered state |

### Notebooks

| Table | Purpose |
|-------|---------|
| `notebooks` | Notebook metadata + kernel tracking |
| `notebook_cells` | Code/markdown cells with outputs |

### System

| Table | Purpose |
|-------|---------|
| `usage_logs` | Token usage per user (input/output/cache) |
| `credentials` | Fernet-encrypted service credentials |
| `embedding_jobs` | Bulk embedding generation tracking |

---

## Knowledge Base System

Three tiers of knowledge injection into the agent's context:

1. **Notes** - Short definitions, acronyms, conventions. Searched by keyword during context building.
2. **Keywords** - Trigger strings that inject context (text or attached files like CSV/JSON/PDF).
3. **Embeddings** - Semantic search over validated SQL patterns. Generated from warehouse tables, Looker dashboards, or Databricks notebooks. Uses OpenAI `text-embedding-3-large` stored in pgvector.

### Feedback Loop

When a user gives thumbs-up on a SQL query result, it triggers async embedding generation. The new embedding is added to the knowledge base so future conversations can retrieve it.

---

## Data Apps

Shareable, regeneratable HTML dashboards built from SQL/Python queries.

**Templates**: Dashboard, Report, Map, Profile, Blank

**Lifecycle**:
1. Create from template
2. Edit HTML with `{SLOT_NAME}` placeholders in the 4-pane editor
3. Bind SQL/Python queries to slots via `data_app_queries`
4. Regenerate: executes all queries, substitutes results into HTML, creates snapshot
5. Publish with a slug URL (`/app/{slug}`) for public access
6. Optionally lock (freeze state) or schedule regeneration (cron)

---

## API Endpoints (40+)

| Group | Key Routes |
|-------|-----------|
| Chat | `WS /api/chat/stream`, `POST /api/chat/message` |
| Conversations | CRUD at `/api/conversations` |
| Data Apps | CRUD at `/api/data-apps`, `POST .../render`, `GET /app/{slug}` |
| Notebooks | CRUD at `/api/notebooks`, `POST .../execute` |
| Knowledge | CRUD for `/api/notes`, `/api/keywords`, `/api/embeddings` |
| Auth | `/api/auth/dev-login`, `/api/auth/google`, `/api/auth/me` |
| Health | `/health`, `/health/db`, `/health/warehouse` |

---

## Configuration

### Required Environment Variables

```
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dante
WAREHOUSE_DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/warehouse
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
```

### Notable Optional Variables

```
LLM_MODEL=claude-sonnet-4-6
LLM_REASONING_EFFORT=medium
AUTH_MODE=dev|google|both
SQL_ROW_LIMIT=10000
SQL_TIMEOUT_SECONDS=60
PYTHON_SANDBOX_MODE=subprocess|docker
PYTHON_SANDBOX_TIMEOUT=120
DEFAULT_DAILY_TOKEN_BUDGET=500000
PARQUET_DIRECTORY=/path/to/parquet/files
```

---

## Deployment

Docker Compose runs PostgreSQL 16 with pgvector, tuned for analytics workloads:

```yaml
services:
  postgres:
    image: pgvector/pgvector:pg16
    command:
      - postgres
      - -c shared_buffers=1GB
      - -c work_mem=256MB
      - -c effective_cache_size=3GB
      - -c max_parallel_workers=8
```

### Dev Setup

```bash
# 1. Database
docker compose up -d postgres

# 2. Backend
cd backend && python -m venv .venv && pip install -e ".[dev]"
alembic upgrade head
uvicorn app.main:app --reload

# 3. Frontend
cd frontend && npm install && npm run dev
```

---

## Notable Fixes

### SDK Stream Timeout

The Claude Agent SDK default stream close timeout (60s) killed MCP tool communication for long conversations. Fixed by setting `CLAUDE_CODE_STREAM_CLOSE_TIMEOUT=1200000` (20 min) at process start.

### Windows UTF-8

Claude generates Unicode characters (em dashes) in Python comments. Windows CP1252 encoding caused syntax errors. Fixed by prepending `# -*- coding: utf-8 -*-`, using explicit `encoding="utf-8"` on file writes, and setting `PYTHONUTF8=1`.

---

## Key Patterns

- **Async everywhere**: All DB operations non-blocking via asyncpg + SQLAlchemy async
- **Streaming**: WebSocket events for real-time token display, tool calls, and results
- **MCP tool protocol**: Tools are async functions on an MCP server, passed to the agent SDK
- **DI via FastAPI**: Database sessions, user IDs, settings injected as typed dependencies
- **Rate limiting**: Per-user daily/monthly token budgets enforced before agent runs
- **Fernet encryption**: Service credentials encrypted at rest

---

## Stats

- ~90 backend Python files, ~50 frontend TypeScript files
- ~20,000 lines backend, ~15,000 lines frontend
- 5 Alembic migrations, 20+ database tables
- 10+ internal MCP tools, 4+ optional external MCP servers

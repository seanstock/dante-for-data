# Dante: Data Science Workbench for Claude Code

**Package:** `dante-ds` v0.2.0
**Purpose:** MCP server + skills that turn Claude Code into a conversational data science workbench — query databases, build charts/dashboards, and manage a persistent knowledge base, all from the terminal.

---

## Architecture Overview

```
Claude Code (MCP Client)
        ↓ stdio
┌───────────────────────────────────────┐
│         MCP Server (14 tools)         │
├───────────┬───────────┬───────────────┤
│ SQL Tools │ Chart/App │ Knowledge     │
│ (query,   │ (Plotly,  │ (patterns,    │
│  describe,│  HTML     │  glossary,    │
│  profile) │  dashbd)  │  embeddings)  │
├───────────┴───────────┴───────────────┤
│          Core Modules                 │
│  connect.py · query.py · chart.py     │
│  app.py · analyze.py · config.py      │
├───────────────────────────────────────┤
│          Storage (.dante/)            │
│  config.yaml · embeddings.db          │
│  patterns/*.sql · terms.yaml          │
│  keywords.yaml · notes.md             │
└───────────────────────────────────────┘
        ↓
  SQLAlchemy → PostgreSQL, MySQL, Snowflake,
               Databricks, BigQuery, SQLite
```

---

## Project Structure

```
src/dante/
├── __init__.py          # Public API (sql, tables, describe, profile, chart, etc.)
├── cli.py               # CLI: launch, ui, mcp serve, ingest, status, open
├── config.py            # Global (~/.dante/) and project (.dante/) config I/O
├── connect.py           # SQLAlchemy connection builder with dialect support
├── query.py             # Read-only SQL execution, schema introspection, LIMIT injection
├── chart.py             # Plotly chart generation (HTML/PNG)
├── app.py               # Data app template engine (dashboard, report, map, profile)
├── analyze.py           # Checkpoint/rollback, report compilation
├── scaffold.py          # Project scaffolding (dirs, .mcp.json, CLAUDE.md, skills)
├── mcp_server.py        # MCP protocol server, tool dispatch
│
├── tools/               # MCP tool implementations
│   ├── sql_tools.py     # dante_sql, dante_tables, dante_describe, dante_profile
│   ├── chart_tools.py   # dante_chart
│   ├── knowledge_tools.py # dante_search, dante_save_pattern, dante_define_term
│   ├── app_tools.py     # dante_app_create, dante_app_add_value, dante_app_set_html, dante_app_render
│   └── analyze_tools.py # dante_checkpoint, dante_rollback
│
├── knowledge/           # 4-layer knowledge system
│   ├── search.py        # Unified search (keywords + embeddings + glossary)
│   ├── patterns.py      # SQL pattern storage (.sql files with YAML frontmatter)
│   ├── glossary.py      # Business term definitions (terms.yaml)
│   ├── keywords.py      # Keyword triggers (keywords.yaml)
│   ├── notes.py         # Free-form markdown notes
│   ├── embeddings.py    # SQLite vector store with cosine similarity
│   └── vectorize.py     # OpenAI embedding generation (text-embedding-3-large)
│
├── ui/                  # Management web UI
│   ├── server.py        # HTTP server with REST API routes
│   └── app.html         # Single-page app (connections, credentials, knowledge, ingest)
│
└── ingest/              # Bulk embedding ingestion pipeline
    ├── __init__.py      # Orchestrator (IngestionConfig, run())
    ├── looker.py        # Ingest from Looker dashboards
    ├── databricks.py    # Ingest from Databricks
    ├── warehouse.py     # Ingest from warehouse schema
    ├── question_gen.py  # Generate natural-language questions via GPT-4o-mini
    └── sql_simplifier.py # Normalize SQL for consistency
```

**Generated project layout** (after `dante launch`):

```
my-project/
├── .mcp.json                    # MCP server config for Claude Code
├── CLAUDE.md                    # Auto-loaded tool reference
├── .claude/skills/              # Slash commands (/query, /dashboard, /analyze, /report, /ingest)
├── .dante/
│   ├── config.yaml              # Project config (default_connection)
│   ├── knowledge/               # Patterns, terms, keywords, notes
│   ├── embeddings.db            # Vector index (SQLite)
│   └── checkpoints/             # Analysis snapshots
├── analysis/                    # Analysis scripts
├── outputs/                     # Charts, dashboards, reports
└── data/                        # Local data files
```

---

## Core Capabilities

### 1. SQL Tools
- **Read-only enforcement** — INSERT/UPDATE/DELETE/DROP are rejected
- **Automatic LIMIT injection** — prevents accidental full-table scans
- **Schema introspection** — list tables, describe columns with sample values, statistical profiling (nulls, cardinality, min/max)
- **6 database dialects** — PostgreSQL, MySQL, Snowflake, Databricks, BigQuery, SQLite

### 2. Knowledge System (4 Layers)

| Layer | Storage | Search Method |
|-------|---------|---------------|
| **Keywords** | `keywords.yaml` | Case-insensitive substring match (fast, no API) |
| **Patterns** | `patterns/*.sql` | Embedding similarity via OpenAI |
| **Glossary** | `terms.yaml` | Substring match on term + definition |
| **Notes** | `notes.md` | Free-form markdown reference |

Search flow: keyword match → embedding similarity → glossary match → merge & deduplicate → return top-k results.

Patterns are stored as `.sql` files with YAML frontmatter containing the question, tables, and description. When saved, an embedding is generated via OpenAI's `text-embedding-3-large` and stored in a local SQLite database for future similarity search.

### 3. Charts & Data Apps
- **Charts** — Plotly-based (bar, line, scatter, pie, heatmap, histogram, box) → HTML or PNG
- **Data Apps** — Template-driven HTML dashboards with 5 templates:
  - `dashboard` — KPI cards + chart grids
  - `report` — Narrative sections with callouts
  - `map` — Location-based panels
  - `profile` — Entity detail with tabs/timeline
  - `blank` — Custom layout

App workflow: create → add SQL-backed values → set HTML with `{SLOT_NAME}` placeholders → render (executes queries, substitutes results, writes standalone HTML).

### 4. Analysis Checkpoints
- **Checkpoint** — snapshots `analysis/` and `outputs/` directories
- **Rollback** — restores from a named checkpoint
- **Report** — compiles analysis scripts and charts into a self-contained HTML report

### 5. Embedding Ingestion
Bulk import SQL patterns from BI platforms:
- **Looker** — pull dashboard titles and underlying SQL
- **Databricks** — query warehouse schema or dashboards
- **Warehouse** — direct schema inspection
- Generates natural-language questions via GPT-4o-mini, creates embeddings, upserts into SQLite

### 6. Management UI
Web-based SPA (`dante ui`) for:
- Database connection setup and testing
- API credential management (OpenAI, Looker, Databricks)
- Knowledge base inspection (patterns, glossary, keywords)
- Embedding ingestion job management

---

## MCP Tools Reference

| # | Tool | Description |
|---|------|-------------|
| 1 | `dante_sql` | Execute read-only SQL, returns markdown table |
| 2 | `dante_tables` | List database tables |
| 3 | `dante_describe` | Column metadata with sample values |
| 4 | `dante_profile` | Statistical profile (nulls, cardinality, distributions) |
| 5 | `dante_chart` | Generate Plotly chart (HTML/PNG) |
| 6 | `dante_search` | Search knowledge base (keywords + embeddings) |
| 7 | `dante_save_pattern` | Save SQL pattern with embedding |
| 8 | `dante_define_term` | Add/update glossary entry |
| 9 | `dante_app_create` | Create data app from template |
| 10 | `dante_app_add_value` | Bind SQL query to app slot |
| 11 | `dante_app_set_html` | Set app HTML with placeholders |
| 12 | `dante_app_render` | Execute queries and render final HTML |
| 13 | `dante_checkpoint` | Save analysis snapshot |
| 14 | `dante_rollback` | Restore from checkpoint |

---

## Python API

```python
import dante

# Query
df = dante.sql("SELECT * FROM orders LIMIT 10")
dante.tables()
dante.describe("orders")
dante.profile("orders")

# Charts
dante.chart(df, x="month", y="revenue", kind="line", title="Monthly Revenue")

# Knowledge
dante.knowledge.search("monthly revenue")
dante.knowledge.save_pattern(question="...", sql="...", tables=["orders"], description="...")

# Analysis
dante.checkpoint("before-pivot")
dante.rollback("before-pivot")
dante.report(title="Revenue Analysis", sections=["analysis/step1.py"], charts=["outputs/chart.html"])
```

---

## CLI Commands

```bash
dante launch [name]          # Scaffold a new project
dante launch --cursor        # Scaffold with Cursor IDE support
dante ui [--port PORT]       # Open management UI (default: 4040)
dante mcp serve              # Start MCP server (used by .mcp.json)
dante ingest [--source ...]  # Run embedding ingestion pipeline
dante status [--json]        # Show project stats
dante open [name]            # Open an artifact in the browser
```

---

## Configuration

**Global** (`~/.dante/`):
- `connections.yaml` — Named database connections (shared across projects)
- `credentials.yaml` — API keys (OpenAI, Looker, Databricks)

**Project** (`.dante/config.yaml`):
- `default_connection` — Which named connection to use

**Environment variables**:
- `OPENAI_API_KEY` — Required for embedding generation/search
- `DANTE_PROJECT` — Project root (set automatically by MCP server)

---

## Dependencies

**Core:** sqlalchemy, pandas, plotly, pyyaml, click, mcp, openai, tenacity, kaleido

**Optional (database drivers):**
- `snowflake-sqlalchemy` — Snowflake
- `databricks-sql-connector` — Databricks
- `sqlalchemy-bigquery` — BigQuery
- `psycopg2-binary` — PostgreSQL
- `pymysql` — MySQL
- `looker-sdk` — Looker API

---

## Key Design Decisions

1. **Read-only SQL** — All queries checked for mutations; protects data integrity
2. **Automatic LIMIT** — Prevents runaway queries
3. **Keywords before embeddings** — Fast fallback when no OpenAI key is available
4. **Pure Python cosine similarity** — No numpy dependency for vector search
5. **In-memory app registry** — Apps built across multiple tool calls without disk persistence
6. **Template CSS classes** — Pre-built styles for professional dashboards without manual CSS
7. **Directory-copy checkpoints** — Simple, reliable snapshot/restore
8. **Global connections** — Database configs reusable across projects
9. **Modular knowledge layers** — Each layer (patterns, glossary, keywords, embeddings) is independent and extensible

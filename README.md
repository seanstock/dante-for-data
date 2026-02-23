# dante-lib

A data science workbench for Claude Code. Gives Claude MCP tools to query databases, build charts, manage knowledge, and create data apps — all from your terminal.

## What it does

- **SQL tools** — execute queries, list tables, describe schemas, profile data
- **Knowledge** — save validated SQL patterns with embeddings, maintain a business glossary, write project notes
- **Charts & apps** — generate Plotly charts and self-contained HTML data apps
- **Embedding ingestion** — bulk import SQL patterns from Looker, Databricks, or your warehouse
- **Management UI** — web UI to configure connections, credentials, and knowledge

## Install

```bash
pip install dante-lib
# or with uv
uv tool install dante-lib
```

For specific database drivers:

```bash
pip install "dante-lib[snowflake]"
pip install "dante-lib[databricks]"
pip install "dante-lib[bigquery]"
pip install "dante-lib[postgres]"
pip install "dante-lib[mysql]"
pip install "dante-lib[all]"   # everything
```

Requires Python 3.11+ and an OpenAI API key for embeddings.

## Quick start

```bash
# Create a new project
dante launch my-project
cd my-project

# Or initialize in an existing directory
dante launch

# Open the management UI to configure your database connection
dante ui
```

Then open Claude Code in the project directory. The `.mcp.json` file is already configured — Claude will have access to all dante tools automatically.

## Project structure

After `dante launch`:

```
my-project/
├── .mcp.json                  # Claude Code MCP config (auto-configured)
├── CLAUDE.md                  # Tool reference loaded by Claude each session
├── .dante/
│   ├── config.yaml            # Default connection
│   ├── knowledge/
│   │   ├── terms.yaml         # Business glossary
│   │   ├── keywords.yaml      # Keyword → SQL hint mappings
│   │   ├── notes.md           # Free-form notes Claude reads each session
│   │   └── patterns/          # Saved SQL patterns (.sql files)
│   └── embeddings.db          # SQLite embedding index (gitignored)
├── .claude/skills/            # Slash commands (/query, /dashboard, /analyze, ...)
├── analysis/                  # Analysis scripts
├── outputs/                   # Generated charts, dashboards, reports
└── data/                      # Local data files
```

## MCP tools

Claude gets these tools when it opens the project:

| Tool | What it does |
|------|-------------|
| `dante_sql` | Execute read-only SQL. Auto-injects LIMIT. Returns markdown table. |
| `dante_tables` | List tables, optionally filter by schema. |
| `dante_describe` | Column names, types, nullability, sample values for a table. |
| `dante_profile` | Row count, null rates, cardinality, distributions. |
| `dante_search` | Semantic search across embeddings + keywords. Returns matching SQL. |
| `dante_save_pattern` | Save validated SQL and generate an embedding for future search. |
| `dante_define_term` | Add or update a business glossary entry. |
| `dante_chart` | Generate a Plotly chart → HTML or PNG. |
| `dante_app_create` | Create a data app from a template (dashboard, report, map, profile, blank). |
| `dante_app_add_value` | Bind a SQL query to a computed value slot in a data app. |
| `dante_app_render` | Execute all queries, substitute values, write final HTML. |
| `dante_checkpoint` | Snapshot `analysis/` and `outputs/` directories. |
| `dante_rollback` | Restore to a previous checkpoint. |

## Slash commands

Skills scaffolded into `.claude/skills/`:

| Command | What it does |
|---------|-------------|
| `/query` | Explore data — searches knowledge first, then explores schema and runs SQL |
| `/dashboard [title]` | Build an interactive HTML dashboard from scratch |
| `/analyze [question]` | Multi-step analysis with checkpoints and a final report |
| `/ingest [--source]` | Run the embedding ingestion pipeline |
| `/report [title]` | Compile analysis scripts and charts into an HTML report |

## Python library

Use `import dante` in analysis scripts for DataFrame-based work:

```python
import dante

df = dante.sql("SELECT * FROM orders LIMIT 100")
dante.tables()
dante.describe("orders")
dante.profile("orders")
dante.chart(df, x="date", y="revenue", kind="bar", title="Revenue")

dante.knowledge.search("monthly revenue by region")
dante.knowledge.save_pattern("monthly_revenue", sql="...", description="...")

dante.checkpoint("before-pivot")
dante.rollback("before-pivot")
```

## Embedding ingestion

Pull SQL patterns from BI platforms into the local embedding index:

```bash
# From the CLI
dante ingest --source looker
dante ingest --source databricks
dante ingest --source all --dry-run

# Or from the management UI
dante ui
```

Ingestion pulls dashboard titles and SQL, generates natural-language questions with GPT-4o-mini, creates OpenAI embeddings, and upserts into `embeddings.db`. Claude then finds these patterns when you call `/query`.

Connections for Looker and Databricks are configured in the management UI and stored in `~/.dante/credentials.yaml` (global, not per-project).

## Management UI

```bash
dante ui           # opens at http://localhost:4040
dante ui --port 8080
```

Configure database connections, API credentials, glossary terms, and keywords. Connections are stored globally (`~/.dante/`) so the same connection works across all your projects.

## CLI reference

```bash
dante launch [name]          # scaffold a new project (or init in place)
dante ui [--port PORT]       # open the management UI
dante mcp serve              # start the MCP server (called by Claude via .mcp.json)
dante ingest [--source ...]  # run embedding ingestion
dante status [--json]        # show connection, knowledge stats, and output count
dante open [name]            # open a generated artifact in the browser
```

## Configuration

**Per-project** (`.dante/config.yaml`):
```yaml
default_connection: my-warehouse
```

**Global** (`~/.dante/connections.yaml`):
```yaml
my-warehouse:
  dialect: snowflake
  account: xy12345
  database: ANALYTICS
  warehouse: COMPUTE_WH
  role: ANALYST
```

**Global credentials** (`~/.dante/credentials.yaml`):
```yaml
openai:
  api_key: sk-...
looker:
  base_url: https://company.looker.com
  client_id: ...
  client_secret: ...
```

## License

MIT

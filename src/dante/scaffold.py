"""Project scaffolding — creates the directory structure, config files, and CLAUDE.md."""

from __future__ import annotations

import json
from pathlib import Path

_CLAUDE_MD = """\
# Dante Data Science Project

You have MCP tools for database queries, charts, dashboards, and knowledge search.
Use MCP tools for interactive work. Use `import dante` for scripted multi-step analysis.

## MCP Tools

| Tool | What it does |
|------|-------------|
| `dante_sql` | Execute read-only SQL. Auto-injects LIMIT. Returns markdown table. |
| `dante_tables` | List tables, optionally filter by schema. |
| `dante_describe` | Column names, types, nullability, sample values for a table. |
| `dante_profile` | Row count, null rates, cardinality, distributions for a table. |
| `dante_search` | Semantic search across embedding index + keywords. Returns matching SQL patterns. |
| `dante_save_pattern` | Save validated SQL + generate embedding for future matching. |
| `dante_define_term` | Add/update a business glossary entry. |
| `dante_chart` | Generate a Plotly chart → HTML or PNG file. |
| `dante_app_create` | Create a Data App from a template (dashboard, report, map, profile, blank). |
| `dante_app_add_value` | Bind a SQL query to a computed value slot in a Data App. |
| `dante_app_render` | Execute all queries, substitute values, write final HTML. |
| `dante_checkpoint` | Snapshot analysis/ and outputs/ directories. |
| `dante_rollback` | Restore to a previous checkpoint. |

## Python Library (`import dante`)

For scripted analysis with DataFrames:

```python
import dante
df = dante.sql("SELECT ...")        # → pandas DataFrame
dante.tables()                      # list tables
dante.describe("orders")            # column metadata
dante.profile("orders")             # statistical profile
dante.chart(df, x=..., y=..., kind="bar", title="...")  # → HTML file
dante.knowledge.search("...")       # semantic search
dante.knowledge.save_pattern(...)   # save validated SQL
dante.checkpoint("name")            # snapshot state
dante.rollback("name")              # restore state
dante.report(title=..., sections=[...], charts=[...])   # → HTML report
```

## Rules

1. Search knowledge first. Before writing SQL, call `dante_search`. If similarity > 0.7, adapt the match.
2. Explore before querying. Call `dante_describe` on unfamiliar tables before writing SQL.
3. Save validated patterns. When the user confirms a result, call `dante_save_pattern`.
4. Never hardcode data into Data Apps. Always use `dante_app_add_value`.
5. Checkpoint before risky steps.
6. One script per analysis step in `analysis/`. Outputs go to `outputs/`. Data goes to `data/`.

## Business Glossary

@.dante/knowledge/terms.yaml

## Notes

@.dante/knowledge/notes.md
"""

_MCP_JSON = {
    "mcpServers": {
        "dante": {
            "command": "dante",
            "args": ["mcp", "serve"],
            "env": {
                "DANTE_PROJECT": ".",
                "OPENAI_API_KEY": "${OPENAI_API_KEY}",
            },
        }
    }
}

_GITIGNORE = """\
# Dante
.dante/connections.yaml
.dante/credentials.yaml
.dante/embeddings.db
.dante/query_log.jsonl
.dante/checkpoints/

# Python
__pycache__/
*.pyc
.venv/
*.egg-info/

# OS
.DS_Store
Thumbs.db
"""

_QUERY_SKILL = """\
---
name: query
description: Explore data by running SQL queries against the connected database
allowed-tools: Bash, dante_sql, dante_tables, dante_describe, dante_profile, dante_search, dante_save_pattern, dante_chart
---

The user wants to explore data. Follow this protocol:

1. Run `dante_search` with the user's question to find matching patterns
2. If a match has similarity > 0.7, adapt its SQL. Otherwise, explore:
   - `dante_tables` to find relevant tables
   - `dante_describe` on candidate tables
3. Write and run the query with `dante_sql`
4. If the user confirms the result is useful, save it: `dante_save_pattern`
5. If they want a visualization, use `dante_chart`

Current status: !`dante status`
"""

_DASHBOARD_SKILL = """\
---
name: dashboard
description: Build an interactive dashboard from data queries
allowed-tools: Bash, dante_sql, dante_search, dante_chart, dante_app_create, dante_app_add_value, dante_app_render
argument-hint: "[title or topic]"
---

Build a Data App dashboard for: $ARGUMENTS

1. Search knowledge for relevant patterns: `dante_search`
2. Explore schema to understand available data
3. Create the app: `dante_app_create` with template "dashboard"
4. Add computed values for each KPI and chart (NEVER hardcode data)
5. Write the HTML using template CSS classes
6. Render and report the output path

Available templates: dashboard, report, map, profile, blank
Dashboard CSS classes: .kpis, .kpi, .kpi-label, .kpi-value, .kpi-change.up/.down, .chart-card, .chart-card.wide, .data-table
"""

_ANALYZE_SKILL = """\
---
name: analyze
description: Run a structured multi-step data analysis with checkpoints
allowed-tools: Bash, Read, Write, dante_sql, dante_tables, dante_describe, dante_profile, dante_search, dante_chart, dante_checkpoint, dante_rollback, dante_save_pattern
argument-hint: "[research question]"
context: fork
---

Run a structured analysis for: $ARGUMENTS

## Protocol
1. **Explore.** `dante_tables`, `dante_describe`, `dante_profile` on relevant tables
2. **Search.** `dante_search` for existing patterns related to this question
3. **Plan.** Write a step-by-step plan to `analysis/plan.md`
4. **Execute.** One `.py` script per step in `analysis/`. Run and verify each.
5. **Checkpoint.** `dante_checkpoint` after each successful step
6. **Iterate.** If stuck, `dante_rollback` and try a different approach
7. **Report.** Compile findings

Current status: !`dante status`
"""

_INGEST_SKILL = """\
---
name: ingest
description: Ingest embeddings from Looker, Databricks, or warehouse schema
disable-model-invocation: true
allowed-tools: Bash
argument-hint: "[--source looker|databricks|warehouse|all]"
---

Run the embedding ingestion pipeline:

```bash
dante ingest $ARGUMENTS
```

Show progress and report results when complete.
"""

_REPORT_SKILL = """\
---
name: report
description: Compile analysis scripts and charts into a self-contained HTML report
allowed-tools: Bash, Read, dante_chart
argument-hint: "[title]"
---

Compile the current analysis into a report titled: $ARGUMENTS

1. Read `analysis/plan.md` for the analysis structure
2. Identify all step scripts in `analysis/` and charts in `outputs/`
3. Compile them into a single HTML report
4. Report the output path
"""


def scaffold_project(name: str, root: Path | None = None) -> Path:
    """Create a new dante-lib project.

    Args:
        name: Project directory name.
        root: Parent directory. Defaults to cwd.

    Returns:
        Path to the created project directory.
    """
    root = root or Path.cwd()
    project = root / name
    project.mkdir(parents=True, exist_ok=True)

    # Create directories
    (project / "analysis").mkdir(exist_ok=True)
    (project / "outputs").mkdir(exist_ok=True)
    (project / "data").mkdir(exist_ok=True)

    # .dante/
    dante_dir = project / ".dante"
    dante_dir.mkdir(exist_ok=True)
    knowledge_dir = dante_dir / "knowledge"
    knowledge_dir.mkdir(exist_ok=True)
    (knowledge_dir / "patterns").mkdir(exist_ok=True)

    # Config
    _write_if_not_exists(dante_dir / "config.yaml", "# default_connection: my-connection\n")
    _write_if_not_exists(knowledge_dir / "terms.yaml", "# Business glossary — add terms here\n# ARR: \"Annual Recurring Revenue. MRR * 12.\"\n")
    _write_if_not_exists(knowledge_dir / "keywords.yaml", "# Keyword triggers — add keywords here\n# revenue: \"Revenue = SUM(amount) from orders table.\"\n")
    _write_if_not_exists(knowledge_dir / "notes.md", "# Project Notes\n\nAdd notes here. Claude sees these at the start of every session.\n")

    # .claude/skills/
    skills_dir = project / ".claude" / "skills"
    _write_skill(skills_dir / "query", _QUERY_SKILL)
    _write_skill(skills_dir / "dashboard", _DASHBOARD_SKILL)
    _write_skill(skills_dir / "analyze", _ANALYZE_SKILL)
    _write_skill(skills_dir / "ingest", _INGEST_SKILL)
    _write_skill(skills_dir / "report", _REPORT_SKILL)

    # Root files
    _write_if_not_exists(project / "CLAUDE.md", _CLAUDE_MD)
    _write_if_not_exists(project / ".mcp.json", json.dumps(_MCP_JSON, indent=2) + "\n")
    _write_if_not_exists(project / ".gitignore", _GITIGNORE)
    _write_if_not_exists(project / "README.md", f"# {name}\n\nA dante-lib data science project.\n")

    return project


def scaffold_in_place(root: Path | None = None) -> Path:
    """Set up dante-lib in an existing directory (no subdirectory created)."""
    root = root or Path.cwd()

    # Create directories
    (root / "analysis").mkdir(exist_ok=True)
    (root / "outputs").mkdir(exist_ok=True)
    (root / "data").mkdir(exist_ok=True)

    dante_dir = root / ".dante"
    dante_dir.mkdir(exist_ok=True)
    knowledge_dir = dante_dir / "knowledge"
    knowledge_dir.mkdir(exist_ok=True)
    (knowledge_dir / "patterns").mkdir(exist_ok=True)

    _write_if_not_exists(dante_dir / "config.yaml", "# default_connection: my-connection\n")
    _write_if_not_exists(knowledge_dir / "terms.yaml", "# Business glossary\n")
    _write_if_not_exists(knowledge_dir / "keywords.yaml", "# Keyword triggers\n")
    _write_if_not_exists(knowledge_dir / "notes.md", "# Project Notes\n\nAdd notes here.\n")

    skills_dir = root / ".claude" / "skills"
    _write_skill(skills_dir / "query", _QUERY_SKILL)
    _write_skill(skills_dir / "dashboard", _DASHBOARD_SKILL)
    _write_skill(skills_dir / "analyze", _ANALYZE_SKILL)
    _write_skill(skills_dir / "ingest", _INGEST_SKILL)
    _write_skill(skills_dir / "report", _REPORT_SKILL)

    _write_if_not_exists(root / "CLAUDE.md", _CLAUDE_MD)
    _write_if_not_exists(root / ".mcp.json", json.dumps(_MCP_JSON, indent=2) + "\n")
    _write_if_not_exists(root / ".gitignore", _GITIGNORE)

    return root


def _write_if_not_exists(path: Path, content: str) -> None:
    """Write a file only if it doesn't already exist."""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _write_skill(skill_dir: Path, content: str) -> None:
    """Write a SKILL.md file only if it doesn't already exist."""
    skill_dir.mkdir(parents=True, exist_ok=True)
    _write_if_not_exists(skill_dir / "SKILL.md", content)

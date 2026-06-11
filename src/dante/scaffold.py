"""Project scaffolding — creates the directory structure, config files, and CLAUDE.md."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path


def _replace_managed_section(
    existing: str, managed_block: str, start_marker: str, end_marker: str
) -> str:
    """Replace (or append) a marker-delimited managed section in *existing*.

    Robust to: no existing block (appends), multiple existing blocks (collapses
    to one), and markers in unexpected order (removes everything from the first
    start marker to the last end marker). The new *managed_block* must already
    include both markers.
    """
    pattern = re.compile(
        re.escape(start_marker) + r".*?" + re.escape(end_marker),
        re.DOTALL,
    )
    if pattern.search(existing):
        # Drop every existing managed block, then re-insert one fresh copy.
        stripped = pattern.sub("", existing).rstrip("\n")
        return f"{stripped}\n{managed_block}\n"
    return existing.rstrip("\n") + "\n" + managed_block + "\n"

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
| `dante_chart` | Generate a Plotly chart → HTML or PNG file. |
| `dante_app_create` | Create a Data App from a template (dashboard, report, map, profile, blank). |
| `dante_app_add_value` | Bind a SQL query to a computed value slot in a Data App. |
| `dante_app_set_html` | Set the HTML body with {SLOT_NAME} placeholders for computed values. |
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

1. Search knowledge first. Before writing SQL, call `dante_search`. Adapt the closest matches.
2. Explore before querying. Call `dante_describe` on unfamiliar tables before writing SQL.
3. Save validated patterns. When the user confirms a result, call `dante_save_pattern`.
4. Never hardcode data into Data Apps. Always use `dante_app_add_value`.
5. Checkpoint before risky steps.
6. One script per analysis step in `analysis/`. Outputs go to `outputs/`. Data goes to `data/`.

## Project Notes

@.dante/knowledge/notes.yaml

## Rules

@~/.dante/knowledge/rules.yaml
"""

_CURSORRULES = """\
# Dante Data Science Project

You have MCP tools for database queries, charts, dashboards, and knowledge search.

## MCP Tools

| Tool | What it does |
|------|-------------|
| `dante_sql` | Execute read-only SQL. Auto-injects LIMIT. Returns markdown table. |
| `dante_tables` | List tables, optionally filter by schema. |
| `dante_describe` | Column names, types, nullability, sample values for a table. |
| `dante_profile` | Row count, null rates, cardinality, distributions for a table. |
| `dante_search` | Semantic search across embedding index + keywords. Returns matching SQL patterns. |
| `dante_save_pattern` | Save validated SQL + generate embedding for future matching. |
| `dante_chart` | Generate a Plotly chart → HTML or PNG file. |
| `dante_app_create` | Create a Data App from a template (dashboard, report, map, profile, blank). |
| `dante_app_add_value` | Bind a SQL query to a computed value slot in a Data App. |
| `dante_app_set_html` | Set the HTML body with {SLOT_NAME} placeholders for computed values. |
| `dante_app_render` | Execute all queries, substitute values, write final HTML. |
| `dante_checkpoint` | Snapshot analysis/ and outputs/ directories. |
| `dante_rollback` | Restore to a previous checkpoint. |

## Rules

1. Search knowledge first. Before writing SQL, call `dante_search`. Adapt the closest matches.
2. Explore before querying. Call `dante_describe` on unfamiliar tables before writing SQL.
3. Save validated patterns. When the user confirms a result, call `dante_save_pattern`.
4. Never hardcode data into Data Apps. Always use `dante_app_add_value`.
5. Checkpoint before risky steps.
6. One script per analysis step in `analysis/`. Outputs go to `outputs/`. Data goes to `data/`.

## Workflows

### Query Exploration

1. Run `dante_search` with the user's question to find matching patterns
2. If `dante_search` returns relevant matches, adapt the closest SQL. Otherwise, explore:
   - `dante_tables` to find relevant tables
   - `dante_describe` on candidate tables
3. Write and run the query with `dante_sql`
4. If the user confirms the result is useful, save it: `dante_save_pattern`
5. If they want a visualization, use `dante_chart`

### Dashboard Building

1. Search knowledge for relevant patterns: `dante_search`
2. Explore schema to understand available data
3. Create the app: `dante_app_create` with template "dashboard"
4. Add computed values for each KPI and chart: `dante_app_add_value` (NEVER hardcode data)
5. Set the HTML body with {SLOT_NAME} placeholders: `dante_app_set_html`
6. Render and report the output path: `dante_app_render`

Available templates: dashboard, report, map, profile, blank
Dashboard CSS classes: .kpis, .kpi, .kpi-label, .kpi-value, .kpi-change.up/.down, .chart-card, .chart-card.wide, .data-table

### Multi-Step Analysis

1. **Explore.** `dante_tables`, `dante_describe`, `dante_profile` on relevant tables
2. **Search.** `dante_search` for existing patterns related to the question
3. **Plan.** Write a step-by-step plan to `analysis/plan.md`
4. **Execute.** One script per step in `analysis/`. Run and verify each.
5. **Checkpoint.** `dante_checkpoint` after each successful step
6. **Iterate.** If stuck, `dante_rollback` and try a different approach
7. **Report.** Compile findings

## Project Notes

See `.dante/knowledge/notes.yaml` for project-specific notes and context.
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
2. If `dante_search` returns relevant matches, adapt the closest SQL. Otherwise, explore:
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
allowed-tools: Bash, dante_sql, dante_search, dante_chart, dante_app_create, dante_app_add_value, dante_app_set_html, dante_app_render
argument-hint: "[title or topic]"
---

Build a Data App dashboard for: $ARGUMENTS

1. Search knowledge for relevant patterns: `dante_search`
2. Explore schema to understand available data
3. Create the app: `dante_app_create` with template "dashboard"
4. Add computed values for each KPI and chart: `dante_app_add_value` (NEVER hardcode data)
5. Set the HTML body with {SLOT_NAME} placeholders: `dante_app_set_html`
6. Render and report the output path: `dante_app_render`

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


def scaffold_project(name: str, root: Path | None = None, cursor: bool = False) -> Path:
    """Create a new dante-lib project.

    Args:
        name: Project directory name.
        root: Parent directory. Defaults to cwd.
        cursor: If True, generate .cursorrules instead of CLAUDE.md and skills.

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
    _write_if_not_exists(
        dante_dir / "config.yaml", "# default_connection: my-connection\n"
    )

    # Seed global rules.yaml
    from dante.config import knowledge_dir as global_knowledge_dir

    _write_if_not_exists(
        global_knowledge_dir() / "rules.yaml",
        '# Global rules — add brand colors, coding preferences, conventions here\n# design_system: "Use dark mode with #111111 background."\n',
    )

    if cursor:
        _write_if_not_exists(project / ".cursorrules", _CURSORRULES)
        _write_cursor_rules(project)
    else:
        # Project-local knowledge (Claude Code reads via @ imports in CLAUDE.md)
        knowledge_dir = dante_dir / "knowledge"
        knowledge_dir.mkdir(exist_ok=True)
        (knowledge_dir / "patterns").mkdir(exist_ok=True)
        _write_if_not_exists(
            knowledge_dir / "keywords.yaml",
            '# Keyword triggers — add keywords here\n# revenue: "Revenue = SUM(amount) from orders table."\n',
        )
        _write_if_not_exists(
            knowledge_dir / "notes.yaml",
            '# Project notes — add project-specific context here\n# data_overview: "Description of the dataset."\n',
        )

        # .claude/skills/
        skills_dir = project / ".claude" / "skills"
        _write_skill(skills_dir / "query", _QUERY_SKILL)
        _write_skill(skills_dir / "dashboard", _DASHBOARD_SKILL)
        _write_skill(skills_dir / "analyze", _ANALYZE_SKILL)
        _write_skill(skills_dir / "ingest", _INGEST_SKILL)
        _write_skill(skills_dir / "report", _REPORT_SKILL)
        _write_if_not_exists(project / "CLAUDE.md", _CLAUDE_MD)

    # Root files
    _write_if_not_exists(project / ".mcp.json", json.dumps(_MCP_JSON, indent=2) + "\n")
    _write_if_not_exists(project / ".gitignore", _GITIGNORE)
    _write_if_not_exists(
        project / "README.md", f"# {name}\n\nA dante-lib data science project.\n"
    )

    return project


def scaffold_in_place(root: Path | None = None, cursor: bool = False) -> Path:
    """Set up dante-lib in an existing directory (no subdirectory created)."""
    root = root or Path.cwd()

    # Create directories
    (root / "analysis").mkdir(exist_ok=True)
    (root / "outputs").mkdir(exist_ok=True)
    (root / "data").mkdir(exist_ok=True)

    dante_dir = root / ".dante"
    dante_dir.mkdir(exist_ok=True)
    _write_if_not_exists(
        dante_dir / "config.yaml", "# default_connection: my-connection\n"
    )

    # Seed global rules.yaml
    from dante.config import knowledge_dir as global_knowledge_dir

    _write_if_not_exists(
        global_knowledge_dir() / "rules.yaml",
        '# Global rules — add brand colors, coding preferences, conventions here\n# design_system: "Use dark mode with #111111 background."\n',
    )

    if cursor:
        _write_if_not_exists(root / ".cursorrules", _CURSORRULES)
        _write_cursor_rules(root)
    else:
        # Project-local knowledge (Claude Code reads via @ imports in CLAUDE.md)
        knowledge_dir = dante_dir / "knowledge"
        knowledge_dir.mkdir(exist_ok=True)
        (knowledge_dir / "patterns").mkdir(exist_ok=True)
        _write_if_not_exists(knowledge_dir / "keywords.yaml", "# Keyword triggers\n")
        _write_if_not_exists(
            knowledge_dir / "notes.yaml",
            '# Project notes — add project-specific context here\n# data_overview: "Description of the dataset."\n',
        )

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


def _write_cursor_rules(project: Path) -> None:
    """Write .cursor/rules/*.mdc files for Cursor IDE integration."""
    from dante.config import knowledge_dir as global_knowledge_dir

    rules_dir = project / ".cursor" / "rules"
    rules_dir.mkdir(parents=True, exist_ok=True)

    # Skill .mdc files
    _write_if_not_exists(
        rules_dir / "dante-query.mdc",
        _mdc(
            "Query exploration workflow — use when the user wants to explore data or run SQL",
            _QUERY_SKILL.split("---", 2)[-1].strip(),  # strip YAML frontmatter
        ),
    )
    _write_if_not_exists(
        rules_dir / "dante-dashboard.mdc",
        _mdc(
            "Dashboard building workflow — use when the user wants to create a dashboard",
            _DASHBOARD_SKILL.split("---", 2)[-1].strip(),
        ),
    )
    _write_if_not_exists(
        rules_dir / "dante-analyze.mdc",
        _mdc(
            "Multi-step analysis workflow — use when the user wants a structured analysis",
            _ANALYZE_SKILL.split("---", 2)[-1].strip(),
        ),
    )

    # Sync global knowledge into .mdc
    gk = global_knowledge_dir()

    # Project rules from global rules.yaml
    rules_content = ""
    rules_path = gk / "rules.yaml"
    if rules_path.exists():
        rules_content = rules_path.read_text(encoding="utf-8")
    if rules_content.strip():
        _write_mdc(
            rules_dir / "project-rules.mdc",
            _mdc(
                "Global project rules — brand colors, coding preferences, conventions",
                "```yaml\n" + rules_content + "\n```",
            ),
        )

    # Knowledge: project notes
    notes_path = project / ".dante" / "knowledge" / "notes.yaml"
    if notes_path.exists():
        notes_content = notes_path.read_text(encoding="utf-8")
        if notes_content.strip():
            _write_mdc(
                rules_dir / "knowledge.mdc",
                _mdc(
                    "Project knowledge — project-specific notes",
                    "## Project Notes\n\n```yaml\n" + notes_content + "\n```",
                ),
            )


def _mdc(description: str, content: str) -> str:
    """Format content as a .mdc file with frontmatter."""
    return f'---\ndescription: "{description}"\nalwaysApply: true\n---\n\n{content}\n'


def _write_mdc(path: Path, content: str) -> None:
    """Write an .mdc file, always overwriting (synced content)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_if_not_exists(path: Path, content: str) -> None:
    """Write a file only if it doesn't already exist."""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def sync_studio_rules(root: Path, cursor: bool = False) -> bool:
    """Fetch org rules from Dante Studio and write a managed section.

    Returns True if rules were synced, False if remote not configured.
    """
    from dante.remote import _get_remote_client

    client = _get_remote_client(root)
    if client is None:
        return False

    # Fetch org-scoped notes (global rules) from the studio
    try:
        import urllib.error
        import urllib.request

        url = f"{client.api_url}/notes"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {client.api_key}")
        req.add_header("Accept", "application/json")

        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        notes = data.get("notes", [])
        org_rules = [n for n in notes if n.get("scope") == "org"]

        if not org_rules:
            return True  # Connected but no rules to sync

    except Exception as e:
        logging.getLogger(__name__).warning("Failed to fetch studio rules: %s", e)
        return False

    # --- Cursor: write to .cursor/rules/dante-studio.mdc ---
    if cursor:
        rules_dir = root / ".cursor" / "rules"
        rules_dir.mkdir(parents=True, exist_ok=True)
        mdc_path = rules_dir / "dante-studio.mdc"

        mdc_lines = ["---"]
        mdc_lines.append("description: Organization rules from Dante Studio")
        mdc_lines.append("globs: **/*")
        mdc_lines.append("alwaysApply: true")
        mdc_lines.append("---")
        mdc_lines.append("")
        mdc_lines.append("# Organization Rules")
        mdc_lines.append("")
        for rule in org_rules:
            mdc_lines.append(f"## {rule.get('title', 'Untitled')}")
            mdc_lines.append("")
            mdc_lines.append(rule.get("content", ""))
            mdc_lines.append("")

        mdc_path.write_text("\n".join(mdc_lines), encoding="utf-8")
        return True

    # --- Claude Code: managed section in CLAUDE.md ---
    target = root / "CLAUDE.md"
    if not target.exists():
        return False

    # Build the managed section content
    lines = []
    lines.append("")
    lines.append("<!-- MANAGED BY DANTE STUDIO — DO NOT EDIT THIS SECTION -->")
    lines.append("## Organization Rules")
    lines.append("")
    for rule in org_rules:
        lines.append(f"### {rule.get('title', 'Untitled')}")
        lines.append("")
        lines.append(rule.get("content", ""))
        lines.append("")
    lines.append("<!-- END DANTE STUDIO MANAGED SECTION -->")

    managed_content = "\n".join(lines).strip("\n")

    START_MARKER = "<!-- MANAGED BY DANTE STUDIO — DO NOT EDIT THIS SECTION -->"
    END_MARKER = "<!-- END DANTE STUDIO MANAGED SECTION -->"

    existing = target.read_text(encoding="utf-8")
    new_content = _replace_managed_section(
        existing, managed_content, START_MARKER, END_MARKER
    )

    # Atomic write: a crash mid-write must not truncate the user's CLAUDE.md.
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(new_content, encoding="utf-8")
    os.replace(tmp, target)
    return True


def _write_skill(skill_dir: Path, content: str) -> None:
    """Write a SKILL.md file only if it doesn't already exist."""
    skill_dir.mkdir(parents=True, exist_ok=True)
    _write_if_not_exists(skill_dir / "SKILL.md", content)

"""CLI entry point for dante.

Commands:
    dante launch [name]     — scaffold a project, open dante ui
    dante ui                — open the management UI
    dante mcp serve         — start the MCP server (called by Claude Code via .mcp.json)
    dante ingest            — run embedding ingestion
    dante status            — show project status
    dante open [name]       — open an artifact in the browser
    dante refresh           — re-run all computed values
    dante serve             — local preview server for data apps
"""

from __future__ import annotations

import asyncio
import json
import sys
import webbrowser
from pathlib import Path

import click


@click.group()
def main():
    """dante-lib — data science workbench for Claude Code."""
    pass


@main.command()
@click.argument("name", required=False)
@click.option("--no-ui", is_flag=True, help="Skip opening the management UI")
@click.option("--cursor", is_flag=True, help="Generate .cursorrules instead of CLAUDE.md and skills (for Cursor IDE)")
def launch(name: str | None, no_ui: bool, cursor: bool):
    """Scaffold a new data science project and open the setup UI."""
    from dante.scaffold import scaffold_project, scaffold_in_place

    if name:
        project_path = scaffold_project(name, cursor=cursor)
        click.echo(f"Created project at {project_path}/")
    else:
        project_path = scaffold_in_place(cursor=cursor)
        click.echo(f"Initialized dante-lib in {project_path}/")

    click.echo()
    click.echo("Project structure:")
    if cursor:
        click.echo(f"  .mcp.json           — MCP server config")
        click.echo(f"  .cursorrules        — Tool reference and workflows for Cursor")
    else:
        click.echo(f"  .mcp.json           — Claude Code MCP config")
        click.echo(f"  .claude/skills/     — Slash commands (/query, /dashboard, /analyze, ...)")
        click.echo(f"  CLAUDE.md           — Tool reference for Claude")
    click.echo(f"  .dante/             — Config, knowledge, embeddings")
    click.echo(f"  analysis/           — Analysis scripts")
    click.echo(f"  outputs/            — Generated charts, dashboards, reports")
    click.echo()

    if not no_ui:
        click.echo("Opening management UI...")
        _start_ui()
    else:
        click.echo("Run 'dante ui' to configure database connections.")


@main.command()
@click.option("--port", default=4040, help="Port for the UI server")
def ui(port: int):
    """Open the management UI for connections, credentials, and knowledge."""
    _start_ui(port=port)


@main.group()
def mcp():
    """MCP server commands."""
    pass


@mcp.command()
def serve():
    """Start the MCP server (stdio transport). Called by Claude Code via .mcp.json."""
    from dante.mcp_server import main as mcp_main
    asyncio.run(mcp_main())


@main.command()
@click.option("--source", type=click.Choice(["looker", "databricks", "warehouse", "all"]), default="all")
@click.option("--min-views", default=10, help="Looker: minimum dashboard views")
@click.option("--lookback-days", default=90, help="Looker: only dashboards accessed within N days")
@click.option("--dry-run", is_flag=True, help="Show what would be ingested without doing it")
def ingest(source: str, min_views: int, lookback_days: int, dry_run: bool):
    """Ingest embeddings from BI platforms into the local index."""
    from dante.ingest import IngestionConfig, run as run_ingest

    config = IngestionConfig(
        sources=[source],
        min_views=min_views,
        lookback_days=lookback_days,
        dry_run=dry_run,
    )
    click.echo(f"Ingesting from {source}{'  (dry run)' if dry_run else ''}...")
    result = asyncio.run(run_ingest(config))
    click.echo(f"Done: {result.created} created, {result.updated} updated, "
               f"{result.skipped} skipped, {result.errors} errors")


@main.command()
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output")
def status(as_json: bool):
    """Show project status: connection, knowledge stats, artifacts."""
    from dante.config import (
        get_default_connection_name,
        get_connection_config,
        project_dir,
        _find_project_root,
    )

    root = _find_project_root()
    pd = project_dir(root)

    conn_name = get_default_connection_name(root)
    conn_config = get_connection_config(root=root)

    # Count knowledge artifacts
    knowledge_dir = pd / "knowledge"
    terms_count = 0
    keywords_count = 0
    patterns_count = 0

    terms_file = knowledge_dir / "terms.yaml"
    if terms_file.exists():
        import yaml
        with open(terms_file, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        terms_count = len([k for k in data if not str(k).startswith("#")])

    keywords_file = knowledge_dir / "keywords.yaml"
    if keywords_file.exists():
        import yaml
        with open(keywords_file, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        keywords_count = len([k for k in data if not str(k).startswith("#")])

    patterns_dir = knowledge_dir / "patterns"
    if patterns_dir.exists():
        patterns_count = len(list(patterns_dir.glob("*.sql")))

    # Count embeddings
    embeddings_count = 0
    embeddings_db = pd / "embeddings.db"
    if embeddings_db.exists():
        import sqlite3
        try:
            conn = sqlite3.connect(str(embeddings_db))
            cur = conn.execute("SELECT COUNT(*) FROM embeddings")
            embeddings_count = cur.fetchone()[0]
            conn.close()
        except Exception:
            pass

    # Count outputs
    outputs_dir = root / "outputs"
    outputs_count = len(list(outputs_dir.glob("*"))) if outputs_dir.exists() else 0

    info = {
        "connection": {
            "name": conn_name,
            "configured": conn_config is not None,
            "dialect": conn_config.get("dialect") if conn_config else None,
            "database": conn_config.get("database") if conn_config else None,
        },
        "knowledge": {
            "glossary_terms": terms_count,
            "keywords": keywords_count,
            "patterns": patterns_count,
            "embeddings": embeddings_count,
        },
        "outputs": outputs_count,
    }

    if as_json:
        click.echo(json.dumps(info, indent=2))
    else:
        click.echo("Dante Project Status")
        click.echo("=" * 40)
        click.echo()
        click.echo("Connection:")
        if conn_config:
            click.echo(f"  Name:     {conn_name}")
            click.echo(f"  Dialect:  {conn_config.get('dialect', '?')}")
            click.echo(f"  Database: {conn_config.get('database', '?')}")
        else:
            click.echo("  Not configured. Run 'dante ui' to set up.")
        click.echo()
        click.echo("Knowledge:")
        click.echo(f"  Glossary terms:  {terms_count}")
        click.echo(f"  Keywords:        {keywords_count}")
        click.echo(f"  SQL patterns:    {patterns_count}")
        click.echo(f"  Embeddings:      {embeddings_count}")
        click.echo()
        click.echo(f"Outputs: {outputs_count} files")


@main.command("open")
@click.argument("name", required=False)
def open_artifact(name: str | None):
    """Open an artifact in the browser."""
    from dante.config import _find_project_root
    outputs = _find_project_root() / "outputs"
    if not outputs.exists():
        click.echo("No outputs/ directory found.")
        return

    if name:
        # Find matching file
        matches = list(outputs.glob(f"{name}*"))
        if not matches:
            click.echo(f"No artifact matching '{name}' found in outputs/")
            return
        target = matches[0]
    else:
        # Open most recent
        files = sorted(outputs.iterdir(), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            click.echo("No artifacts in outputs/")
            return
        target = files[0]

    click.echo(f"Opening {target.name}")
    webbrowser.open(target.resolve().as_uri())


def _start_ui(port: int = 4040):
    """Start the dante UI server."""
    from dante.ui.server import run_server
    click.echo(f"Starting dante UI at http://localhost:{port}")
    click.echo("Press Ctrl+C to stop.")
    webbrowser.open(f"http://localhost:{port}")
    run_server(port=port)


if __name__ == "__main__":
    main()

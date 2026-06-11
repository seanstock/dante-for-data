# Dante Code Map

_Auto-generated structural index of `src/dante/`. 291 lines vs 6852 source lines (96% reduction)._

_Regenerate: `python scripts/codemap.py`_

## src/dante/__init__.py
> dante-lib — MCP server + skills that turn Claude Code into a data science workbench.
Exports: connect, sql, tables, describe, profile, chart, checkpoint, rollback, report, app, knowledge, ingest

## src/dante/_utils.py
> Shared internal utilities for dante-lib.
Imports: __future__, asyncio, pathlib, re, typing, unicodedata
Constants: T
- `slugify(text: str, fallback: str='file', max_len: int=80) -> str`
- `ensure_outputs_dir(root: Path | None=None) -> Path`
- `dataframe_to_markdown(df: Any, empty_msg: str='_No results._') -> str`
- `run_async(coro: Coroutine[Any, Any, T]) -> T`

## src/dante/analyze.py
> Analysis tools: checkpoint, rollback, and report compilation.
Imports: __future__, datetime, html, json, pathlib, re, shutil
- `checkpoint(name: str, root: Path | None=None) -> str`
- `rollback(name: str='', root: Path | None=None) -> str`
- `list_checkpoints(root: Path | None=None) -> list[str]`
- `report(title: str, sections: list[str] | None=None, charts: list[str] | None=None, root: Path | None=None) -> str`

## src/dante/app.py
> Data App template engine — HTML dashboards with computed values.
Imports: __future__, html, pathlib, webbrowser
- class **App**
  - `__init__(title: str, template: str='dashboard', root: Path | None=None)`
  - `html() -> str`
  - `html(value: str)`
  - `css() -> str`
  - `css(value: str)`
  - `js() -> str`
  - `js(value: str)`
  - `add_value(name: str, sql_query: str, format: str='scalar')`
  - `remove_value(name: str)`
  - `value_names() -> list[str]`
  - `render() -> str`
  - `open()`
  - `refresh() -> str`
- `create(title: str, template: str='dashboard', root: Path | None=None) -> App`

## src/dante/chart.py
> Chart generation via Plotly → self-contained HTML or PNG files.
Imports: __future__, pandas, pathlib, plotly.express, plotly.graph_objects
- `chart(data, x: str | None=None, y: str | list[str] | None=None, kind: str='bar', title: str | None=None, filename: str | None=None, format: str='html', theme: str='dark', root: Path | None=None) -> str`
- `figure_to_inline_html(fig: go.Figure, div_id: str='chart') -> str`

## src/dante/cli.py
> CLI entry point for dante.
Imports: __future__, asyncio, click, json, logging, webbrowser
- `main()`
- `launch(name: str | None, ui: bool, cursor: bool)`
- `ui(port: int)`
- `mcp()`
- `serve()`
- `ingest(source: str, min_views: int, lookback_days: int, dry_run: bool)`
- `status(as_json: bool)`
- `open_artifact(name: str | None)`

## src/dante/config.py
> Project and global configuration management.
Imports: os, pathlib, yaml
- `global_dir() -> Path`
- `project_dir(root: Path | None=None) -> Path`
- `knowledge_dir(root: Path | None=None) -> Path`
- `load_global_connections() -> dict`
- `save_global_connections(data: dict) -> None`
- `load_global_credentials() -> dict`
- `save_global_credentials(data: dict) -> None`
- `load_project_config(root: Path | None=None) -> dict`
- `save_project_config(data: dict, root: Path | None=None) -> None`
- `get_default_connection_name(root: Path | None=None) -> str | None`
- `get_connection_config(name: str | None=None, root: Path | None=None) -> dict | None`

## src/dante/connect.py
> Database connection management via SQLAlchemy.
Imports: __future__, logging, pathlib, sqlalchemy, sqlalchemy.engine, threading, time, urllib.parse
- `connect(url: str | None=None, name: str | None=None, root: Path | None=None) -> Engine`
- `dispose_all() -> None`
- `test_connection(conn_config: dict) -> tuple[bool, str]`

## src/dante/ingest/__init__.py
> Embedding ingestion pipeline.
Imports: __future__, dataclasses, logging, typing
- class **IngestionConfig**
- class **IngestionResult**
  - `to_dict() -> dict`
- `async run(config: IngestionConfig) -> IngestionResult`

## src/dante/ingest/_common.py
> Shared ingestion utilities — deduplicates the embed loop across connectors.
Imports: __future__, hashlib, logging, time
- `make_embedding_id(platform: str, prefix: str, id_a: str, id_b: str) -> str`
- `get_credentials(platform: str, required_keys: list[str]) -> dict | None`
- `async embed_charts(charts: list[dict], *, source: str, config: IngestionConfig, make_id: callable, sql_transform: callable | None=None) -> IngestionResult`

## src/dante/ingest/databricks.py
> Databricks Lakeview dashboard ingestion.
Imports: __future__, json, logging, re, requests
- `async ingest_databricks(config: IngestionConfig) -> IngestionResult`

## src/dante/ingest/looker.py
> Looker dashboard ingestion for embedding generation.
Imports: __future__, json, logging
- `async ingest_looker(config: IngestionConfig) -> IngestionResult`

## src/dante/ingest/mode.py
> Mode Analytics ingestion (experimental).
Imports: __future__, logging, requests
- `async ingest_mode(config: IngestionConfig) -> IngestionResult`

## src/dante/ingest/question_gen.py
> Heuristic-based conversion of chart titles to natural-language questions.
Imports: re
- `generate_question(entity_title: str, dashboard_title: str) -> str`

## src/dante/ingest/redash.py
> Redash ingestion (experimental).
Imports: __future__, logging, requests
- `async ingest_redash(config: IngestionConfig) -> IngestionResult`

## src/dante/ingest/sigma.py
> Sigma Computing ingestion (experimental).
Imports: __future__, logging, requests
- `async ingest_sigma(config: IngestionConfig) -> IngestionResult`

## src/dante/ingest/sql_simplifier.py
> LLM-based SQL simplification for embedding quality.
Imports: __future__, logging
- `async simplify_sql(raw_sql: str, chart_title: str, enabled: bool=True) -> str`

## src/dante/ingest/superset.py
> Apache Superset ingestion (experimental).
Imports: __future__, json, logging, requests
- `async ingest_superset(config: IngestionConfig) -> IngestionResult`

## src/dante/ingest/warehouse.py
> Warehouse schema metadata ingestion.
Imports: __future__, hashlib, logging, sqlalchemy
- `async ingest_warehouse(config: IngestionConfig) -> IngestionResult`

## src/dante/knowledge/__init__.py
> Knowledge system for dante-lib.
Imports: __future__, logging, pathlib
- `search(query: str, top_k: int=5, threshold: float=0.3, root: Path | None=None) -> list[dict]`
- `async search_async(query: str, top_k: int=5, threshold: float=0.3, root: Path | None=None) -> list[dict]`
- `save_pattern(question: str, sql: str, tables: list[str] | None=None, description: str='', root: Path | None=None) -> dict`
- `async save_pattern_async(question: str, sql: str, tables: list[str] | None=None, description: str='', root: Path | None=None) -> str`
- `list_patterns(root: Path | None=None) -> list[dict]`
- `add_keyword(keyword: str, content: str, root: Path | None=None) -> None`
- `remove_keyword(keyword: str, root: Path | None=None) -> bool`
- `add_note(text: str, root: Path | None=None) -> None`
- `stats(root: Path | None=None) -> dict`
- `rebuild(root: Path | None=None) -> dict`

## src/dante/knowledge/embeddings.py
> SQLite-based embedding storage with cosine similarity search.
Imports: __future__, datetime, json, math, pathlib, sqlite3
- `init_db(db_path: Path) -> sqlite3.Connection`
- `upsert(conn: sqlite3.Connection, id: str, question: str, sql: str='', source: str='manual', dashboard: str='', description: str='', embedding_vector: list[float] | None=None) -> None`
- `delete(conn: sqlite3.Connection, id: str) -> bool`
- `get(conn: sqlite3.Connection, id: str) -> dict | None`
- `search(conn: sqlite3.Connection, embedding_vector: list[float], top_k: int=5, threshold: float=0.3) -> list[dict]`
- `stats(conn: sqlite3.Connection) -> dict`
- `count(conn: sqlite3.Connection) -> int`
- `list_all(conn: sqlite3.Connection) -> list[dict]`

## src/dante/knowledge/keywords.py
> Read/write .dante/knowledge/keywords.yaml and substring matching.
Imports: __future__, logging, pathlib, yaml
- `load(root: Path | None=None) -> dict[str, str]`
- `save(keywords: dict[str, str], root: Path | None=None) -> None`
- `add(keyword: str, content: str, root: Path | None=None) -> None`
- `remove(keyword: str, root: Path | None=None) -> bool`
- `list_keywords(root: Path | None=None) -> list[dict[str, str]]`
- `match(query: str, root: Path | None=None) -> list[dict[str, str]]`

## src/dante/knowledge/notes.py
> Read, write, and append to .dante/knowledge/notes.md.
Imports: __future__, pathlib
- `read(root: Path | None=None) -> str`
- `write(content: str, root: Path | None=None) -> None`
- `append(text: str, root: Path | None=None) -> None`

## src/dante/knowledge/patterns.py
> Read and write .dante/knowledge/patterns/*.sql files.
Imports: __future__, datetime, pathlib, yaml
- `save_pattern(question: str, sql: str, tables: list[str] | None=None, description: str='', source: str='manual', root: Path | None=None) -> Path`
- `load_pattern(path: Path) -> dict`
- `list_patterns(root: Path | None=None) -> list[dict]`
- `delete_pattern(filename: str, root: Path | None=None) -> bool`
- `get_pattern(question: str, root: Path | None=None) -> dict | None`

## src/dante/knowledge/search.py
> Unified search combining keyword matching and embedding similarity.
Imports: __future__, logging, pathlib
- `async search_async(query: str, top_k: int=10, threshold: float=0.3, root: Path | None=None) -> list[dict]`
- `search(query: str, top_k: int=10, threshold: float=0.3, root: Path | None=None) -> list[dict]`

## src/dante/knowledge/vectorize.py
> OpenAI embedding generation via text-embedding-3-large.
Imports: __future__, logging, openai, os, sys, tenacity
Constants: MODEL, DIMENSIONS
- `async generate_embedding(text: str) -> list[float]`
- `async generate_embeddings_batch(texts: list[str]) -> list[list[float]]`

## src/dante/mcp_server.py
> Dante MCP server — exposes database tools to Claude via stdio transport.
Imports: __future__, asyncio, inspect, mcp.server, mcp.server.stdio, mcp.types, traceback
Constants: TOOLS
- `async list_tools() -> list[Tool]`
- `async call_tool(name: str, arguments: dict) -> list[TextContent]`
- `async main() -> None`

## src/dante/query.py
> Safe SQL execution with automatic LIMIT injection and audit logging.
Imports: __future__, datetime, json, logging, pandas, pathlib, re, sqlalchemy, sqlalchemy.engine, time
- `sql(query: str, limit: int=_DEFAULT_LIMIT, engine: Engine | None=None, root: Path | None=None) -> pd.DataFrame`
- `sql_markdown(query: str, limit: int=_DEFAULT_LIMIT, engine: Engine | None=None, root: Path | None=None) -> str`
- `tables(schema: str | None=None, engine: Engine | None=None) -> list[str]`
- `tables_markdown(schema: str | None=None, engine: Engine | None=None) -> str`
- `describe(table: str, schema: str | None=None, engine: Engine | None=None) -> pd.DataFrame`
- `describe_markdown(table: str, schema: str | None=None, engine: Engine | None=None) -> str`
- `profile(table: str, schema: str | None=None, engine: Engine | None=None) -> pd.DataFrame`
- `profile_markdown(table: str, schema: str | None=None, engine: Engine | None=None) -> str`

## src/dante/remote.py
> Remote knowledge client for dante-ds.
Imports: __future__, json, logging, pathlib, typing, urllib.error, urllib.request
- class **RemoteKnowledge**
  - `__init__(api_url: str, api_key: str) -> None`
  - `search(query: str, top_k: int=10) -> list[dict]`
  - `save_pattern(question: str, sql: str, tables: list[str] | None=None, description: str='') -> dict`
  - `list_patterns(status: str | None=None, limit: int=50, offset: int=0) -> list[dict]`
  - `edit_pattern(pattern_id: str, **updates: Any) -> dict`
  - `delete_pattern(pattern_id: str) -> bool`
  - `list_keywords(scope: str | None='org') -> list[dict]`
  - `create_keyword(keyword: str, content: str) -> dict`
  - `delete_keyword(keyword_id: str) -> bool`
  - `stats() -> dict`

## src/dante/scaffold.py
> Project scaffolding — creates the directory structure, config files, and CLAUDE.md.
Imports: __future__, json, logging, pathlib
- `scaffold_project(name: str, root: Path | None=None, cursor: bool=False) -> Path`
- `scaffold_in_place(root: Path | None=None, cursor: bool=False) -> Path`
- `sync_studio_rules(root: Path, cursor: bool=False) -> bool`

## src/dante/tools/analyze_tools.py
> MCP tool implementations for analysis checkpointing and rollback.
Imports: __future__
- `dante_checkpoint(name: str) -> str`
- `dante_rollback(name: str='') -> str`

## src/dante/tools/app_tools.py
> MCP tool implementations for app/dashboard generation.
Imports: __future__, threading
- `dante_app_create(title: str, template: str='dashboard') -> str`
- `dante_app_add_value(app_id: str, name: str, sql: str, format: str='scalar') -> str`
- `dante_app_set_html(app_id: str, html: str, css: str='', js: str='') -> str`
- `dante_app_render(app_id: str) -> str`

## src/dante/tools/chart_tools.py
> MCP tool function for chart generation.
Imports: __future__, json, pandas
- `dante_chart(data: str, x: str | None=None, y: str | None=None, kind: str='bar', title: str='Chart', format: str='html', theme: str='dark') -> str`

## src/dante/tools/knowledge_tools.py
> MCP tool implementations for knowledge management (search, patterns).
Imports: __future__
- `async dante_search(query: str, top_k: int=10) -> str`
- `async dante_save_pattern(question: str, sql: str, tables: list[str], description: str) -> str`

## src/dante/tools/sql_tools.py
> MCP tool functions for SQL query execution and database introspection.
Imports: __future__
- `dante_sql(query: str, limit: int=5000) -> str`
- `dante_tables(schema: str | None=None) -> str`
- `dante_describe(table: str, schema: str | None=None) -> str`
- `dante_profile(table: str, schema: str | None=None) -> str`

## src/dante/ui/server.py
> Lightweight HTTP server for dante ui.
Imports: __future__, datetime, functools, http.server, json, os, pathlib, threading, urllib.parse, uuid, yaml
- class **DanteUIHandler**(SimpleHTTPRequestHandler)
  - `__init__(*args, project_root: Path | None=None, **kwargs)`
  - `do_GET()`
  - `do_POST()`
  - `do_PUT()`
  - `do_DELETE()`
  - `log_message(format, *args)`
- `run_server(port: int=4040, project_root: Path | None=None)`

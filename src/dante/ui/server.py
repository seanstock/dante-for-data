"""Lightweight HTTP server for dante ui.

Serves the single-page app and handles API routes for managing
connections, credentials, knowledge, and project status.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import yaml

from dante.config import (
    global_dir,
    knowledge_dir,
    load_global_connections,
    load_global_credentials,
    load_project_config,
    project_dir,
    save_global_connections,
    save_global_credentials,
    save_project_config,
)
from dante.connect import test_connection

_UI_DIR = Path(__file__).parent


# ── Job storage helpers ──────────────────────────────────────────────────────

def _jobs_file() -> Path:
    from dante.config import global_dir
    return global_dir() / "jobs.json"


def _load_jobs() -> list:
    f = _jobs_file()
    if f.exists():
        try:
            with open(f, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return []
    return []


def _save_jobs(jobs: list) -> None:
    f = _jobs_file()
    with open(f, "w", encoding="utf-8") as fh:
        json.dump(jobs, fh, indent=2, default=str)


def _run_ingest_background(
    job_id: str, source: str, skip_existing: bool, dashboard_limit: int
) -> None:
    """Run ingestion in a background thread and update the job record."""
    import asyncio
    from dante.ingest import IngestionConfig, run as run_ingest

    # Mark as running
    jobs = _load_jobs()
    for j in jobs:
        if j["id"] == job_id:
            j["status"] = "running"
            break
    _save_jobs(jobs)

    try:
        config = IngestionConfig(
            sources=[source],
            skip_existing=skip_existing,
            dashboard_limit=dashboard_limit,
        )
        result = asyncio.run(run_ingest(config))

        jobs = _load_jobs()
        for j in jobs:
            if j["id"] == job_id:
                j["status"] = "completed"
                j["finished_at"] = datetime.now(timezone.utc).isoformat()
                j["result"] = result.to_dict()
                break
        _save_jobs(jobs)
    except Exception as exc:
        jobs = _load_jobs()
        for j in jobs:
            if j["id"] == job_id:
                j["status"] = "failed"
                j["finished_at"] = datetime.now(timezone.utc).isoformat()
                j["error"] = str(exc)
                break
        _save_jobs(jobs)


class DanteUIHandler(SimpleHTTPRequestHandler):
    """HTTP request handler for the dante UI."""

    def __init__(self, *args, project_root: Path | None = None, **kwargs):
        self.project_root = project_root or Path.cwd()
        super().__init__(*args, directory=str(_UI_DIR), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self._serve_app()
        elif path.startswith("/api/"):
            self._handle_api_get(path, parsed.query)
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/"):
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                data = {}
            self._handle_api_post(path, data)
        else:
            self.send_error(404)

    def do_PUT(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/patterns/"):
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                data = {}

            from dante.knowledge.patterns import save_pattern, delete_pattern

            old_filename = path.split("/")[-1]
            question = data.get("question", "")
            sql = data.get("sql", "")
            tables = data.get("tables", [])
            description = data.get("description", "")

            if not question:
                self._json_response({"error": "Question is required"}, 400)
                return

            # Delete old file first (slug may change if question changed)
            delete_pattern(old_filename)

            new_path = save_pattern(
                question=question,
                sql=sql,
                tables=tables if isinstance(tables, list) else [t.strip() for t in tables.split(",") if t.strip()],
                description=description,
            )
            self._json_response({"ok": True, "filename": new_path.name})
        else:
            self.send_error(404)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/"):
            self._handle_api_delete(path)
        else:
            self.send_error(404)

    def _serve_app(self):
        app_path = _UI_DIR / "app.html"
        content = app_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _json_response(self, data, status=200):
        body = json.dumps(data, indent=2, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_api_get(self, path: str, query: str):
        if path == "/api/connections":
            data = load_global_connections()
            self._json_response(data.get("connections", {}))

        elif path == "/api/credentials":
            creds = load_global_credentials()
            # Mask sensitive values
            masked = {}
            for key, val in creds.items():
                if isinstance(val, dict):
                    masked[key] = {
                        k: ("****" if "secret" in k.lower() or "password" in k.lower() or "token" in k.lower() or "key" in k.lower() else v)
                        for k, v in val.items()
                    }
                else:
                    masked[key] = val
            self._json_response(masked)

        elif path == "/api/config":
            cfg = load_project_config(self.project_root)
            self._json_response(cfg)

        elif path == "/api/glossary":
            terms_path = knowledge_dir() / "terms.yaml"
            if terms_path.exists():
                with open(terms_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                self._json_response(data)
            else:
                self._json_response({})

        elif path == "/api/keywords":
            kw_path = knowledge_dir() / "keywords.yaml"
            if kw_path.exists():
                with open(kw_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                self._json_response(data)
            else:
                self._json_response({})

        elif path == "/api/notes":
            notes_path = project_dir(self.project_root) / "knowledge" / "notes.yaml"
            if notes_path.exists():
                with open(notes_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                self._json_response(data)
            else:
                self._json_response({})

        elif path == "/api/rules":
            rules_path = knowledge_dir() / "rules.yaml"
            if rules_path.exists():
                with open(rules_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                self._json_response(data)
            else:
                self._json_response({})

        elif path == "/api/patterns":
            from dante.knowledge.patterns import list_patterns
            self._json_response(list_patterns())

        elif path == "/api/status":
            self._json_response(self._get_status())

        elif path == "/api/jobs":
            self._json_response(_load_jobs())

        else:
            self.send_error(404)

    def _handle_api_post(self, path: str, data: dict):
        if path == "/api/connections":
            # Save or update a connection
            name = data.get("name")
            config = data.get("config", {})
            if not name:
                self._json_response({"error": "Connection name required"}, 400)
                return
            conns = load_global_connections()
            conns.setdefault("connections", {})[name] = config
            save_global_connections(conns)
            self._json_response({"ok": True, "name": name})

        elif path == "/api/connections/test":
            config = data.get("config", {})
            success, message = test_connection(config)
            self._json_response({"success": success, "message": message})

        elif path == "/api/credentials":
            creds = load_global_credentials()
            creds.update(data)
            save_global_credentials(creds)
            self._json_response({"ok": True})

        elif path == "/api/config":
            cfg = load_project_config(self.project_root)
            cfg.update(data)
            save_project_config(cfg, self.project_root)
            self._json_response({"ok": True})

        elif path == "/api/glossary":
            term = data.get("term")
            definition = data.get("definition")
            if not term:
                self._json_response({"error": "Term required"}, 400)
                return
            terms_path = knowledge_dir() / "terms.yaml"
            terms_path.parent.mkdir(parents=True, exist_ok=True)
            existing = {}
            if terms_path.exists():
                with open(terms_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
            existing[term] = definition
            with open(terms_path, "w", encoding="utf-8") as f:
                yaml.dump(existing, f, default_flow_style=False, sort_keys=True)
            self._json_response({"ok": True})

        elif path == "/api/keywords":
            keyword = data.get("keyword")
            content = data.get("content")
            if not keyword:
                self._json_response({"error": "Keyword required"}, 400)
                return
            kw_path = knowledge_dir() / "keywords.yaml"
            kw_path.parent.mkdir(parents=True, exist_ok=True)
            existing = {}
            if kw_path.exists():
                with open(kw_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
            existing[keyword] = content
            with open(kw_path, "w", encoding="utf-8") as f:
                yaml.dump(existing, f, default_flow_style=False, sort_keys=True)
            self._json_response({"ok": True})

        elif path == "/api/patterns":
            from dante.knowledge.patterns import save_pattern
            question = data.get("question", "")
            sql = data.get("sql", "")
            tables = data.get("tables", [])
            description = data.get("description", "")
            if not question:
                self._json_response({"error": "Question is required"}, 400)
                return
            new_path = save_pattern(
                question=question,
                sql=sql,
                tables=tables if isinstance(tables, list) else [t.strip() for t in tables.split(",") if t.strip()],
                description=description,
            )
            self._json_response({"ok": True, "filename": new_path.name})

        elif path == "/api/notes":
            name = data.get("name")
            content = data.get("content")
            if not name:
                self._json_response({"error": "Name required"}, 400)
                return
            notes_path = project_dir(self.project_root) / "knowledge" / "notes.yaml"
            notes_path.parent.mkdir(parents=True, exist_ok=True)
            existing = {}
            if notes_path.exists():
                with open(notes_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
            existing[name] = content
            with open(notes_path, "w", encoding="utf-8") as f:
                yaml.dump(existing, f, default_flow_style=False, sort_keys=True, allow_unicode=True)
            self._json_response({"ok": True})

        elif path == "/api/rules":
            name = data.get("name")
            content = data.get("content")
            if not name:
                self._json_response({"error": "Name required"}, 400)
                return
            rules_path = knowledge_dir() / "rules.yaml"
            rules_path.parent.mkdir(parents=True, exist_ok=True)
            existing = {}
            if rules_path.exists():
                with open(rules_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
            existing[name] = content
            with open(rules_path, "w", encoding="utf-8") as f:
                yaml.dump(existing, f, default_flow_style=False, sort_keys=True, allow_unicode=True)
            self._json_response({"ok": True})

        elif path == "/api/ingest":
            source = data.get("source", "all")
            skip_existing = bool(data.get("skip_existing", False))
            dashboard_limit = int(data.get("dashboard_limit", 0))

            job_id = uuid.uuid4().hex[:8]
            now = datetime.now(timezone.utc).isoformat()
            job = {
                "id": job_id,
                "source": source,
                "started_at": now,
                "finished_at": None,
                "status": "pending",
                "result": None,
            }
            jobs = _load_jobs()
            jobs.insert(0, job)
            _save_jobs(jobs[:50])

            t = threading.Thread(
                target=_run_ingest_background,
                args=(job_id, source, skip_existing, dashboard_limit),
                daemon=True,
            )
            t.start()
            self._json_response({"ok": True, "job_id": job_id})

        else:
            self.send_error(404)

    def _handle_api_delete(self, path: str):
        if path.startswith("/api/connections/"):
            name = path.split("/")[-1]
            conns = load_global_connections()
            conns.get("connections", {}).pop(name, None)
            save_global_connections(conns)
            self._json_response({"ok": True})

        elif path.startswith("/api/glossary/"):
            term = path.split("/")[-1]
            terms_path = knowledge_dir() / "terms.yaml"
            if terms_path.exists():
                with open(terms_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
                existing.pop(term, None)
                with open(terms_path, "w", encoding="utf-8") as f:
                    yaml.dump(existing, f, default_flow_style=False, sort_keys=True)
            self._json_response({"ok": True})

        elif path.startswith("/api/keywords/"):
            keyword = path.split("/")[-1]
            kw_path = knowledge_dir() / "keywords.yaml"
            if kw_path.exists():
                with open(kw_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
                existing.pop(keyword, None)
                with open(kw_path, "w", encoding="utf-8") as f:
                    yaml.dump(existing, f, default_flow_style=False, sort_keys=True)
            self._json_response({"ok": True})

        elif path.startswith("/api/notes/"):
            name = path.split("/")[-1]
            notes_path = project_dir(self.project_root) / "knowledge" / "notes.yaml"
            if notes_path.exists():
                with open(notes_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
                existing.pop(name, None)
                with open(notes_path, "w", encoding="utf-8") as f:
                    yaml.dump(existing, f, default_flow_style=False, sort_keys=True, allow_unicode=True)
            self._json_response({"ok": True})

        elif path.startswith("/api/rules/"):
            name = path.split("/")[-1]
            rules_path = knowledge_dir() / "rules.yaml"
            if rules_path.exists():
                with open(rules_path, encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
                existing.pop(name, None)
                with open(rules_path, "w", encoding="utf-8") as f:
                    yaml.dump(existing, f, default_flow_style=False, sort_keys=True, allow_unicode=True)
            self._json_response({"ok": True})

        elif path.startswith("/api/patterns/"):
            filename = path.split("/")[-1]
            pattern_path = knowledge_dir() / "patterns" / filename
            # Delete the .sql file
            if pattern_path.exists():
                pattern_path.unlink()
            # Also delete from embedding DB
            try:
                from dante.knowledge.embeddings import init_db, delete as emb_delete
                db_path = knowledge_dir() / "embeddings.db"
                if db_path.exists():
                    conn = init_db(db_path)
                    slug = filename.replace(".sql", "") if filename.endswith(".sql") else filename
                    emb_delete(conn, slug)
                    conn.close()
            except Exception:
                pass
            self._json_response({"ok": True})

        else:
            self.send_error(404)

    def _get_status(self) -> dict:
        from dante.config import get_default_connection_name, get_connection_config

        pd = project_dir(self.project_root)
        conn_name = get_default_connection_name(self.project_root)
        conn_config = get_connection_config(root=self.project_root)

        # Count knowledge (global knowledge dir shared across all projects)
        kd = knowledge_dir()
        terms_count = 0
        keywords_count = 0
        patterns_count = 0

        terms_file = kd / "terms.yaml"
        if terms_file.exists():
            with open(terms_file, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            terms_count = len(data)

        kw_file = kd / "keywords.yaml"
        if kw_file.exists():
            with open(kw_file, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            keywords_count = len(data)

        patterns_dir = kd / "patterns"
        if patterns_dir.exists():
            patterns_count = len(list(patterns_dir.glob("*.sql")))

        embeddings_count = 0
        embeddings_db = kd / "embeddings.db"
        if embeddings_db.exists():
            import sqlite3
            try:
                conn = sqlite3.connect(str(embeddings_db))
                cur = conn.execute("SELECT COUNT(*) FROM embeddings")
                embeddings_count = cur.fetchone()[0]
                conn.close()
            except Exception:
                pass

        return {
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
        }

    def log_message(self, format, *args):
        """Suppress default request logging."""
        pass


def run_server(port: int = 4040, project_root: Path | None = None):
    """Run the dante UI server."""
    import signal

    root = project_root or Path.cwd()
    handler = partial(DanteUIHandler, project_root=root)
    server = HTTPServer(("127.0.0.1", port), handler)

    def _shutdown(sig, frame):
        server.server_close()
        os._exit(0)

    signal.signal(signal.SIGINT, _shutdown)

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

"""Tests for dante.ui.server — the local management HTTP server.

These run a real HTTPServer on an ephemeral port and exercise the
DELETE/PUT routing, path-traversal protection, body-size limit, and
Host-header validation.
"""

import http.client
import json
import threading
from functools import partial
from http.server import HTTPServer

import pytest

from dante.config import knowledge_dir
from dante.ui.server import DanteUIHandler


@pytest.fixture
def server(tmp_path):
    """Start the UI server on an ephemeral localhost port for a tmp project."""
    handler = partial(DanteUIHandler, project_root=tmp_path)
    httpd = HTTPServer(("127.0.0.1", 0), handler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield port, tmp_path
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _request(port, method, path, body=None, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    hdrs = {"Host": f"127.0.0.1:{port}"}
    if headers:
        hdrs.update(headers)
    payload = json.dumps(body) if body is not None else None
    if payload is not None:
        hdrs.setdefault("Content-Type", "application/json")
    conn.request(method, path, body=payload, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


# ── DELETE routing (the critical bug: _handle_api_delete didn't exist) ────────


def test_delete_keyword_succeeds(server):
    port, root = server
    # Seed a keyword via POST.
    status, _ = _request(port, "POST", "/api/keywords",
                         {"keyword": "kpi", "content": "key perf indicator"})
    assert status == 200
    # Delete it.
    status, body = _request(port, "DELETE", "/api/keywords/kpi")
    assert status == 200, body
    # Confirm it's gone.
    status, body = _request(port, "GET", "/api/keywords")
    assert status == 200
    assert "kpi" not in json.loads(body)


def test_delete_pattern_succeeds(server):
    port, root = server
    status, body = _request(port, "POST", "/api/patterns",
                           {"question": "How many users?", "sql": "SELECT 1"})
    assert status == 200, body
    filename = json.loads(body)["filename"]
    status, body = _request(port, "DELETE", f"/api/patterns/{filename}")
    assert status == 200, body
    assert not (knowledge_dir(root) / "patterns" / filename).exists()


def test_delete_unknown_route_returns_404(server):
    port, _ = server
    status, _ = _request(port, "DELETE", "/api/nonsense/x")
    assert status == 404


# ── Path traversal protection ─────────────────────────────────────────────────


def test_delete_pattern_rejects_traversal(server):
    port, root = server
    # Plant a file outside the patterns dir that must NOT be deletable.
    victim = root / "victim.sql"
    victim.write_text("important")
    status, _ = _request(port, "DELETE", "/api/patterns/..%2F..%2Fvictim.sql")
    assert status == 400
    assert victim.exists()


def test_put_skill_rejects_traversal_name(server):
    port, root = server
    status, _ = _request(port, "PUT", "/api/skills/legit",
                        {"name": "../escaped", "body": "x"})
    assert status == 400
    assert not (root.parent / "escaped").exists()


# ── Body-size limit ───────────────────────────────────────────────────────────


def test_oversized_body_rejected(server):
    port, root = server
    huge = {"keyword": "k", "content": "x" * 2_000_000}
    try:
        status, _ = _request(port, "POST", "/api/keywords", huge)
        assert status == 413
    except (ConnectionError, OSError):
        # Server rejected and closed the connection before reading the body —
        # acceptable: the oversized payload was never buffered or processed.
        pass
    # Either way, the oversized keyword must not have been written.
    kw_path = knowledge_dir(root) / "keywords.yaml"
    if kw_path.exists():
        import yaml
        assert "k" not in (yaml.safe_load(kw_path.read_text()) or {})


# ── Host-header validation (DNS-rebinding defense) ────────────────────────────


def test_foreign_host_header_rejected(server):
    port, _ = server
    status, _ = _request(port, "GET", "/api/status",
                        headers={"Host": "evil.example.com"})
    assert status == 403

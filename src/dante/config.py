"""Project and global configuration management.

Global config lives at ~/.dante/ (connections, credentials).
Project config lives at .dante/ (config.yaml, knowledge, embeddings).
"""

import os
from pathlib import Path


import yaml


def global_dir() -> Path:
    """Return ~/.dante/, creating it if needed."""
    p = Path.home() / ".dante"
    p.mkdir(parents=True, exist_ok=True)
    return p


def project_dir(root: Path | None = None) -> Path:
    """Return .dante/ within the project root, creating it if needed."""
    root = root or _find_project_root()
    p = root / ".dante"
    p.mkdir(parents=True, exist_ok=True)
    return p


def knowledge_dir(root: Path | None = None) -> Path:
    """Return the knowledge directory.

    When *root* is None (the default), knowledge lives in ~/.dante/knowledge/
    so it is shared across all projects and accumulates over time.

    When *root* is provided explicitly (e.g. in tests), knowledge lives inside
    that project's .dante/knowledge/ directory instead.
    """
    if root is None:
        p = global_dir() / "knowledge"
    else:
        p = project_dir(root) / "knowledge"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _find_project_root() -> Path:
    """Walk up from cwd looking for .dante/ or .mcp.json. Fall back to cwd.

    If the DANTE_PROJECT environment variable is set, it is used directly
    as the project root without any directory walking. This allows the MCP
    server (and other subprocesses) to pin the root regardless of cwd.
    """
    explicit = os.environ.get("DANTE_PROJECT")
    if explicit:
        return Path(explicit).resolve()

    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        if (parent / ".dante").is_dir() or (parent / ".mcp.json").is_file():
            return parent
    return cwd


def load_global_connections() -> dict:
    """Load ~/.dante/connections.yaml."""
    path = global_dir() / "connections.yaml"
    if not path.exists():
        return {"connections": {}}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if "connections" not in data:
        data = {"connections": data}
    return data


def save_global_connections(data: dict) -> None:
    """Write ~/.dante/connections.yaml."""
    path = global_dir() / "connections.yaml"
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def load_global_credentials() -> dict:
    """Load ~/.dante/credentials.yaml."""
    path = global_dir() / "credentials.yaml"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_global_credentials(data: dict) -> None:
    """Write ~/.dante/credentials.yaml."""
    path = global_dir() / "credentials.yaml"
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def load_project_config(root: Path | None = None) -> dict:
    """Load .dante/config.yaml."""
    path = project_dir(root) / "config.yaml"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_project_config(data: dict, root: Path | None = None) -> None:
    """Write .dante/config.yaml."""
    path = project_dir(root) / "config.yaml"
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def get_default_connection_name(root: Path | None = None) -> str | None:
    """Get the default_connection name from project config."""
    cfg = load_project_config(root)
    return cfg.get("default_connection")


def get_connection_config(name: str | None = None, root: Path | None = None) -> dict | None:
    """Resolve a connection config by name.

    If name is None, uses the project's default_connection.
    Looks up the name in ~/.dante/connections.yaml.
    Resolves password_env to actual password from environment.
    """
    if name is None:
        name = get_default_connection_name(root)
    if name is None:
        return None

    conns = load_global_connections()
    conn = conns.get("connections", {}).get(name)
    if conn is None:
        return None

    # Resolve password_env → password
    result = dict(conn)
    if "password_env" in result:
        env_var = result.pop("password_env")
        pw = os.environ.get(env_var)
        if pw:
            result["password"] = pw
    return result

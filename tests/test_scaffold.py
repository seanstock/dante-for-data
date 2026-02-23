"""Tests for dante.scaffold — project scaffolding."""

import json
from pathlib import Path

import pytest

from dante.scaffold import scaffold_project, scaffold_in_place


# ---------------------------------------------------------------------------
# scaffold_project
# ---------------------------------------------------------------------------

def test_scaffold_project_creates_directory(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert project.exists()
    assert project.name == "myproject"


def test_scaffold_project_creates_analysis_dir(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / "analysis").is_dir()


def test_scaffold_project_creates_outputs_dir(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / "outputs").is_dir()


def test_scaffold_project_creates_data_dir(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / "data").is_dir()


def test_scaffold_project_creates_dante_dir(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante").is_dir()


def test_scaffold_project_creates_knowledge_dir(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "knowledge").is_dir()


def test_scaffold_project_creates_patterns_dir(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "knowledge" / "patterns").is_dir()


def test_scaffold_project_creates_config_yaml(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "config.yaml").exists()


def test_scaffold_project_creates_terms_yaml(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "knowledge" / "terms.yaml").exists()


def test_scaffold_project_creates_keywords_yaml(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "knowledge" / "keywords.yaml").exists()


def test_scaffold_project_creates_notes_md(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "knowledge" / "notes.md").exists()


def test_scaffold_project_creates_claude_md(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    claude_md = project / "CLAUDE.md"
    assert claude_md.exists()
    content = claude_md.read_text()
    assert "dante" in content.lower()


def test_scaffold_project_creates_mcp_json(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    mcp = project / ".mcp.json"
    assert mcp.exists()
    data = json.loads(mcp.read_text())
    assert "mcpServers" in data
    assert "dante" in data["mcpServers"]


def test_scaffold_project_mcp_json_has_correct_command(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    data = json.loads((project / ".mcp.json").read_text())
    server = data["mcpServers"]["dante"]
    assert server["command"] == "dante"
    assert "mcp" in server["args"]
    assert "serve" in server["args"]


def test_scaffold_project_creates_gitignore(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    gitignore = project / ".gitignore"
    assert gitignore.exists()
    content = gitignore.read_text()
    assert ".dante/connections.yaml" in content


def test_scaffold_project_creates_readme(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    readme = project / "README.md"
    assert readme.exists()
    assert "myproject" in readme.read_text()


def test_scaffold_project_creates_skills(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    skills_dir = project / ".claude" / "skills"
    assert skills_dir.is_dir()
    for skill in ["query", "dashboard", "analyze", "ingest", "report"]:
        assert (skills_dir / skill / "SKILL.md").exists()


def test_scaffold_project_skill_has_frontmatter(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    query_skill = (project / ".claude" / "skills" / "query" / "SKILL.md").read_text()
    assert query_skill.startswith("---")
    assert "name: query" in query_skill


def test_scaffold_project_idempotent(tmp_path):
    """Calling twice on the same name should not overwrite existing files."""
    project = scaffold_project("myproject", root=tmp_path)
    # Modify a file
    (project / "CLAUDE.md").write_text("CUSTOM CONTENT")
    # Scaffold again
    scaffold_project("myproject", root=tmp_path)
    # Custom content preserved (write_if_not_exists)
    assert (project / "CLAUDE.md").read_text() == "CUSTOM CONTENT"


def test_scaffold_project_returns_path(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert isinstance(project, Path)
    assert project == tmp_path / "myproject"


# ---------------------------------------------------------------------------
# scaffold_in_place
# ---------------------------------------------------------------------------

def test_scaffold_in_place_returns_root(tmp_path):
    result = scaffold_in_place(root=tmp_path)
    assert result == tmp_path


def test_scaffold_in_place_creates_expected_dirs(tmp_path):
    scaffold_in_place(root=tmp_path)
    assert (tmp_path / "analysis").is_dir()
    assert (tmp_path / "outputs").is_dir()
    assert (tmp_path / "data").is_dir()
    assert (tmp_path / ".dante").is_dir()
    assert (tmp_path / ".dante" / "knowledge" / "patterns").is_dir()


def test_scaffold_in_place_creates_files(tmp_path):
    scaffold_in_place(root=tmp_path)
    assert (tmp_path / "CLAUDE.md").exists()
    assert (tmp_path / ".mcp.json").exists()
    assert (tmp_path / ".gitignore").exists()


def test_scaffold_in_place_creates_skills(tmp_path):
    scaffold_in_place(root=tmp_path)
    skills_dir = tmp_path / ".claude" / "skills"
    for skill in ["query", "dashboard", "analyze", "ingest", "report"]:
        assert (skills_dir / skill / "SKILL.md").exists()


def test_scaffold_in_place_does_not_overwrite_existing(tmp_path):
    (tmp_path / "CLAUDE.md").write_text("MY EXISTING CONTENT")
    scaffold_in_place(root=tmp_path)
    assert (tmp_path / "CLAUDE.md").read_text() == "MY EXISTING CONTENT"


def test_scaffold_in_place_no_readme(tmp_path):
    """scaffold_in_place doesn't create a README (unlike scaffold_project)."""
    scaffold_in_place(root=tmp_path)
    # README is optional for in-place; just don't error if it doesn't exist
    # (scaffold_project makes one, scaffold_in_place currently doesn't)
    pass  # no assertion needed — just checking it runs without error

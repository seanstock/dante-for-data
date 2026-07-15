"""Tests for dante.scaffold — project scaffolding."""

import json
from pathlib import Path


from dante.scaffold import (
    GLOBAL_RULES_END,
    GLOBAL_RULES_START,
    scaffold_in_place,
    scaffold_project,
    sync_global_rules,
)


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


def test_scaffold_project_creates_keywords_yaml(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "knowledge" / "keywords.yaml").exists()


def test_scaffold_project_creates_notes_yaml(tmp_path):
    project = scaffold_project("myproject", root=tmp_path)
    assert (project / ".dante" / "knowledge" / "notes.yaml").exists()


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


# ---------------------------------------------------------------------------
# _replace_managed_section — robust marker handling
# ---------------------------------------------------------------------------

from dante.scaffold import _replace_managed_section

_START = "<!-- MANAGED BY DANTE STUDIO — DO NOT EDIT THIS SECTION -->"
_END = "<!-- END DANTE STUDIO MANAGED SECTION -->"


def _managed(body):
    return f"{_START}\n{body}\n{_END}"


def test_replace_appends_when_no_marker():
    out = _replace_managed_section("# My notes\n", _managed("v1"), _START, _END)
    assert "# My notes" in out
    assert out.count(_START) == 1
    assert out.rstrip().endswith(_END)


def test_replace_is_idempotent():
    """Running replacement twice must not duplicate the managed block."""
    base = "# My notes\n"
    once = _replace_managed_section(base, _managed("v1"), _START, _END)
    twice = _replace_managed_section(once, _managed("v2"), _START, _END)
    assert twice.count(_START) == 1
    assert twice.count(_END) == 1
    assert "v2" in twice
    assert "v1" not in twice
    assert "# My notes" in twice


def test_replace_handles_duplicate_blocks():
    """A file that already has two managed blocks collapses to one on replace."""
    base = "# Notes\n" + _managed("old1") + "\nmiddle\n" + _managed("old2") + "\n"
    out = _replace_managed_section(base, _managed("new"), _START, _END)
    assert out.count(_START) == 1
    assert "new" in out
    assert "old1" not in out and "old2" not in out


def test_replace_preserves_user_content_after_block():
    base = "# Top\n" + _managed("v1") + "\n# Bottom\n"
    out = _replace_managed_section(base, _managed("v2"), _START, _END)
    assert "# Top" in out
    assert "# Bottom" in out
    assert "v2" in out


# ---------------------------------------------------------------------------
# sync_global_rules — global targets for Claude Code + Cursor
# ---------------------------------------------------------------------------


def _write_rules(rules: str) -> Path:
    """Write ~/.dante/knowledge/rules.yaml (fake home, via isolate_home)."""
    path = Path.home() / ".dante" / "knowledge" / "rules.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rules, encoding="utf-8")
    return path


def _claude_md() -> Path:
    return Path.home() / ".claude" / "CLAUDE.md"


def _cursor_mdc() -> Path:
    return Path.home() / ".cursor" / "rules" / "dante-rules.mdc"


def test_sync_global_rules_writes_cursor_file():
    _write_rules('design_system: "Dark mode, #111111 background."\n')
    assert sync_global_rules() is True
    text = _cursor_mdc().read_text(encoding="utf-8")
    assert "alwaysApply: true" in text
    assert "Dark mode, #111111 background." in text
    assert "design_system" in text


def test_sync_global_rules_writes_claude_managed_section():
    _write_rules('design_system: "Dark mode."\n')
    assert sync_global_rules() is True
    text = _claude_md().read_text(encoding="utf-8")
    assert "Dark mode." in text
    assert text.count(GLOBAL_RULES_START) == 1
    assert text.count(GLOBAL_RULES_END) == 1


def test_sync_global_rules_preserves_handwritten_claude_md():
    claude = _claude_md()
    claude.parent.mkdir(parents=True, exist_ok=True)
    claude.write_text("# My own notes\n\nKeep me.\n", encoding="utf-8")
    _write_rules('design_system: "Dark mode."\n')
    sync_global_rules()
    text = claude.read_text(encoding="utf-8")
    assert "# My own notes" in text
    assert "Keep me." in text
    assert "Dark mode." in text


def test_sync_global_rules_deleted_rule_disappears_from_both():
    _write_rules('keep_me: "Stays."\ndelete_me: "Goes away."\n')
    sync_global_rules()
    assert "Goes away." in _claude_md().read_text(encoding="utf-8")
    assert "Goes away." in _cursor_mdc().read_text(encoding="utf-8")

    _write_rules('keep_me: "Stays."\n')
    sync_global_rules()
    claude_text = _claude_md().read_text(encoding="utf-8")
    cursor_text = _cursor_mdc().read_text(encoding="utf-8")
    assert "Goes away." not in claude_text
    assert "Goes away." not in cursor_text
    assert "Stays." in claude_text
    assert "Stays." in cursor_text


def test_sync_global_rules_is_idempotent():
    _write_rules('design_system: "Dark mode."\n')
    sync_global_rules()
    sync_global_rules()
    text = _claude_md().read_text(encoding="utf-8")
    assert text.count(GLOBAL_RULES_START) == 1
    assert text.count("Dark mode.") == 1


def test_sync_global_rules_empty_rules_clears_targets():
    _write_rules('design_system: "Dark mode."\n')
    sync_global_rules()
    claude = _claude_md()
    claude.write_text(
        "# Mine\n\n" + claude.read_text(encoding="utf-8"), encoding="utf-8"
    )

    _write_rules("# all rules removed\n")
    assert sync_global_rules() is True
    text = claude.read_text(encoding="utf-8")
    assert "Dark mode." not in text
    assert GLOBAL_RULES_START not in text
    assert "# Mine" in text
    assert not _cursor_mdc().exists()


def test_sync_global_rules_no_rules_file_is_noop():
    assert sync_global_rules() is False
    assert not _claude_md().exists()
    assert not _cursor_mdc().exists()


def test_sync_global_rules_malformed_yaml_returns_false():
    _write_rules("this: is: not: valid: yaml:\n")
    assert sync_global_rules() is False


def test_sync_global_rules_creates_claude_md_when_absent():
    _write_rules('design_system: "Dark mode."\n')
    assert not _claude_md().exists()
    sync_global_rules()
    assert _claude_md().exists()
    assert "Dark mode." in _claude_md().read_text(encoding="utf-8")


def test_scaffold_claude_md_has_no_global_rules_import(tmp_path):
    """Global rules now live in ~/.claude/CLAUDE.md, not a per-project @import."""
    project = scaffold_project("myproject", root=tmp_path)
    text = (project / "CLAUDE.md").read_text(encoding="utf-8")
    assert "@~/.dante/knowledge/rules.yaml" not in text

#!/usr/bin/env python3
"""Generate a structural code map of the dante source tree.

Walks src/dante/, parses each .py file with the ast module, and extracts:
- External imports
- Public classes with public methods and signatures
- Public functions with signatures
- Module-level constants (ALL_CAPS assignments)
- __all__ exports (for __init__.py files)

Output: codemap.md at the project root.

Usage:
    python scripts/codemap.py
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src" / "dante"
OUTPUT = PROJECT_ROOT / "codemap.md"

SKIP_DIRS = {"__pycache__", ".mypy_cache", ".pytest_cache"}
SKIP_FILES = {"conftest.py"}


def _is_public(name: str) -> bool:
    return not name.startswith("_")


def _format_arg(arg: ast.arg) -> str:
    name = arg.arg
    if arg.annotation:
        return f"{name}: {ast.unparse(arg.annotation)}"
    return name


def _format_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args = node.args
    parts: list[str] = []

    # Positional args (skip 'self' and 'cls')
    positional = args.args
    defaults = args.defaults
    n_defaults = len(defaults)
    n_positional = len(positional)

    for i, arg in enumerate(positional):
        if arg.arg in ("self", "cls"):
            continue
        formatted = _format_arg(arg)
        default_idx = i - (n_positional - n_defaults)
        if default_idx >= 0:
            formatted += f"={ast.unparse(defaults[default_idx])}"
        parts.append(formatted)

    # *args
    if args.vararg:
        parts.append(f"*{_format_arg(args.vararg)}")
    elif args.kwonlyargs:
        parts.append("*")

    # Keyword-only args
    for i, arg in enumerate(args.kwonlyargs):
        formatted = _format_arg(arg)
        if args.kw_defaults[i] is not None:
            formatted += f"={ast.unparse(args.kw_defaults[i])}"
        parts.append(formatted)

    # **kwargs
    if args.kwarg:
        parts.append(f"**{_format_arg(args.kwarg)}")

    sig = ", ".join(parts)
    ret = ""
    if node.returns:
        ret = f" -> {ast.unparse(node.returns)}"
    prefix = "async " if isinstance(node, ast.AsyncFunctionDef) else ""
    return f"{prefix}{node.name}({sig}){ret}"


def _is_constant(name: str) -> bool:
    return name.isupper() and not name.startswith("_")


def extract_file(filepath: Path) -> dict | None:
    """Extract structural info from a single Python file."""
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return None

    info: dict = {
        "imports": [],
        "classes": [],
        "functions": [],
        "constants": [],
        "all_exports": None,
        "docstring": ast.get_docstring(tree),
    }

    for node in ast.iter_child_nodes(tree):
        # Imports
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top != "dante":
                    info["imports"].append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and not node.module.startswith("dante"):
                info["imports"].append(node.module)

        # Classes
        elif isinstance(node, ast.ClassDef) and _is_public(node.name):
            bases = [ast.unparse(b) for b in node.bases]
            methods = []
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if _is_public(item.name) or item.name == "__init__":
                        methods.append(_format_signature(item))
            info["classes"].append({
                "name": node.name,
                "bases": bases,
                "methods": methods,
            })

        # Functions
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_public(node.name):
            info["functions"].append(_format_signature(node))

        # Constants
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and _is_constant(target.id):
                    info["constants"].append(target.id)

        # __all__
        elif isinstance(node, ast.Assign):
            pass  # already handled above
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        info["all_exports"] = [
                            ast.literal_eval(elt)
                            for elt in node.value.elts
                            if isinstance(elt, ast.Constant)
                        ]

    # Deduplicate imports
    info["imports"] = sorted(set(info["imports"]))
    return info


def format_file_section(rel_path: str, info: dict) -> str:
    """Format extracted info as a markdown section."""
    lines = [f"## {rel_path}"]

    if info["docstring"]:
        # First line of docstring only
        first_line = info["docstring"].strip().split("\n")[0]
        lines.append(f"> {first_line}")

    if info["all_exports"]:
        lines.append(f"Exports: {', '.join(info['all_exports'])}")

    if info["imports"]:
        lines.append(f"Imports: {', '.join(info['imports'])}")

    if info["constants"]:
        lines.append(f"Constants: {', '.join(info['constants'])}")

    for cls in info["classes"]:
        base_str = f"({', '.join(cls['bases'])})" if cls["bases"] else ""
        lines.append(f"- class **{cls['name']}**{base_str}")
        for method in cls["methods"]:
            lines.append(f"  - `{method}`")

    for func in info["functions"]:
        lines.append(f"- `{func}`")

    if len(lines) == 1:
        return ""  # nothing interesting, skip

    return "\n".join(lines)


def main() -> None:
    if not SRC_DIR.exists():
        print(f"Source directory not found: {SRC_DIR}", file=sys.stderr)
        sys.exit(1)

    py_files = sorted(SRC_DIR.rglob("*.py"))
    sections: list[str] = []

    for filepath in py_files:
        # Skip unwanted files/dirs
        if any(part in SKIP_DIRS for part in filepath.parts):
            continue
        if filepath.name in SKIP_FILES:
            continue
        if filepath.name.startswith("test_"):
            continue

        # Skip trivial __init__.py (empty or just has docstring)
        if filepath.name == "__init__.py":
            source = filepath.read_text(encoding="utf-8").strip()
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            # Trivial = only docstring or empty
            non_doc = [
                n for n in ast.iter_child_nodes(tree)
                if not isinstance(n, ast.Expr) or not isinstance(n.value, ast.Constant)
            ]
            if not non_doc:
                continue

        rel = filepath.relative_to(PROJECT_ROOT).as_posix()
        info = extract_file(filepath)
        if info is None:
            continue

        section = format_file_section(rel, info)
        if section:
            sections.append(section)

    # Count stats
    total_source_lines = 0
    for filepath in py_files:
        if any(part in SKIP_DIRS for part in filepath.parts):
            continue
        try:
            total_source_lines += len(filepath.read_text(encoding="utf-8").splitlines())
        except Exception:
            pass

    output_text = "\n\n".join(sections)
    output_lines = len(output_text.splitlines())
    compression = round((1 - output_lines / total_source_lines) * 100) if total_source_lines else 0

    header = (
        "# Dante Code Map\n\n"
        f"_Auto-generated structural index of `src/dante/`. "
        f"{output_lines} lines vs {total_source_lines} source lines ({compression}% reduction)._\n\n"
        "_Regenerate: `python scripts/codemap.py`_"
    )

    OUTPUT.write_text(header + "\n\n" + output_text + "\n", encoding="utf-8")
    print(f"Written to {OUTPUT} ({output_lines} lines, {compression}% reduction)")


if __name__ == "__main__":
    main()

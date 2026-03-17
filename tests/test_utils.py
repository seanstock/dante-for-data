"""Tests for dante._utils — shared internal utilities."""

from pathlib import Path


from dante._utils import ensure_outputs_dir, slugify


# ---------------------------------------------------------------------------
# slugify
# ---------------------------------------------------------------------------


def test_slugify_basic():
    assert slugify("Hello World") == "hello-world"


def test_slugify_preserves_numbers():
    assert slugify("Revenue 2024 Q3") == "revenue-2024-q3"


def test_slugify_strips_special():
    assert slugify("What's the ARR ($M)?") == "whats-the-arr-m"


def test_slugify_collapses_dashes():
    assert slugify("a -- b --- c") == "a-b-c"


def test_slugify_strips_leading_trailing_dashes():
    assert slugify("---hello---") == "hello"


def test_slugify_empty_returns_fallback():
    assert slugify("") == "file"
    assert slugify("", fallback="chart") == "chart"


def test_slugify_max_len():
    result = slugify("a" * 200, max_len=10)
    assert len(result) <= 10


def test_slugify_unicode():
    result = slugify("café résumé")
    assert "cafe" in result


# ---------------------------------------------------------------------------
# ensure_outputs_dir
# ---------------------------------------------------------------------------


def test_ensure_outputs_dir_creates_directory(tmp_path):
    outputs = tmp_path / "outputs"
    assert not outputs.exists()
    result = ensure_outputs_dir(root=tmp_path)
    assert result == outputs
    assert outputs.is_dir()


def test_ensure_outputs_dir_idempotent(tmp_path):
    result1 = ensure_outputs_dir(root=tmp_path)
    result2 = ensure_outputs_dir(root=tmp_path)
    assert result1 == result2


def test_ensure_outputs_dir_returns_path_object(tmp_path):
    result = ensure_outputs_dir(root=tmp_path)
    assert isinstance(result, Path)


def test_ensure_outputs_dir_existing_dir_untouched(tmp_path):
    """If outputs/ already exists with a file in it, don't clobber it."""
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    marker = outputs / "existing.txt"
    marker.write_text("keep me")
    ensure_outputs_dir(root=tmp_path)
    assert marker.read_text() == "keep me"

"""Tests for dante.remote — RemoteKnowledge client and _get_remote_client helper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dante.remote import RemoteKnowledge, _get_remote_client


# ---------------------------------------------------------------------------
# _get_remote_client
# ---------------------------------------------------------------------------


def test_get_remote_client_returns_none_when_not_configured(tmp_path):
    """Returns None when neither project nor global config has remote settings."""
    # tmp_path has no .dante/config.yaml at all
    client = _get_remote_client(root=tmp_path)
    assert client is None


def test_get_remote_client_returns_none_when_config_has_no_remote_key(tmp_path):
    """Returns None when config exists but lacks 'remote' section."""
    from dante.config import save_project_config

    save_project_config({"default_connection": "dev"}, root=tmp_path)
    client = _get_remote_client(root=tmp_path)
    assert client is None


def test_get_remote_client_returns_none_when_api_url_missing(tmp_path):
    """Returns None when 'remote' section exists but api_url is absent."""
    from dante.config import save_project_config

    save_project_config({"remote": {"api_key": "sk-abc"}}, root=tmp_path)
    client = _get_remote_client(root=tmp_path)
    assert client is None


def test_get_remote_client_returns_none_when_api_key_missing(tmp_path):
    """Returns None when 'remote' section exists but api_key is absent."""
    from dante.config import save_project_config

    save_project_config(
        {"remote": {"api_url": "https://api.example.com"}}, root=tmp_path
    )
    client = _get_remote_client(root=tmp_path)
    assert client is None


def test_get_remote_client_uses_project_config(tmp_path):
    """Returns a client when project config has both api_url and api_key."""
    from dante.config import save_project_config

    save_project_config(
        {"remote": {"api_url": "https://api.example.com", "api_key": "sk-test"}},
        root=tmp_path,
    )
    client = _get_remote_client(root=tmp_path)
    assert client is not None
    assert isinstance(client, RemoteKnowledge)
    assert client.api_url == "https://api.example.com"
    assert client.api_key == "sk-test"


def test_get_remote_client_strips_trailing_slash(tmp_path):
    """api_url trailing slash is stripped by RemoteKnowledge.__init__."""
    from dante.config import save_project_config

    save_project_config(
        {"remote": {"api_url": "https://api.example.com/", "api_key": "sk-test"}},
        root=tmp_path,
    )
    client = _get_remote_client(root=tmp_path)
    assert client is not None
    assert not client.api_url.endswith("/")


def test_get_remote_client_uses_global_config_as_fallback(tmp_path, monkeypatch):
    """Falls back to global config when project config has no remote settings."""
    import yaml

    # Use tmp_path as the "home" directory so global config lives at
    # tmp_path/.dante/config.yaml.  The *project* root is a subdirectory so
    # the two config files are distinct.
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home_dir))

    global_dante = home_dir / ".dante"
    global_dante.mkdir(parents=True, exist_ok=True)
    (global_dante / "config.yaml").write_text(
        yaml.dump({"remote": {"api_url": "https://global.example.com", "api_key": "gk-1"}})
    )

    # Project root is separate; its config has no remote section
    project_root = tmp_path / "project"
    project_root.mkdir()
    from dante.config import save_project_config

    save_project_config({"default_connection": "dev"}, root=project_root)

    client = _get_remote_client(root=project_root)
    assert client is not None
    assert client.api_url == "https://global.example.com"
    assert client.api_key == "gk-1"


def test_get_remote_client_project_config_takes_priority(tmp_path, monkeypatch):
    """Project config takes priority over global config."""
    import yaml

    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home_dir))

    global_dante = home_dir / ".dante"
    global_dante.mkdir(parents=True, exist_ok=True)
    (global_dante / "config.yaml").write_text(
        yaml.dump({"remote": {"api_url": "https://global.example.com", "api_key": "gk-global"}})
    )

    project_root = tmp_path / "project"
    project_root.mkdir()
    from dante.config import save_project_config

    save_project_config(
        {"remote": {"api_url": "https://project.example.com", "api_key": "pk-project"}},
        root=project_root,
    )

    client = _get_remote_client(root=project_root)
    assert client is not None
    assert client.api_url == "https://project.example.com"
    assert client.api_key == "pk-project"


# ---------------------------------------------------------------------------
# RemoteKnowledge.__init__
# ---------------------------------------------------------------------------


def test_remote_knowledge_stores_url_and_key():
    client = RemoteKnowledge("https://api.example.com", "sk-abc123")
    assert client.api_url == "https://api.example.com"
    assert client.api_key == "sk-abc123"


def test_remote_knowledge_strips_trailing_slash():
    client = RemoteKnowledge("https://api.example.com/", "sk-abc")
    assert client.api_url == "https://api.example.com"


# ---------------------------------------------------------------------------
# RemoteKnowledge.search
# ---------------------------------------------------------------------------


def test_search_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")
    mock_response = [{"question": "What is churn?", "sql": "SELECT 1", "similarity": 0.9}]

    with patch("dante.remote._http_request", return_value=mock_response) as mock_req:
        results = client.search("churn rate", top_k=5)

    mock_req.assert_called_once_with(
        "https://api.example.com/knowledge/search",
        method="POST",
        body={"query": "churn rate", "top_k": 5, "source": "library"},
        api_key="sk-test",
    )
    assert results == mock_response


def test_search_sends_source_library():
    """The search request body must always include source='library'."""
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value=[]) as mock_req:
        client.search("revenue")

    _, kwargs = mock_req.call_args
    assert kwargs["body"]["source"] == "library"


def test_search_returns_list_on_unexpected_response():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value={"error": "bad"}):
        results = client.search("anything")

    assert results == []


def test_search_default_top_k():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value=[]) as mock_req:
        client.search("test query")

    _, kwargs = mock_req.call_args
    assert kwargs["body"]["top_k"] == 10


# ---------------------------------------------------------------------------
# RemoteKnowledge.save_pattern
# ---------------------------------------------------------------------------


def test_save_pattern_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")
    expected = {"id": "abc", "question": "What is revenue?"}

    with patch("dante.remote._http_request", return_value=expected) as mock_req:
        result = client.save_pattern(
            question="What is revenue?",
            sql="SELECT SUM(amount) FROM orders",
            tables=["orders"],
            description="Total revenue",
        )

    mock_req.assert_called_once_with(
        "https://api.example.com/knowledge/patterns",
        method="POST",
        body={
            "question": "What is revenue?",
            "sql": "SELECT SUM(amount) FROM orders",
            "tables": ["orders"],
            "description": "Total revenue",
        },
        api_key="sk-test",
    )
    assert result == expected


def test_save_pattern_defaults_tables_and_description():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value={}) as mock_req:
        client.save_pattern("Q?", "SELECT 1")

    _, kwargs = mock_req.call_args
    assert kwargs["body"]["tables"] == []
    assert kwargs["body"]["description"] == ""


def test_save_pattern_returns_dict_on_unexpected_response():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value=[1, 2, 3]):
        result = client.save_pattern("Q?", "SELECT 1")

    assert result == {}


# ---------------------------------------------------------------------------
# RemoteKnowledge.list_patterns
# ---------------------------------------------------------------------------


def test_list_patterns_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")
    expected = [{"id": "p1"}, {"id": "p2"}]

    with patch("dante.remote._http_request", return_value=expected) as mock_req:
        result = client.list_patterns()

    mock_req.assert_called_once_with(
        "https://api.example.com/knowledge/patterns?limit=50&offset=0",
        method="GET",
        body=None,
        api_key="sk-test",
    )
    assert result == expected


def test_list_patterns_with_status():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value=[]) as mock_req:
        client.list_patterns(status="active", limit=10, offset=5)

    call_url = mock_req.call_args[0][0]
    assert "status=active" in call_url
    assert "limit=10" in call_url
    assert "offset=5" in call_url


# ---------------------------------------------------------------------------
# RemoteKnowledge.edit_pattern
# ---------------------------------------------------------------------------


def test_edit_pattern_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")
    expected = {"id": "p1", "description": "updated"}

    with patch("dante.remote._http_request", return_value=expected) as mock_req:
        result = client.edit_pattern("p1", description="updated")

    mock_req.assert_called_once_with(
        "https://api.example.com/knowledge/patterns/p1",
        method="PATCH",
        body={"description": "updated"},
        api_key="sk-test",
    )
    assert result == expected


# ---------------------------------------------------------------------------
# RemoteKnowledge.delete_pattern
# ---------------------------------------------------------------------------


def test_delete_pattern_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value=None) as mock_req:
        result = client.delete_pattern("p1")

    mock_req.assert_called_once_with(
        "https://api.example.com/knowledge/patterns/p1",
        method="DELETE",
        body=None,
        api_key="sk-test",
    )
    assert result is True


def test_delete_pattern_returns_false_on_connection_error():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", side_effect=ConnectionError("404")):
        result = client.delete_pattern("ghost")

    assert result is False


# ---------------------------------------------------------------------------
# RemoteKnowledge.list_keywords
# ---------------------------------------------------------------------------


def test_list_keywords_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")
    expected = {"keywords": [{"keyword": "revenue", "content": "SUM(amount)"}]}

    with patch("dante.remote._http_request", return_value=expected) as mock_req:
        result = client.list_keywords(scope="org")

    mock_req.assert_called_once_with(
        "https://api.example.com/api/keywords?scope=org",
        method="GET",
        body=None,
        api_key="sk-test",
    )
    assert result == expected["keywords"]


def test_list_keywords_returns_empty_on_unexpected_response():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value="bad"):
        result = client.list_keywords()

    assert result == []


def test_create_keyword_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")
    expected = {"id": "k1", "keyword": "revenue", "content": "SUM(amount)"}

    with patch("dante.remote._http_request", return_value=expected) as mock_req:
        result = client.create_keyword("revenue", "SUM(amount)")

    mock_req.assert_called_once_with(
        "https://api.example.com/api/keywords",
        method="POST",
        body={"keyword": "revenue", "content": "SUM(amount)", "scope": "org"},
        api_key="sk-test",
    )
    assert result == expected


def test_delete_keyword_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value=None) as mock_req:
        result = client.delete_keyword("k1")

    mock_req.assert_called_once_with(
        "https://api.example.com/api/keywords/k1",
        method="DELETE",
        body=None,
        api_key="sk-test",
    )
    assert result is True


def test_delete_keyword_returns_false_on_connection_error():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", side_effect=ConnectionError("404")):
        result = client.delete_keyword("ghost")

    assert result is False


# ---------------------------------------------------------------------------
# RemoteKnowledge.stats
# ---------------------------------------------------------------------------


def test_stats_calls_correct_url():
    client = RemoteKnowledge("https://api.example.com", "sk-test")
    expected = {"pattern_count": 42, "keyword_count": 7}

    with patch("dante.remote._http_request", return_value=expected) as mock_req:
        result = client.stats()

    mock_req.assert_called_once_with(
        "https://api.example.com/knowledge/stats",
        method="GET",
        body=None,
        api_key="sk-test",
    )
    assert result == expected


def test_stats_returns_empty_dict_on_unexpected_response():
    client = RemoteKnowledge("https://api.example.com", "sk-test")

    with patch("dante.remote._http_request", return_value=[1, 2]):
        result = client.stats()

    assert result == {}


# ---------------------------------------------------------------------------
# _http_request error handling
# ---------------------------------------------------------------------------


def test_http_request_raises_connection_error_on_http_error():
    """HTTPError from urllib is converted to ConnectionError."""
    import urllib.error
    from dante.remote import _http_request

    http_err = urllib.error.HTTPError(
        url="http://x.com", code=500, msg="Internal Server Error", hdrs=None, fp=None
    )

    with patch("urllib.request.urlopen", side_effect=http_err):
        with pytest.raises(ConnectionError, match="HTTP 500"):
            _http_request("http://x.com/path", api_key="k")


def test_http_request_raises_connection_error_on_url_error():
    """URLError from urllib is converted to ConnectionError."""
    import urllib.error
    from dante.remote import _http_request

    url_err = urllib.error.URLError(reason="Name or service not known")

    with patch("urllib.request.urlopen", side_effect=url_err):
        with pytest.raises(ConnectionError, match="Network error"):
            _http_request("http://x.com/path", api_key="k")

"""Tests for connection lifecycle management in dante.connect."""

import time
from unittest.mock import patch

from dante.connect import connect, dispose_all, _engines, _engines_lock, _engine_last_used


def _clean_engines():
    """Clear cached engines before/after each test."""
    with _engines_lock:
        for e in _engines.values():
            try:
                e.dispose()
            except Exception:
                pass
        _engines.clear()
        _engine_last_used.clear()


def test_pool_pre_ping_enabled():
    """Engines should be created with pool_pre_ping for health checking."""
    _clean_engines()
    try:
        engine = connect(url="sqlite:///:memory:")
        assert engine.pool._pre_ping is True
    finally:
        _clean_engines()


def test_dispose_all_clears_engines():
    _clean_engines()
    try:
        connect(url="sqlite:///test_dispose_1.db")
        connect(url="sqlite:///test_dispose_2.db")
        with _engines_lock:
            assert len(_engines) >= 2
        dispose_all()
        with _engines_lock:
            assert len(_engines) == 0
            assert len(_engine_last_used) == 0
    finally:
        _clean_engines()


def test_idle_timeout_disposes_engine():
    """Engines idle beyond the timeout should be recreated on next access."""
    _clean_engines()
    try:
        url = "sqlite:///:memory:"
        e1 = connect(url=url)

        # Simulate the engine being idle for longer than timeout
        with _engines_lock:
            _engine_last_used[url] = time.monotonic() - 700  # 700s > 600s timeout

        e2 = connect(url=url)
        # Should get a new engine since the old one was idle
        assert e2 is not e1
    finally:
        _clean_engines()


def test_last_used_updated_on_access():
    """Accessing an engine should update its last-used timestamp."""
    _clean_engines()
    try:
        url = "sqlite:///:memory:"
        connect(url=url)

        with _engines_lock:
            t1 = _engine_last_used[url]

        time.sleep(0.05)
        connect(url=url)

        with _engines_lock:
            t2 = _engine_last_used[url]

        assert t2 > t1
    finally:
        _clean_engines()

"""Unit tests for the downloader's HTTP layer.

These tests cover contracts introduced when adopting `requests.Session`:

1. Every download function raises `requests.HTTPError` on non-2xx
   responses rather than silently propagating garbled bytes into a
   downstream parser (e.g. zipfile choking on a 404 HTML page).
2. The shared `session` has a plausibly-current Chrome User-Agent.
   AEMO's `aemo.com.au` host filters out stale UAs as scrapers, so
   this matters for the registration-list endpoint in particular.

Tests use the `aemo_mock_server` fixture directly so they hit a real
HTTP server (in-process) and exercise the actual `requests` code path
— not a monkeypatch on the function under test.
"""
import tempfile

import pytest
import requests

from nemosis import downloader


def test_session_user_agent_claims_current_chrome():
    """Lock in the contract that the session's default UA looks like a
    current Chrome. Bumping the version requires updating this test;
    that's intentional — it forces a conscious decision."""
    ua = downloader.session.headers["User-Agent"]
    assert "Chrome/130" in ua, ua
    assert "Mozilla/5.0" in ua, ua


def test_download_unzip_csv_raises_on_404(aemo_mock_server):
    """Before this contract, a 404 (e.g. archive doesn't exist for a
    month) was caught downstream when `zipfile.ZipFile` choked on the
    404 HTML response with `BadZipFile` — a confusing error that
    obscured the real cause. Now HTTPError surfaces directly."""
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(requests.HTTPError):
            downloader.download_unzip_csv(
                f"{aemo_mock_server}/does/not/exist.zip", tmp
            )


def test_download_csv_raises_on_404(aemo_mock_server):
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(requests.HTTPError):
            downloader.download_csv(
                f"{aemo_mock_server}/does/not/exist.csv",
                f"{tmp}/out.csv",
            )


def test_download_xl_raises_on_404(aemo_mock_server):
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(requests.HTTPError):
            downloader.download_xl(
                f"{aemo_mock_server}/does/not/exist.xlsx",
                f"{tmp}/out.xlsx",
            )

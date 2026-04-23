"""Shared pytest fixtures.

`nemosis_fixture` is the workhorse: it spins up a local HTTP server that
serves tests/fixtures/data/ (the committed tree built by build.py), swaps
every AEMO URL in `nemosis.defaults` to point at it, and gives the test
a fresh cache directory. Tests that use this fixture behave exactly as
if they were hitting nemweb.com.au, but in milliseconds and offline.
"""
from __future__ import annotations

import http.server
import logging
import socket
import threading
from pathlib import Path

import pytest

from nemosis import defaults


# NEMOSIS's own INFO/DEBUG logs leak into test output and clutter failure
# context. Silence anything below WARNING — genuinely surprising conditions
# (e.g. a 404 on a file we expected to fixture) still surface.
logging.getLogger("nemosis").setLevel(logging.WARNING)

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "data"


# ---------------------------------------------------------------------------
# Local HTTP server (session-scoped — started once, reused across all tests)
# ---------------------------------------------------------------------------

class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_args, **_kwargs) -> None:
        pass   # silence the default per-request stderr logging


def _pick_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def aemo_mock_server() -> str:
    port = _pick_free_port()

    def make_handler(*args, **kwargs):
        return _QuietHandler(*args, directory=str(FIXTURE_ROOT), **kwargs)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), make_handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# URL redirection + isolated cache dir
# ---------------------------------------------------------------------------

def _redirect_url(url: str, base: str) -> str:
    """Rewrite `https://whatever.com/foo/bar` → `{base}/foo/bar`."""
    after_scheme = url.index("://") + 3
    path_start = url.find("/", after_scheme)
    if path_start < 0:
        return base + "/"
    return base + url[path_start:]


@pytest.fixture
def nemosis_fixture(aemo_mock_server, tmp_path, monkeypatch) -> Path:
    """Point nemosis at the local mock server and supply a clean cache dir.

    Returns the cache directory (a tmp_path) so tests can pass it to
    dynamic_data_compiler / cache_compiler / static_table.
    """
    base = aemo_mock_server

    monkeypatch.setattr(defaults, "nem_web_domain_url", base + "/")
    monkeypatch.setattr(defaults, "aemo_mms_url", _redirect_url(defaults.aemo_mms_url, base))
    monkeypatch.setattr(defaults, "fcas_4_url", _redirect_url(defaults.fcas_4_url, base))
    monkeypatch.setattr(defaults, "fcas_4_url_hist", _redirect_url(defaults.fcas_4_url_hist, base))
    monkeypatch.setattr(
        defaults, "static_table_url",
        {k: _redirect_url(v, base) for k, v in defaults.static_table_url.items()},
    )
    monkeypatch.setattr(defaults, "raw_data_cache", str(tmp_path))

    return tmp_path

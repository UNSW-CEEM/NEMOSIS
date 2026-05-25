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


# ---------------------------------------------------------------------------
# Streaming helpers: download_to_path / download_to_dir idempotency
#
# The (path, downloaded) return signal is what keeps keep_zip=False safe for
# concurrent same-cache use: cleanup only fires for zips THIS call actually
# wrote to disk; pre-existing zips from another process are reported as
# `downloaded=False` and left alone.
# ---------------------------------------------------------------------------

def test_download_to_path_returns_true_on_first_call(aemo_mock_server, tmp_path):
    target = tmp_path / "out.csv"
    url = f"{aemo_mock_server}/-/media/files/electricity/nem/settlements_and_payments/settlements/auction-reports/archive/ancillary-services-market-causer-pays-variables-file.csv"

    downloaded = downloader.download_to_path(url, str(target))

    assert downloaded is True
    assert target.is_file()
    assert target.stat().st_size > 0


def test_download_to_path_returns_false_when_file_exists(aemo_mock_server, tmp_path):
    """Idempotency signal: a second call with the destination already
    present must report downloaded=False and skip the network. This is
    the contract keep_zip=False relies on to avoid deleting another
    process's pre-existing zip."""
    target = tmp_path / "out.csv"
    target.write_bytes(b"pre-existing contents - pretend another process wrote this")
    original_bytes = target.read_bytes()

    url = f"{aemo_mock_server}/-/media/files/electricity/nem/settlements_and_payments/settlements/auction-reports/archive/ancillary-services-market-causer-pays-variables-file.csv"

    downloaded = downloader.download_to_path(url, str(target))

    assert downloaded is False
    assert target.read_bytes() == original_bytes, "existing file must not be overwritten"


def test_download_to_path_force_redo_overrides_existing(aemo_mock_server, tmp_path):
    """force_redo=True bypasses the idempotency check and re-downloads
    even if the destination already exists. Useful when the user knows
    the cached file is stale."""
    target = tmp_path / "out.csv"
    target.write_bytes(b"stale contents")

    url = f"{aemo_mock_server}/-/media/files/electricity/nem/settlements_and_payments/settlements/auction-reports/archive/ancillary-services-market-causer-pays-variables-file.csv"

    downloaded = downloader.download_to_path(url, str(target), force_redo=True)

    assert downloaded is True
    assert target.read_bytes() != b"stale contents", "force_redo=True should overwrite"


def test_download_to_dir_forwards_force_redo(aemo_mock_server, tmp_path):
    """Regression test for the PR67 bug where download_to_dir accepted
    force_redo but never forwarded it to download_to_path. With the bug,
    force_redo=True would silently no-op."""
    url = f"{aemo_mock_server}/-/media/files/electricity/nem/settlements_and_payments/settlements/auction-reports/archive/ancillary-services-market-causer-pays-variables-file.csv"

    # First call establishes the file
    path, downloaded_first = downloader.download_to_dir(url, str(tmp_path))
    assert downloaded_first is True

    # Second call without force_redo: should short-circuit
    _, downloaded_second = downloader.download_to_dir(url, str(tmp_path))
    assert downloaded_second is False

    # Third call WITH force_redo: must re-download (bug regression check)
    _, downloaded_third = downloader.download_to_dir(url, str(tmp_path), force_redo=True)
    assert downloaded_third is True, (
        "force_redo=True should re-download; if this fails, download_to_dir "
        "may have stopped forwarding force_redo to download_to_path"
    )

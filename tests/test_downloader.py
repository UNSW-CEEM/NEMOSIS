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


def test_download_xlsx_raises_on_404(aemo_mock_server):
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(requests.HTTPError):
            downloader.download_xlsx(
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


# ---------------------------------------------------------------------------
# TTL-cached parent-directory HTML pre-check
#
# Scanning back into pre-2015 history hits many monthly archive paths that
# 404. Each one was a 200-500ms round-trip. We now fetch each parent
# directory's HTML index once, cache it for ~1 hour, and answer most
# missing-file questions locally. These tests cover:
# 1. download_html caches across calls
# 2. _pre_check_file_is_missing returns False / True / None correctly
# 3. download_to_path surfaces a missing-file pre-check as HTTPError(404)
# 4. URLs outside the nemweb archive paths are not affected (no regression
#    on the existing mock-server-driven test suite)
# ---------------------------------------------------------------------------

@pytest.fixture
def _patch_precheck_prefixes(aemo_mock_server, monkeypatch):
    """Re-point the pre-check prefix list at the local mock server so we
    can exercise the nemweb-only code path against fixtures."""
    monkeypatch.setattr(
        downloader, "_NEMWEB_HTML_PRECHECK_PREFIXES", (aemo_mock_server + "/",)
    )


def test_download_html_caches_across_calls(aemo_mock_server, monkeypatch):
    """A second call for the same URL should be served from the TTL cache
    rather than hitting the network. We instrument session.get to count
    real fetches."""
    url = f"{aemo_mock_server}/"
    real_get = downloader.session.get
    call_count = 0

    def counting_get(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return real_get(*args, **kwargs)

    monkeypatch.setattr(downloader.session, "get", counting_get)

    first = downloader.download_html(url)
    second = downloader.download_html(url)

    assert first == second
    assert call_count == 1, (
        f"download_html should cache; saw {call_count} network calls for 2 lookups"
    )


def test_pre_check_returns_none_for_non_nemweb_urls():
    """URLs outside the nemweb archive prefixes get None so callers fall
    through to a real request — protects every existing test that points
    NEMOSIS at the local mock server (which doesn't match the prefix)."""
    assert downloader._pre_check_file_is_missing("http://127.0.0.1:1234/foo.zip") is None
    assert downloader._pre_check_file_is_missing("https://www.aemo.com.au/x.xlsx") is None


def test_pre_check_returns_none_for_non_archive_extensions(_patch_precheck_prefixes, aemo_mock_server):
    """The pre-check only considers .zip and .csv files — other suffixes
    fall through (the parent listing wouldn't help us anyway)."""
    url = f"{aemo_mock_server}/some/dir/file.xlsx"
    assert downloader._pre_check_file_is_missing(url) is None


def test_pre_check_returns_false_when_file_is_listed(_patch_precheck_prefixes, aemo_mock_server):
    """File present in the parent directory's HTML index → False
    (i.e. NOT missing). The SimpleHTTPRequestHandler-generated index
    lists every entry in the directory."""
    listed = (
        f"{aemo_mock_server}/Data_Archive/Wholesale_Electricity/MMSDM/2018/"
        "MMSDM_2018_04/MMSDM_Historical_Data_SQLLoader/DATA/"
        "PUBLIC_DVD_DISPATCHLOAD_201804010000.zip"
    )
    assert downloader._pre_check_file_is_missing(listed) is False


def test_pre_check_returns_true_when_file_is_not_listed(_patch_precheck_prefixes, aemo_mock_server):
    """File absent from the parent directory's HTML index → True
    (missing). This is the speed-win path."""
    not_listed = (
        f"{aemo_mock_server}/Data_Archive/Wholesale_Electricity/MMSDM/2018/"
        "MMSDM_2018_04/MMSDM_Historical_Data_SQLLoader/DATA/"
        "PUBLIC_DVD_DOES_NOT_EXIST_201804010000.zip"
    )
    assert downloader._pre_check_file_is_missing(not_listed) is True


def test_pre_check_returns_none_when_parent_html_unreachable(_patch_precheck_prefixes, aemo_mock_server):
    """If the parent listing itself 404s, the pre-check returns None so
    the caller falls through to a real request rather than silently
    treating the file as missing on a transient infrastructure hiccup."""
    # /does/not/exist/ doesn't exist as a directory, so the parent
    # listing request will 404 and download_html_as_soup will raise.
    orphan = f"{aemo_mock_server}/does/not/exist/foo.zip"
    assert downloader._pre_check_file_is_missing(orphan) is None


def test_download_to_path_raises_http_404_when_pre_check_says_missing(
    _patch_precheck_prefixes, aemo_mock_server, tmp_path, monkeypatch
):
    """When the pre-check confirms a file is missing, download_to_path
    must raise the same HTTPError shape callers already expect from a
    real 404 — that's how the existing 404-warning paths in run() etc
    handle missing months without changing them. Counts session.get
    calls to prove the wire fetch was skipped."""
    not_listed = (
        f"{aemo_mock_server}/Data_Archive/Wholesale_Electricity/MMSDM/2018/"
        "MMSDM_2018_04/MMSDM_Historical_Data_SQLLoader/DATA/"
        "PUBLIC_DVD_DOES_NOT_EXIST_201804010000.zip"
    )
    # Prime the parent-listing cache so we can isolate the second leg.
    downloader._pre_check_file_is_missing(not_listed)

    real_get = downloader.session.get
    call_count = 0

    def counting_get(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return real_get(*args, **kwargs)

    monkeypatch.setattr(downloader.session, "get", counting_get)

    target = tmp_path / "out.zip"
    with pytest.raises(requests.HTTPError) as exc_info:
        downloader.download_to_path(not_listed, str(target))

    assert exc_info.value.response.status_code == 404
    assert call_count == 0, (
        f"pre-check should skip the wire fetch; saw {call_count} session.get calls"
    )
    assert not target.exists(), "no partial output should be left behind"


def test_download_to_path_proceeds_when_pre_check_says_present(
    _patch_precheck_prefixes, aemo_mock_server, tmp_path
):
    """A False (present) answer from the pre-check must not short-circuit
    the real download — otherwise we'd return None bytes."""
    listed = (
        f"{aemo_mock_server}/Data_Archive/Wholesale_Electricity/MMSDM/2018/"
        "MMSDM_2018_04/MMSDM_Historical_Data_SQLLoader/DATA/"
        "PUBLIC_DVD_DISPATCHLOAD_201804010000.zip"
    )
    target = tmp_path / "out.zip"
    downloaded = downloader.download_to_path(listed, str(target))

    assert downloaded is True
    assert target.is_file()
    assert target.stat().st_size > 0

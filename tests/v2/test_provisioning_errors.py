"""What a failed provision reports, and what it undoes."""

from pathlib import Path
from uuid import UUID

import httpx
import pytest
from dbos import error as dbos_error

from lclstream_api.v2.clients.fastcache import DETAIL_LIMIT, _raise_for_status
from lclstream_api.v2.core.transfer import (
    CacheMode,
    DeleteConfig,
    DeleteWorkDir,
    ProvisionProgress,
)
from lclstream_api.v2.workflows import _root_cause

CACHE_ID = UUID("87654321-4321-8765-4321-876543218765")
WORK_DIR = Path("/sdf/home/v/valmar/lclstreamer/lclstreamer_mfx1_2_abcd1234")


def _http_503(body: str = '{"detail":"No free cache ports"}') -> httpx.HTTPStatusError:
    """The error our own client raises, so the whole chain is under test."""
    request = httpx.Request("POST", "https://cache.example/api/v1/caches/")
    response = httpx.Response(503, text=body, request=request)
    try:
        _raise_for_status(response)
    except httpx.HTTPStatusError as exc:
        return exc
    raise AssertionError("expected _raise_for_status to raise")


def test_failure_detail_is_capped() -> None:
    """The message is persisted by DBOS; an HTML error page must not be."""
    exc = _http_503("x" * (DETAIL_LIMIT * 3))
    assert len(str(exc)) < DETAIL_LIMIT * 2


def test_root_cause_unwraps_the_dbos_retry_wrapper() -> None:
    """DBOS's wrapper message is a fixed 'exceeded its maximum of N retries',
    so the real reason only survives if we dig it out of .errors."""
    underlying = _http_503()
    wrapped = dbos_error.DBOSMaxStepRetriesExceeded("_create_cache", 5, [underlying])

    assert "exceeded its maximum" in str(wrapped)
    assert _root_cause(wrapped) is underlying
    assert "No free cache ports" in str(_root_cause(wrapped))


def test_root_cause_passes_through_an_ordinary_error() -> None:
    plain = LookupError("transfer vanished")
    assert _root_cause(plain) is plain


def test_config_is_deleted_before_the_directory_holding_it() -> None:
    """compensation() reverses the ledger, so recording the work dir after
    the config would delete the directory out from under it."""
    progress = (
        ProvisionProgress()
        .with_cache(CACHE_ID, mode=CacheMode.per_transfer)
        .with_work_dir(WORK_DIR)
        .with_config(WORK_DIR / "lclstreamer.yaml")
    )

    kinds = [type(step) for step in progress.compensation()]
    assert kinds.index(DeleteConfig) < kinds.index(DeleteWorkDir)


def test_relative_cache_log_path_is_rejected() -> None:
    """fastcache_api's CACHE_LOG_DIR defaults to a relative path, which would
    resolve against some other host's cwd once we hand it to IRI."""
    from lclstream_api.v2.core.transfer import CacheEndpoint

    with pytest.raises(ValueError, match="absolute"):
        CacheEndpoint.from_uris(
            CACHE_ID,
            "dtn-01",
            "tcp://dtn-01:30000",
            "tcp://dtn-01:30001",
            Path("cache-logs/x/cache.log"),
        )

"""Where the cache log lives, and who can read it.

fastcache_api runs as its own service user and writes the cache log itself,
so the path is whatever it reports back -- not something we derive under the
requester's home, which it has no permission to write.
"""

from pathlib import Path
from uuid import UUID

import pytest

from lclstream_api.v2.config import LCLStreamerProducerSettings
from lclstream_api.v2.core.logs import LogStream, log_stream_path
from lclstream_api.v2.core.producer import CacheMode, transfer_work_dir
from lclstream_api.v2.core.transfer import CacheEndpoint

TRANSFER_ID = UUID("12345678-1234-5678-1234-567812345678")
CACHE_ID = UUID("87654321-4321-8765-4321-876543218765")
# What fastcache_api reports: its own tree, not the requester's.
SERVICE_LOG = Path(f"/srv/fastcache/{CACHE_ID}/cache.log")


def _settings() -> LCLStreamerProducerSettings:
    return LCLStreamerProducerSettings(
        data_base_dir="/sdf/data/lcls/ds", home_base_dir="/sdf/home", environments={}
    )


def test_cache_endpoint_carries_the_reported_log_path() -> None:
    endpoint = CacheEndpoint.from_uris(
        CACHE_ID,
        "dtn-01",
        "tcp://dtn-01:30000",
        "tcp://dtn-01:30001",
        SERVICE_LOG,
    )
    assert endpoint.log_path == SERVICE_LOG


@pytest.mark.parametrize("cache_mode", list(CacheMode))
def test_cache_log_path_is_the_one_fastcache_reported(cache_mode: CacheMode) -> None:
    settings = _settings()
    path = log_stream_path(
        LogStream.cache,
        settings,
        "mfx101592326",
        "12",
        TRANSFER_ID,
        "valmar",
        cache_mode=cache_mode,
        cache_log_path=SERVICE_LOG,
    )
    assert path == SERVICE_LOG

    # The regression: this used to resolve under the requester's home, which
    # the fastcache_api service user cannot write.
    work_dir = transfer_work_dir(settings, "mfx101592326", "12", TRANSFER_ID, "valmar")
    assert work_dir not in path.parents


def test_producer_logs_still_live_in_the_work_dir() -> None:
    settings = _settings()
    work_dir = transfer_work_dir(settings, "mfx101592326", "12", TRANSFER_ID, "valmar")
    for stream in (LogStream.producer_stdout, LogStream.producer_stderr):
        path = log_stream_path(
            stream, settings, "mfx101592326", "12", TRANSFER_ID, "valmar"
        )
        assert path is not None
        assert path.parent == work_dir

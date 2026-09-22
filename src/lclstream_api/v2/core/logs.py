from enum import StrEnum
from pathlib import Path
from typing import Literal
from uuid import UUID

from ..config import LCLStreamerProducerSettings
from .producer import (
    PRODUCER_STDERR_FILENAME,
    PRODUCER_STDOUT_FILENAME,
    CacheMode,
    transfer_work_dir,
)


class LogStream(StrEnum):
    cache = "cache"
    producer_stdout = "producer_stdout"
    producer_stderr = "producer_stderr"


class LogReadMode(StrEnum):
    head = "head"
    tail = "tail"


_STREAM_FILENAMES: dict[LogStream, str] = {
    LogStream.producer_stdout: PRODUCER_STDOUT_FILENAME,
    LogStream.producer_stderr: PRODUCER_STDERR_FILENAME,
}


def producer_log_path(
    stream: Literal[LogStream.producer_stdout, LogStream.producer_stderr],
    settings: LCLStreamerProducerSettings,
    exp: str,
    run: str,
    transfer_id: UUID,
    username: str,
) -> Path:
    return (
        transfer_work_dir(settings, exp, run, transfer_id, username)
        / _STREAM_FILENAMES[stream]
    )


def log_stream_path(
    stream: LogStream,
    settings: LCLStreamerProducerSettings,
    exp: str,
    run: str,
    transfer_id: UUID,
    username: str,
    cache_mode: CacheMode = CacheMode.per_transfer,
    cache_log_path: Path | None = None,
) -> Path | None:
    """Dispatch for callers that handle an arbitrary log stream.

    The cache log is not ours to place: fastcache_api writes it as its own
    service user, under a tree only it owns, and reports the path back. We
    store that and hand it out. None until the cache has been provisioned.
    """
    if stream is LogStream.cache:
        return cache_log_path
    return producer_log_path(stream, settings, exp, run, transfer_id, username)

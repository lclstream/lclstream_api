from enum import StrEnum
from pathlib import Path
from typing import Literal
from uuid import UUID

from ..config import LCLStreamerProducerSettings
from .producer import (
    PRODUCER_STDERR_FILENAME,
    PRODUCER_STDOUT_FILENAME,
    CacheMode,
    shared_cache_dir,
    transfer_work_dir,
)

CACHE_LOG_FILENAME = "cache.log"


class LogStream(StrEnum):
    cache = "cache"
    producer_stdout = "producer_stdout"
    producer_stderr = "producer_stderr"


class LogReadMode(StrEnum):
    head = "head"
    tail = "tail"


_STREAM_FILENAMES: dict[LogStream, str] = {
    LogStream.cache: CACHE_LOG_FILENAME,
    LogStream.producer_stdout: PRODUCER_STDOUT_FILENAME,
    LogStream.producer_stderr: PRODUCER_STDERR_FILENAME,
}


def cache_log_path(
    settings: LCLStreamerProducerSettings,
    exp: str,
    run: str,
    transfer_id: UUID,
    cache_mode: CacheMode = CacheMode.per_transfer,
) -> Path:
    if cache_mode is CacheMode.shared:
        return shared_cache_dir(settings, exp) / CACHE_LOG_FILENAME
    return transfer_work_dir(settings, exp, run, transfer_id) / CACHE_LOG_FILENAME


def producer_log_path(
    stream: Literal[LogStream.producer_stdout, LogStream.producer_stderr],
    settings: LCLStreamerProducerSettings,
    exp: str,
    run: str,
    transfer_id: UUID,
) -> Path:
    return (
        transfer_work_dir(settings, exp, run, transfer_id) / _STREAM_FILENAMES[stream]
    )


def log_stream_path(
    stream: LogStream,
    settings: LCLStreamerProducerSettings,
    exp: str,
    run: str,
    transfer_id: UUID,
    cache_mode: CacheMode = CacheMode.per_transfer,
) -> Path:
    """Dispatch for callers that handle an arbitrary log stream."""
    if stream is LogStream.cache:
        return cache_log_path(settings, exp, run, transfer_id, cache_mode)
    return producer_log_path(stream, settings, exp, run, transfer_id)

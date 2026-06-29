from enum import StrEnum
from pathlib import Path
from uuid import UUID

from ..config import LCLStreamerProducerSettings
from .producer import (
    PRODUCER_STDERR_FILENAME,
    PRODUCER_STDOUT_FILENAME,
    transfer_work_dir,
)

CACHE_LOG_FILENAME = "cache.log"


class LogStream(StrEnum):
    cache = "cache"
    producer_stdout = "producer_stdout"
    producer_stderr = "producer_stderr"


_STREAM_FILENAMES: dict[LogStream, str] = {
    LogStream.cache: CACHE_LOG_FILENAME,
    LogStream.producer_stdout: PRODUCER_STDOUT_FILENAME,
    LogStream.producer_stderr: PRODUCER_STDERR_FILENAME,
}


def log_stream_path(
    stream: LogStream,
    settings: LCLStreamerProducerSettings,
    exp: str,
    run: str,
    transfer_id: UUID,
) -> Path:
    return (
        transfer_work_dir(settings, exp, run, transfer_id) / _STREAM_FILENAMES[stream]
    )

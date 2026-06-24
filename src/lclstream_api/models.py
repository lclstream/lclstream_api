import logging
import time
from enum import StrEnum
from uuid import UUID

from psik.models import JobState
from pydantic import BaseModel

_logger = logging.getLogger(__name__)


class TransferStatus(BaseModel):
    time: float
    jobndx: int
    state: JobState
    info: str

    id: UUID
    url: str
    user: str


class CacheMetrics(BaseModel):
    time: float
    producers: int
    recvd: int
    sent: int
    buffered: int


def empty_metric() -> CacheMetrics:
    return CacheMetrics(time=time.time(), producers=0, recvd=0, sent=0, buffered=0)


class ClientName(StrEnum):
    cache = "cache"
    producer = "producer"
    consumer = "consumer"


class PortTransition(BaseModel):
    time: float
    client: ClientName
    state: JobState
    info: str
    jobndx: int


class TransferInfo(BaseModel):
    user: str
    log: list[PortTransition]
    metrics: CacheMetrics


class PortEntry(BaseModel):
    id: UUID
    user: str
    port: int
    internal_url: str
    external_url: str

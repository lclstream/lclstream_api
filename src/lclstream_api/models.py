import logging
import time
from enum import Enum

from psik.models import JobID, JobState
from pydantic import BaseModel

_logger = logging.getLogger(__name__)


class TransferStatus(BaseModel):
    time: float
    jobndx: int
    state: JobState
    info: str

    id: JobID
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


class ClientName(str, Enum):
    cache = "cache"
    producer = "producer"
    consumer = "consumer"


class PortTransition(BaseModel):
    time: float
    client: ClientName
    state: JobState
    info: str


class TransferInfo(BaseModel):
    user: str
    log: list[PortTransition]
    metrics: CacheMetrics


class PortEntry(BaseModel):
    user: str
    port: int
    internal_url: str
    external_url: str

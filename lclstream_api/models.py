import time

from pydantic import BaseModel, Field

from psik.models import Transition, JobID, JobState

class TransferStatus(Transition):
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
    return CacheMetrics(time=time.time(),
                        producers=0, recvd=0, sent=0, buffered=0)

class PortEntry(BaseModel):
    user: str
    port: int
    internal_url: str
    external_url: str
    cache_state: JobState = JobState.new
    cache_start: float = Field(default_factory=lambda: time.time(), frozen=True)
    cache_metrics: CacheMetrics = Field(default_factory=empty_metric)

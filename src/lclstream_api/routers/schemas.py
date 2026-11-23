from pydantic import BaseModel

from ..core.enums import JobState
from ..core.models import CacheMetrics, PortTransition


class TransferStatus(BaseModel):
    time: float
    jobndx: int
    state: JobState
    info: str

    id: int
    url: str
    user: str


class TransferInfo(BaseModel):
    user: str
    log: list[PortTransition]
    metrics: CacheMetrics

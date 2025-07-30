from psik.models import Transition, JobID

class TransferStatus(Transition):
    id: JobID
    url: str
    user: str

class TransferMetrics(TransferStatus):
    messages: int
    kbytes: int
    start: float
    seconds_elapsed: float

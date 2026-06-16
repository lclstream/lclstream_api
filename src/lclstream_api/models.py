import logging
import time
from enum import Enum

_logger = logging.getLogger(__name__)

from psik import Job
from psik.models import JobID, JobState
from pydantic import BaseModel


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


class PortEntry:
    user: str
    port: int
    internal_url: str
    external_url: str

    states: dict[ClientName, JobState]
    log: list[PortTransition]
    job: Job | None

    cache_metrics: CacheMetrics

    def __init__(
        self,
        user: str,
        port: int,
        internal_url: str,
        external_url: str,
        states: dict[ClientName, JobState] = {},
        log: list[PortTransition] = [],
        cache_metrics: CacheMetrics | None = None,
        job: Job | None = None,
    ):
        self.user = user
        self.port = port
        self.internal_url = internal_url
        self.external_url = external_url
        self.states = dict(states)

        # ensure all clients are in some state.
        for name in ClientName:
            if name not in self.states:
                states[name] = JobState.new
        if len(log) == 0:  # always have a log entry to simplify life
            log = [
                PortTransition(
                    time=time.time(),
                    client=ClientName.cache,
                    state=JobState.new,
                    info="",
                )
            ]
        self.log = log
        self.job = job
        if cache_metrics is None:
            self.cache_metrics = empty_metric()

    # TODO: clear, then set timeout events to fire based on
    # each status transition
    async def transition(
        self,
        name: ClientName,
        state: JobState,
        jobndx: int = 0,
        info: str = "",
        job: Job | None = None,
    ) -> None:
        self.log.append(
            PortTransition(time=time.time(), client=name, state=state, info=info)
        )
        self.states[name] = state
        if name == ClientName.producer:
            if state == JobState.new:
                assert job is not None
                self.job = job
                # cancel job if cache is not yet alive
                if self.states[ClientName.cache].is_final():
                    _logger.error(
                        "cache is %s at %s",
                        self.states[ClientName.cache].value,
                        str(self.log[-1]),
                    )
                    await self.cancel_job()
            elif state.is_final():
                print("FIXME - terminate cache if cache has not received any messages.")
                pass

        if name == ClientName.cache and state.is_final():
            await self.cancel_job()

    async def cancel_job(self):
        """Cancel the job associated with this port.
        The cache task must be canceled separately.
        """
        if self.job is None:
            return

        name = ClientName.producer
        if self.states[name].is_final():
            # producer is already done.
            return

        # cache should not have died first.
        # we need to cancel the job now.
        await self.job.cancel()
        # TODO: check whether self.job.cancel() triggers
        # a callback to this server, or whether we should
        # append this event to the log now...
        cstate = self.states[ClientName.cache]
        self.log.append(
            PortTransition(
                time=time.time(),
                client=name,
                state=JobState.canceled,
                info=f"cache {cstate.value}",
            )
        )
        self.states[name] = JobState.canceled

    def metrics(self, metric: CacheMetrics):
        self.cache_metrics = metric

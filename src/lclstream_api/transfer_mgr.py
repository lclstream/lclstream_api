import logging
from pathlib import Path
from typing import Annotated, Optional, Awaitable, Callable, Tuple
import time

from psik import Job, JobManager

from .config import Config
from .jobs import create_producer, create_forwarder
from .lclstreamer_param import Parameters
from .models import (
    ClientName,
    JobState,
    TransferInfo,
    TransferStatus,
    PortTransition,
    CacheMetrics,
    PortEntry,
    empty_metric,
)

_logger = logging.getLogger(__name__)

class Transfer:
    """ Transfer implements a finite-state machine that tracks the
        status of a given transfer.
    """

    eid: int
    states: dict[ClientName, JobState]
    log: list[PortTransition]
    producer_job:  Job | None
    forwarder_job: Job | None

    cache_metrics: CacheMetrics

    def __init__(self, eid: int, on_complete: Optional[Callable] = None):
        self.eid = eid
        self.on_complete = on_complete
        self.states = {}

        # ensure all clients are in some state.
        for name in ClientName:
            if name not in self.states:
                self.states[name] = JobState.new
        self.log = []
        self.producer_job = None
        self.forwarder_job = None
        self.cache_metrics = empty_metric()

        # TODO: setup a timer here

    async def _cancel_producer(self, finalize=True):
        job = self.producer_job
        if job is None or self.states[ClientName.producer].is_final():
            return
        if finalize:
            self.done()

        self.states[ClientName.producer] = JobState.canceled
        await job.cancel()

    async def _cancel_forwarder(self, finalize=True):
        job = self.forwarder_job
        if job is None or self.states[ClientName.cache].is_final():
            return
        if finalize:
            self.done()

        self.states[ClientName.cache] = JobState.canceled
        await job.cancel()

    def transition(
        self,
        name: ClientName,
        state: JobState,
        jobndx: int = 0,
        info: str = "",
        job: Job | None = None,
    ) -> Optional[Callable[[], Awaitable[None]]]:
        # TODO: reset timer.
        # TODO: wire up the last timer tick to call self.cancel()
        #       so that naturally terminating jobs ensure on_complete called.

        self.log.append(
            PortTransition(time=time.time(), client=name, state=state, jobndx=jobndx, info=info)
        )
        _logger.debug("Transfer(%d) %s: %s -> %s", self.eid, name.value, self.states[name].value, state.value)
        self.states[name] = state
        if name == ClientName.producer:
            if state == JobState.new:
                assert job is not None
                self.producer_job = job
                # cancel job if cache is not yet alive
                if self.states[ClientName.cache].is_final():
                    _logger.error(
                        "Transfer(%d): cache is %s at %s",
                        self.eid,
                        self.states[ClientName.cache].value,
                        str(self.log[-1]),
                    )
                    return self._cancel_producer
            elif state.is_final():
                # "Normal" termination path.
                # This is a little excessive, since
                # the forwarder *should* shut down on its own...
                return self._cancel_forwarder

        elif name == ClientName.cache:
            if state == JobState.new:
                assert job is not None
                self.forwarder_job = job

            if state.is_final():
                if not self.states[ClientName.producer].is_final():
                    _logger.warning("Transfer(%d): Cache completed while producer is %s - canceling.", self.eid, self.states[ClientName.producer].value)
                    return self._cancel_producer

                # both finalized - normal completion path.
                _logger.info("Transfer(%d): Successful completion.", self.eid)
                self.done()

        # TODO: handle user-initiated transitions here.
        # (e.g. cancel, which currently calls cancel_job directly.)
        return None

    def done(self) -> None:
        if self.on_complete:
            self.on_complete()
            self.on_complete = None

    async def cancel_job(self):
        """Cancel the job associated with this port.
        """
        await self._cancel_producer(False)
        await self._cancel_forwarder(False)
        self.done()

    def metrics(self, metric: CacheMetrics):
        self.cache_metrics = metric

async def create_transfer(entry: PortEntry, request: Parameters, mgr: JobManager, cfg: Config, on_complete: Optional[Callable]) -> Tuple[Job|Exception, Job|Exception, Transfer|Exception]:
    """ Create the transfer.
        Does not start jobs or add to the DB!
    """
    internal_url = entry.internal_url
    external_url = entry.external_url

    # 1. Create the producer job
    producer_spec = create_producer(request, internal_url, cfg)

    #   1a. Persist the job directory to disk.
    try:
        producer_job = await mgr.create(producer_spec)
    except AssertionError as e:
        return e, Exception(), Exception()

    #   1b. Write lclstreamer spec file to the job directory.
    # The caller must ensure request has been thoroughly
    # validated before calling.
    assert producer_job.spec.directory is not None
    (Path(producer_job.spec.directory) / "lclstreamer.json").write_text(
        request.model_dump_json(indent=2)
    )

    # 2. Create the forwarder job
    forwarder_spec = create_forwarder(entry, cfg)

    #   2a. Persist the forwarder job to disk.
    try:
        forwarder_job = await mgr.create(forwarder_spec)
    except AssertionError as e:
        return producer_job, e, Exception()

    # 3. Create the formal PortEntry structure
    try:
        xfer = Transfer(entry.eid, on_complete)
    except Exception as e:
        return producer_job, forwarder_job, e

    #   3a. Record "new" transitions so xfer can cache the job-s.
    for client, job in [(ClientName.cache, forwarder_job),
                        (ClientName.producer, producer_job)]:
        jobndx = job.history[-1].jobndx
        action = xfer.transition(
            client,
            JobState.new,
            jobndx=jobndx,
            info="ok",
            job=job,
        )
        if action:
            await action()

    return producer_job, forwarder_job, xfer

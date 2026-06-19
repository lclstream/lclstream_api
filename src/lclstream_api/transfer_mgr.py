import logging
from pathlib import Path
from typing import Annotated

from psik import Job, JobManager

from .config import Config
from .jobs import create_producer, create_forwarder
from .lclstreamer_param import Parameters
from .models import (
    ClientName,
    JobID,
    JobState,
    TransferInfo,
    TransferStatus,
    PortEntry,
    empty_metric,
)
from .ports import Database

# TODO: use timers to reap completed jobs from the db
# using await db.delete(jobid)
class Transfer:
    """ Transfer implements a finite-state machine that tracks the
        status of a given transfer.
    """

    entry: PortEntry
    states: dict[ClientName, JobState]
    log: list[PortTransition]
    producer_job:  Job | None
    forwarder_job: Job | None

    cache_metrics: CacheMetrics

    def __init__(self, db: Database, entry: PortEntry):
        self.entry = entry
        self.db = db # back-ref
        self.states = {}

        # ensure all clients are in some state.
        for name in ClientName:
            if name not in self.states:
                states[name] = JobState.new
        self.log = log
        self.producer_job = None
        self.forwarder_job = None
        self.cache_metrics = empty_metric()

        # TODO: setup a timer here

    @classmethod
    async def new(self, db, stamp, user, port):
        entry = await db.create(stamp, user, port)
        return cls(db, entry)

    async def transition(
        self,
        name: ClientName,
        state: JobState,
        jobndx: int = 0,
        info: str = "",
        job: Job | None = None,
    ) -> None:
        # TODO: reset timer.

        self.log.append(
            PortTransition(time=time.time(), client=name, state=state, info=info)
        )
        self.states[name] = state
        if name == ClientName.producer:
            if state == JobState.new:
                assert job is not None
                self.producer_job = job
                # cancel job if cache is not yet alive
                if self.states[ClientName.cache].is_final():
                    _logger.error(
                        "cache is %s at %s",
                        self.states[ClientName.cache].value,
                        str(self.log[-1]),
                    )
                    await self.cancel_job()
            elif state.is_final():
                await self.cancel_job()

        elif name == ClientName.cache:
            if state == JobState.new:
                assert job is not None
                self.forwarder_job = job

            if state.is_final():
                await self.cancel_job()

        # TODO: handle user-initiated transitions here.
        # (e.g. cancel, which currently calls cancel_job directly.)

    async def cancel_job(self):
        """Cancel the job associated with this port.
        """
        if self.producer_job:
            name = ClientName.producer
            if not self.states[name].is_final():
                await self.producer_job.cancel()
            self.states[name] = JobState.canceled
            self.producer_job = None

        if self.forwarder_job:
            name = ClientName.cache
            if not self.states[name].is_final():
                await self.forwarder_job.cancel()
            self.states[name] = JobState.canceled
            self.forwarder_job = None

    def metrics(self, metric: CacheMetrics):
        self.cache_metrics = metric

async def create_transfer(db: Database, port, request: Parameters, mgr: JobManager, cfg: Config) -> Tuple[Job|Exception, Job|Exception, Transfer|Exception]:
    internal_url = db.internal_url(port)
    external_url = db.external_url(port)

    # 1. Create the producer job
    producer_spec = create_producer(request, internal_url, cfg)

    #   1a. Persist the job directory to disk.
    try:
        producer_job = await mgr.create(producer_spec)
    except AssertionError as e:
        return e, None, None

    #   1b. Write lclstreamer spec file to the job directory.
    # The caller must ensure request has been thoroughly
    # validated before calling.
    (Path(producer_job.spec.directory) / "lclstreamer.json").write_text(
        request.model_dump_json(indent=2)
    )

    # 2. Create the forwarder job
    forwarder_spec = await create_forwarder(port, internal_url, external_url, cfg)

    #   2a. Persist the forwarder job to disk.
    try:
        forwarder_job = await mgr.create(forwarder_spec)
    except AssertionError as e:
        return producer_job, e, None

    # 3. Create the formal PortEntry structure
    try:
        trs = await Transfer.new(db, producer_job.stamp, user, port)
    except Exception as e:
        return producer_job, forwarder_job, e

    #   3a. Record "new" transitions so trs can cache the job-s.
    for client, job in [(ClientName.cache, forwarder_job),
                        (ClientName.producer, producer_job)]:
        jobndx = job.history[-1].jobndx
        await trs.transition(
            client,
            JobState.new,
            jobndx=jobndx,
            info="ok",
            job=job,
        )

    return producer_job, forwarder_job, trs

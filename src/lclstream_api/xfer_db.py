"""This file implements a "Transfers" table, which essentially has
the format:

- id [foreign_key] (same key as PortEntry table)
++ foreign key to state transition log (id, xfer, **PortTransition)
++ foreign key to current state table (id, xfer, ClientName, state)
"""

import logging
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from psik import Job
from psik.models import JobID

from .models import ClientName
from .transfer_mgr import Transfer

_logger = logging.getLogger(__name__)


class XferDatabase:  # singleton
    def __init__(self) -> None:
        self.jobs: dict[UUID, Transfer] = {}

        # second table for fast indexing (on callbacks)
        self.jobids: dict[tuple[ClientName, str], UUID] = {}

    def items(self):
        return self.jobs.items()

    def lookup_job(
        self, client: ClientName, jobid: JobID
    ) -> tuple[Transfer, Job | None]:
        id = self.jobids[(client, jobid)]
        xfer = self.jobs[id]
        if client == ClientName.producer:
            job = xfer.producer_job
        else:
            job = xfer.forwarder_job
        return xfer, job

    def add(self, id: UUID, xfer: Transfer) -> None:
        if id in self.jobs:
            raise KeyError(f"{id} already exists!")
        self.jobs[id] = xfer

        # maintain index table
        if xfer.producer_job:
            self.jobids[(ClientName.producer, xfer.producer_job.stamp)] = id
        if xfer.forwarder_job:
            self.jobids[(ClientName.cache, xfer.forwarder_job.stamp)] = id

    def __getitem__(self, id: UUID) -> Transfer:
        return self.jobs[id]

    async def delete(self, id: UUID) -> Transfer:
        xfer = self.jobs.pop(id)
        # this removes callbacks
        if xfer.producer_job:
            self.jobids.pop((ClientName.producer, xfer.producer_job.stamp))
        if xfer.forwarder_job:
            self.jobids.pop((ClientName.cache, xfer.forwarder_job.stamp))
        await xfer.cancel_job()
        return xfer


DB: XferDatabase = None  # type: ignore[assignment]


def get_database() -> XferDatabase:
    # initialize on first access (allows db to be configurable)
    global DB
    if DB is None:
        DB = XferDatabase()
    return DB


Database = Annotated[XferDatabase, Depends(get_database)]

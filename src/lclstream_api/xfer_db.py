""" This file implements a "Transfers" table, which essentially has
    the format:

    - eid [foreign_key] (same key as PortEntry table)
    ++ foreign key to state transition log (id, xfer, **PortTransition)
    ++ foreign key to current state table (id, xfer, ClientName, state)
"""

import logging
from typing import Annotated, Tuple

from fastapi import Depends

from psik import Job
from psik.models import JobID

from .models import ClientName
from .transfer_mgr import Transfer

_logger = logging.getLogger(__name__)


class XferDatabase:  # singleton
    def __init__(self) -> None:
        self.jobs: dict[int, Transfer] = {}
        
        # second table for fast indexing (on callbacks)
        self.jobids: dict[Tuple[ClientName, str], int] = {}

    def items(self):
        return self.jobs.items()

    def lookup_job(self, client: ClientName, jobid: JobID) -> Tuple[Transfer, Job|None]:
        eid = self.jobids[(client, jobid)]
        xfer = self.jobs[eid]
        if client == ClientName.producer:
            job = xfer.producer_job
        else:
            job = xfer.forwarder_job
        return xfer, job

    def add(self, eid: int, xfer: Transfer) -> None:
        if eid in self.jobs:
            raise KeyError(f"{eid} already exists!")
        self.jobs[eid] = xfer

        # maintain index table
        if xfer.producer_job:
            self.jobids[(ClientName.producer, xfer.producer_job.stamp)] = eid
        if xfer.forwarder_job:
            self.jobids[(ClientName.cache, xfer.forwarder_job.stamp)] = eid

    def __getitem__(self, eid: int) -> Transfer:
        return self.jobs[eid]

    def delete(self, eid: int) -> Transfer:
        xfer = self.jobs.pop(eid)
        if xfer.producer_job:
            self.jobids.pop((ClientName.producer, xfer.producer_job.stamp))
        if xfer.forwarder_job:
            self.jobids.pop((ClientName.cache, xfer.forwarder_job.stamp))
        if xfer.on_complete:
            xfer.on_complete()
            xfer.on_complete = None
        return xfer

DB: XferDatabase = None  # type: ignore[assignment]


def get_database() -> XferDatabase:
    # initialize on first access (allows db to be configurable)
    global DB
    if DB is None:
        DB = XferDatabase()
    return DB

Database = Annotated[XferDatabase, Depends(get_database)]

"""This file implements a "Transfers" table, which essentially has
the format:

- eid [foreign_key] (same key as PortEntry table)
++ foreign key to state transition log (id, xfer, **PortTransition)
++ foreign key to current state table (id, xfer, ClientName, state)
"""

import logging
from typing import Annotated
from datetime import datetime

from fastapi import Depends
from psik import Job
from psik.models import JobID

from .models import ClientName, UserCredential, Principal
from .transfer_mgr import Transfer

_logger = logging.getLogger(__name__)


class XferDatabase:  # singleton
    def __init__(self) -> None:
        self.jobs: dict[int, Transfer] = {}
        self.credentials: dict[Principal, UserCredential] = {}

        # second table for fast indexing (on callbacks)
        self.jobids: dict[tuple[ClientName, str], int] = {}

    def items(self):
        return self.jobs.items()

    def lookup_job(
        self, client: ClientName, jobid: JobID
    ) -> tuple[Transfer, Job | None]:
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

    def get_credential(self, principal: Principal) -> UserCredential | None:
        return self.credentials.get(principal)

    def upsert_credential(self, cred: UserCredential) -> None:
        principal = Principal(issuer=cred.issuer, subject=cred.subject, email=cred.email)
        existing = self.credentials.get(principal)
        if existing is None or cred.expires_at > existing.expires_at:
            self.credentials[principal] = cred

    def purge_expired_credentials(self, now: datetime) -> int:
        expired = [p for p, c in self.credentials.items() if c.expires_at <= now]
        for p in expired:
            del self.credentials[p]
        return len(expired)

    async def delete(self, eid: int) -> Transfer:
        xfer = self.jobs.pop(eid)
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

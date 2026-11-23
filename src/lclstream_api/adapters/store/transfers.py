"""This file implements a "Transfers" table, which essentially has
the format:

- transfer_id [foreign_key] (same key as PortEntry table)
++ foreign key to state transition log (id, xfer, **PortTransition)
++ foreign key to current state table (id, xfer, ClientName, state)
"""

import logging

from psik import Job
from psik.models import JobID

from ...core.models import ClientName
from ...services.transfer import Transfer

_logger = logging.getLogger(__name__)


class XferDatabase:  # singleton
    def __init__(self) -> None:
        self.jobs: dict[int, Transfer] = {}

        # second table for fast indexing (on callbacks)
        self.jobids: dict[tuple[ClientName, str], int] = {}

    def items(self):
        return self.jobs.items()

    def lookup_job(
        self, client: ClientName, jobid: JobID
    ) -> tuple[Transfer, Job | None]:
        transfer_id = self.jobids[(client, jobid)]
        xfer = self.jobs[transfer_id]
        if client == ClientName.producer:
            job = xfer.producer_job
        else:
            job = xfer.forwarder_job
        return xfer, job

    def add(self, transfer_id: int, xfer: Transfer) -> None:
        if transfer_id in self.jobs:
            raise KeyError(f"{transfer_id} already exists!")
        self.jobs[transfer_id] = xfer

        # maintain index table
        if xfer.producer_job:
            self.jobids[(ClientName.producer, xfer.producer_job.stamp)] = transfer_id
        if xfer.forwarder_job:
            self.jobids[(ClientName.cache, xfer.forwarder_job.stamp)] = transfer_id

    def __getitem__(self, transfer_id: int) -> Transfer:
        return self.jobs[transfer_id]

    async def delete(self, transfer_id: int) -> Transfer:
        xfer = self.jobs.pop(transfer_id)
        # this removes callbacks
        if xfer.producer_job:
            self.jobids.pop((ClientName.producer, xfer.producer_job.stamp))
        if xfer.forwarder_job:
            self.jobids.pop((ClientName.cache, xfer.forwarder_job.stamp))
        await xfer.cancel_job()
        return xfer

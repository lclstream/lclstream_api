import logging

import psik
from fastapi import (
    APIRouter,
    BackgroundTasks,
    HTTPException,
)
from lclstreamer.models.parameters import Parameters
from svcs.fastapi import DepContainer

from ..auth.current_user import CurrentUser
from ..config import Config
from ..models import (
    ClientName,
    TransferInfo,
    TransferStatus,
)
from ..ports import PortDatabase
from ..transfer_mgr import Transfer, create_transfer
from ..xfer_db import XferDatabase

_logger = logging.getLogger(__name__)


async def _submit_job(job: psik.Job, xfer: Transfer, client: ClientName) -> None:
    try:
        await job.submit()
    except psik.SubmitException as e:
        _logger.error("Transfer(%d) %s submit failed: %s", xfer.eid, client.value, e)
        action = xfer.transition(client, psik.JobState.failed, info=str(e))
        if action:
            await action()
    except Exception as e:
        _logger.error(
            "Transfer(%d) %s unexpected submit error: %s", xfer.eid, client.value, e
        )
        action = xfer.transition(client, psik.JobState.failed, info=str(e))
        if action:
            await action()


transfers = APIRouter(responses={401: {"description": "Unauthorized"}})


@transfers.get("/", include_in_schema=False)
@transfers.get("")
async def list_transfers(
    services: DepContainer,
    user: CurrentUser,
    index: int = 0,
    limit: int | None = None,
    state: psik.JobState | None = None,
) -> list[TransferStatus]:
    """
    Get information about transfers owned by the authenticated user.

      - index: the index of the last transfer info to retrieve
               Items are sorted by time, so index 0 is the most recent.
      - limit: (optional) how many TransferStatus-s to retrieve
      - state: (optional) filter by job state
    """

    db, ports = await services.aget(XferDatabase, PortDatabase)

    out = []
    for eid, entry in ports.items():
        if entry.user != user:
            continue
        try:
            xfer = db[eid]
        except KeyError:
            continue

        cstate = xfer.states[ClientName.cache]
        if state is not None and state != cstate:
            continue
        last = xfer.log[-1]
        out.append(
            TransferStatus(
                id=eid,
                url=entry.external_url,
                user=entry.user,
                time=last.time,
                jobndx=last.jobndx,
                state=cstate,
                info=last.info,
            )
        )
    out.sort(key=lambda x: -float(x.id))
    if index is not None and index > 0:
        if index >= len(out):
            out = []
        else:
            out = out[index:]
    if limit is not None:
        out = out[:limit]
    return out


@transfers.post("/", include_in_schema=False)
@transfers.post("")
async def new_transfer(
    request: Parameters,
    services: DepContainer,
    bg_tasks: BackgroundTasks,
    user: CurrentUser,
) -> TransferStatus:
    """
    Submit a transfer to run ASAP.

    If successful this will return the eid created.
    """

    # 0. TODO: any additional validation of request/user goes here.
    # e.g. user can access requested dataset and has permissions to
    # start lclstreamer on psana

    db, ports, cfg, mgr = await services.aget(
        XferDatabase, PortDatabase, Config, psik.JobManager
    )

    try:
        entry = ports.create(user)
    except RuntimeError:
        _logger.error("Out of ports.")
        raise HTTPException(status_code=500, detail="Out of ports.") from None

    def on_complete():
        return ports.delete(entry.eid)

    try:
        forwarder_job, producer_job, xfer = await create_transfer(
            entry, request, mgr, cfg, on_complete
        )
    except Exception:
        on_complete()
        raise

    # all these error states mean the transfer never started
    # and jobs are either un-created or stuck at "new" state
    if isinstance(producer_job, Exception):
        on_complete()
        raise HTTPException(
            status_code=400, detail=f"Error creating producer job: {str(producer_job)}"
        )
    if producer_job.spec.directory is None:
        on_complete()
        raise HTTPException(
            status_code=500, detail="Error creating producer job directory."
        )

    if isinstance(forwarder_job, Exception):
        on_complete()
        raise HTTPException(
            status_code=400,
            detail=f"Error creating forwarder job: {str(forwarder_job)}",
        )
    if forwarder_job.spec.directory is None:
        on_complete()
        raise HTTPException(
            status_code=500, detail="Error creating forwarder job directory."
        )

    if isinstance(xfer, Exception):
        on_complete()
        raise HTTPException(
            status_code=400, detail=f"Error creating port pair: {str(xfer)}"
        )

    db.add(entry.eid, xfer)
    # Submit jobs to the queue
    bg_tasks.add_task(_submit_job, forwarder_job, xfer, ClientName.cache)
    bg_tasks.add_task(_submit_job, producer_job, xfer, ClientName.producer)

    last = xfer.log[-1]
    return TransferStatus(
        id=entry.eid,
        url=entry.external_url,
        user=entry.user,
        time=last.time,
        jobndx=last.jobndx,
        state=last.state,
        info=last.info,
    )


@transfers.get("/{id}")
async def get_transfer(
    id: int, services: DepContainer, user: CurrentUser
) -> TransferInfo:
    """Read job
    - id: The transfer ID

    Returns information associated with this transfer.
    """
    db, ports = await services.aget(XferDatabase, PortDatabase)
    try:
        xfer = db[id]
        entry = ports[id]
    except KeyError:
        raise HTTPException(status_code=404, detail="Transfer is not active.") from None
    if entry.user != user:
        raise HTTPException(status_code=403, detail="Access denied.")
    return TransferInfo(user=entry.user, log=xfer.log, metrics=xfer.cache_metrics)


@transfers.delete("/{id}")
async def cancel_transfer(
    id: int, bg_tasks: BackgroundTasks, services: DepContainer, user: CurrentUser
) -> None:
    db, ports = await services.aget(XferDatabase, PortDatabase)
    # Cancel job
    try:
        xfer = db[id]
        entry = ports[id]
    except KeyError:
        raise HTTPException(status_code=404, detail="Transfer is not active.") from None
    if entry.user != user:
        raise HTTPException(status_code=403, detail="Access denied.")
    bg_tasks.add_task(xfer.cancel_job)

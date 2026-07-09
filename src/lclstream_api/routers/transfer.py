import logging
from typing import Annotated

import psik
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
)

from ..auth import CurrentUser
from ..config import Config, load_config, to_mgr
from ..lclstreamer_param import Parameters
from ..models import (
    ClientName,
    TransferInfo,
    TransferStatus,
)
from ..ports import PortUsage
from ..transfer_mgr import create_transfer
from ..xfer_db import Database

_logger = logging.getLogger(__name__)

CachedConfig = Annotated[Config, Depends(load_config)]


def default_mgr(cfg: CachedConfig) -> psik.JobManager:
    return to_mgr(cfg)


Manager = Annotated[psik.JobManager, Depends(default_mgr)]


transfers = APIRouter(responses={401: {"description": "Unauthorized"}})


@transfers.get("/", include_in_schema=False)
@transfers.get("")
async def list_transfers(
    user: CurrentUser,
    db: Database,
    ports: PortUsage,
    index: int = 0,
    limit: int | None = None,
    state: psik.JobState | None = None,
) -> list[TransferStatus]:
    """
    Get information about transfers.

      - index: the index of the last transfer info to retrieve
               Items are sorted by time, so index 0 is the most recent.
      - limit: (optional) how many TransferStatus-s to retrieve
      - state: (optional) filter by job state
    """

    out = []
    for eid, entry in ports.items():
        try:
            xfer = db[eid]
        except KeyError:
            continue
        # TODO: allow admin-s to see all transfers
        if user != entry.user: # show only the user's transfers
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
    user: CurrentUser,
    db: Database,
    ports: PortUsage,
    bg_tasks: BackgroundTasks,
    cfg: CachedConfig,
    mgr: Manager,
    request: Parameters,
) -> TransferStatus:
    """
    Submit a transfer to run ASAP.

    If successful this will return the eid created.

    FIXME: lookup user following certified docs
    or using a FastAPI User mixin using token-auth.
    """

    # 0. TODO: any additional validation of request/user goes here.
    # e.g. user can access requested dataset and has permissions to
    # start lclstreamer on psana

    try:
        entry = ports.create(user)
    except RuntimeError:
        _logger.error("Out of ports.")
        raise HTTPException(status_code=500, detail="Out of ports.")
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
    bg_tasks.add_task(forwarder_job.submit)
    bg_tasks.add_task(producer_job.submit)

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
async def get_transfer(user: CurrentUser, ports: PortUsage, db: Database, id: int) -> TransferInfo:
    """Read job
    - id: The transfer ID

    Returns information associated with this transfer.
    """
    try:
        xfer = db[id]
        entry = ports[id]
    except KeyError:
        raise HTTPException(status_code=404, detail="Transfer is not active.")

    if entry.user != user:
        raise HTTPException(status_code=404, detail="Transfer is not active.")

    return TransferInfo(user=entry.user, log=xfer.log, metrics=xfer.cache_metrics)


@transfers.delete("/{id}")
async def cancel_transfer(user: CurrentUser, bg_tasks: BackgroundTasks, ports: PortUsage, db: Database, id: int) -> None:
    # Cancel job
    try:
        xfer  = db[id]
        entry = db[id]
    except KeyError:
        raise HTTPException(status_code=404, detail="Transfer is not active.")

    if entry.user != user:
        raise HTTPException(status_code=404, detail="Transfer is not active.")
    bg_tasks.add_task(xfer.cancel_job)

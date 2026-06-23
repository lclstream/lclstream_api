import logging
from pathlib import Path
from typing import Annotated

import psik
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
)

from ..config import Config, load_config, to_mgr
from ..jobs import create_producer, create_forwarder
from ..lclstreamer_param import Parameters
from ..models import (
    ClientName,
    JobID,
    JobState,
    TransferInfo,
    TransferStatus,
)
from ..ports import Database
from ..transfer_mgr import Transfer

_logger = logging.getLogger(__name__)

CachedConfig = Annotated[Config, Depends(load_config)]


def default_mgr(cfg: CachedConfig) -> psik.JobManager:
    return to_mgr(cfg)


Manager = Annotated[psik.JobManager, Depends(default_mgr)]


transfers = APIRouter(responses={401: {"description": "Unauthorized"}})

# Not needed, since the db stores the job.
# async def get_job(jobid: JobID, mgr: Manager) -> Path:
#    base = mgr.prefix / jobid
#    if not await base.is_dir():
#        raise HTTPException(status_code=404, detail="Transfer not found")
#    return Path(base)


@transfers.get("/", include_in_schema=False)
@transfers.get("")
async def list_transfers(
    db: Database,
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
    for jobid, entry in db.items():
        cstate = entry.states[ClientName.cache]
        if state is not None and state != cstate:
            continue
        last = entry.log[-1]
        out.append(
            TransferStatus(
                id=jobid,
                url=entry.external_url,
                user=entry.user,
                time=last.time,
                jobndx=0,
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
    db: Database,
    bg_tasks: BackgroundTasks,
    cfg: CachedConfig,
    mgr: Manager,
    user: str = "none",
) -> TransferStatus:
    """
    Submit a transfer to run ASAP.

    If successful this will return the jobid created.

    FIXME: lookup user following certified docs
    or using a FastAPI User mixin using token-auth.
    """

    # 0. TODO: any additional validation of request/user goes here.
    # e.g. user can access requested dataset and has permissions to
    # start lclstreamer on psana

    port = db.alloc()
    if port is None:
        _logger.error("Out of ports.")
        raise HTTPException(status_code=500, detail="Out of ports.")

    try:
        xfer, forwarder_job, producer_job = await create_transfer(db, port, request, mgr, cfg)
        db.add( Transfer.new() )
    except Exception:
        db.free(port)
        raise

    # all these error states mean the transfer never started
    # and jobs are either un-created or stuck at "new" state
    if isinstance(producer_job, str):
        raise HTTPException(status_code=400, detail=f"Error creating producer job: {str(e)}")
    if producer_job.spec.directory is None:
        raise HTTPException(status_code=500, detail="Error creating producer job directory.")

    if forwarder_job is None:
        raise HTTPException(status_code=400, detail=f"Error creating forwarder job: {str(e)}")
    if forwarder_job.spec.directory is None:
        raise HTTPException(status_code=500, detail="Error creating forwarder job directory.")

    if isinstance(xfer, str):
        raise HTTPException(status_code=400, detail=f"Error creating port pair: {xfer}")

    # Submit jobs to the queue
    bg_tasks.add_task(forwarder_job.submit)
    bg_tasks.add_task(producer_job.submit)

    last = xfer.log[-1]
    return TransferStatus(
        id=producer_job.stamp,
        url=external_url,
        user=xfer.user,
        time=last.time,
        jobndx=last.jobndx,
        state=last.state,
        info=last.info,
    )

@transfers.get("/{jobid}")
async def get_transfer(jobid: JobID, db: Database) -> TransferInfo:
    """Read job
    - jobid: the job's ID string

    Returns information associated with this transfer.
    """
    try:
        entry = db[jobid]
    except KeyError:
        raise HTTPException(status_code=404, detail="Transfer is not active.")
    return TransferInfo(user=entry.user, log=entry.log, metrics=entry.cache_metrics)


@transfers.delete("/{jobid}")
async def cancel_transfer(
    jobid: JobID, bg_tasks: BackgroundTasks, db: Database
) -> None:
    # Cancel job
    try:
        entry = db[jobid]
    except KeyError:
        raise HTTPException(status_code=404, detail="Transfer is not active.")
    bg_tasks.add_task(db.delete, jobid)

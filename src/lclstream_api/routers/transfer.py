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
from ..jobs import create_job
from ..lclstreamer_param import Parameters
from ..models import (
    ClientName,
    JobID,
    JobState,
    TransferInfo,
    TransferStatus,
)
from ..ports import Database

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
) -> TransferStatus:
    """
    Submit a transfer to run ASAP.

    If successful this will return the jobid created.

    FIXME: lookup user following certified docs.
    """

    user = "none"
    port = db.alloc()
    if port is None:
        _logger.error("Out of ports.")
        raise HTTPException(status_code=500, detail="Out of ports.")

    # TODO: periodically, check on jobs and reap completed jobs
    # from the db using await db.delete(jobid)

    internal_url = db.internal_url(port)
    # TODO: additional validation of request should go here.
    spec = create_job(request, internal_url, cfg)  # create the JobSpec

    try:
        job = await mgr.create(spec)
    except AssertionError as e:
        db.free(port)
        raise HTTPException(status_code=400, detail=f"Error creating job: {str(e)}")

    if job.spec.directory is None:
        db.free(port)
        raise HTTPException(status_code=500, detail="Error creating job directory.")

    # Write lclstreamer spec file to the job directory.
    # NOTE: this file must be thoroughly validated
    # before we should run based on it.
    try:
        (Path(job.spec.directory) / "lclstreamer.json").write_text(
            request.model_dump_json(indent=2)
        )

        entry = await db.create(job.stamp, user, port)
        # This new transition is necessary so entry can cache
        # the job.
        jobndx = job.history[-1].jobndx
        await entry.transition(
            ClientName.producer,
            JobState.new,
            jobndx=jobndx,
            info=str(job.history[-1].info),
            job=job,
        )
        last = entry.log[-1]
        bg_tasks.add_task(job.submit)
    except Exception as e:
        db.free(port)  # Not db.delete, since db.create
        # does not create a job entry on failure,
        # and add_task should not fail.
        raise HTTPException(status_code=400, detail=f"Error writing job: {str(e)}")
    return TransferStatus(
        id=job.stamp,
        url=entry.external_url,
        user=entry.user,
        time=last.time,
        jobndx=jobndx,
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

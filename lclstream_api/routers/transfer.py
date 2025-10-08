from typing import Optional, List
from typing_extensions import Annotated
from pathlib import Path
import logging
_logger = logging.getLogger(__name__)

from fastapi import (
    APIRouter,
    HTTPException,
    BackgroundTasks,
    Depends,
)
import psik

from ..config import to_mgr, load_config, Config
from ..models import (
    TransferStatus,
    CacheMetrics,
    JobID
)
from ..jobs import create_job
from ..ports import Database
from ..lclstreamer_param import Parameters

CachedConfig = Annotated[Config, Depends(load_config)]

def default_mgr(cfg: CachedConfig) -> psik.JobManager:
    return to_mgr(cfg)
Manager = Annotated[psik.JobManager, Depends(default_mgr)]


transfers = APIRouter(responses={
        401: {"description": "Unauthorized"}})

async def get_job(jobid: JobID, mgr: Manager) -> Path:
    base = mgr.prefix / jobid
    if not await base.is_dir():
        raise HTTPException(status_code=404, detail="Transfer not found")
    return Path(base)

@transfers.get("/", include_in_schema=False)
@transfers.get("")
async def list_transfers(mgr: Manager,
                         db: Database,
                         index: int = 0,
                         limit: Optional[int] = None,
                         state: Optional[psik.JobState] = None,
                        ) -> List[TransferStatus]:
    """
    Get information about transfers.

      - index: the index of the last transfer info to retrieve
               Items are sorted by time, so index 0 is the most recent.
      - limit: (optional) how many TransferStatus-s to retrieve
      - state: (optional) filter by job state
    """

    out = []
    #async for job in mgr.ls(): # alternate outer loop
        #try: entry = db[job.stamp] except KeyError: continue
    for jobid, entry in db.items():
        try:
            pre = await get_job(jobid, mgr)
            job = await psik.Job(pre)
        except Exception:
            continue
        last = job.history[-1]
        if last.state.is_final():
            await db.delete(jobid)

        if state is not None and state != last.state:
            continue
        out.append(TransferStatus(
                    id = job.stamp,
                    url = entry.external_url,
                    user = entry.user,
                    time = last.time,
                    jobndx = last.jobndx,
                    state = last.state,
                    info = last.info))
    out.sort(key = lambda x: -float(x.id))
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
async def new_transfer(request: Parameters,
                       db: Database,
                       bg_tasks: BackgroundTasks,
                       cfg: CachedConfig,
                       mgr: Manager) -> TransferStatus:
    """
    Submit a transfer to run ASAP.

    If successful this will return the jobid created.

    FIXME: lookup user following certified docs.
    """

    user = "none"
    port = db.alloc()
    if port is None:
        raise HTTPException(status_code=500, detail="Out of ports.")

    # TODO: periodically, check on jobs and reap completed jobs
    # from the db using await db.delete(jobid)

    internal_url = db.internal_url(port)
    # TODO: additional validation of request should go here.
    spec = create_job(request, internal_url, cfg) # create the JobSpec

    try:
        job = await mgr.create(spec)
    except AssertionError as e:
        db.free(port)
        raise HTTPException(status_code=400,
                            detail=f"Error creating job: {str(e)}")

    if job.spec.directory is None:
        db.free(port)
        raise HTTPException(status_code=500, detail="Error creating job directory.")

    # Write lclstreamer spec file to the job directory.
    # NOTE: this file must be thoroughly validated
    # before we should run based on it.
    try:
        (Path(job.spec.directory)/"lclstreamer.json").write_text(
            request.model_dump_json(indent=2)
        )

        last = job.history[-1]
        entry = await db.create(job.stamp, user, port)
        bg_tasks.add_task(job.submit)
    except Exception as e:
        db.free(port) # Not db.delete, since db.create
                      # does not create a job entry on failure,
                      # and add_task should not fail.
        raise HTTPException(status_code=400,
                            detail=f"Error writing job: {str(e)}")
    return TransferStatus(
                    id = job.stamp,
                    url = entry.external_url,
                    user = entry.user,
                    time = last.time,
                    jobndx = last.jobndx,
                    state = last.state,
                    info = last.info)

@transfers.get('/{jobid}')
async def get_transfer(jobid: JobID,
                       db: Database,
                       mgr: Manager) -> List[TransferStatus]:
    """Read job
      - jobid: the job's ID string
    """
    pre = await get_job(jobid, mgr)
    try:
        job = await psik.Job(pre)
    except Exception:
        raise HTTPException(status_code=500, detail="Error reading job")
    try:
        entry = db[jobid]
    except KeyError:
        raise HTTPException(status_code=404, detail="Transfer is not active.")

    out = []
    for last in job.history:
        out.append(TransferStatus(
                    id = job.stamp,
                    url = entry.external_url,
                    user = entry.user,
                    time = last.time,
                    jobndx = last.jobndx,
                    state = last.state,
                    info = last.info))
    return out

@transfers.delete('/{jobid}')
async def cancel_transfer(jobid: JobID,
                          bg_tasks: BackgroundTasks,
                          db: Database,
                          mgr: Manager) -> None:
    # Cancel job
    pre = await get_job(jobid, mgr)
    try:
        job = await psik.Job(pre)
    except Exception:
        raise HTTPException(status_code=500, detail="Error reading job")
    bg_tasks.add_task(job.cancel)
    bg_tasks.add_task(db.delete, jobid)

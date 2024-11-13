from typing import Optional, List, Dict
from typing_extensions import Annotated
from pathlib import Path
import logging
_logger = logging.getLogger(__name__)

from pydantic import BaseModel, Field
from fastapi import (
    APIRouter,
    HTTPException,
    Form,
    Query,
    File,
    BackgroundTasks,
)
import psik

from lclstream.models import DataRequest, AccessMode

from ..config import Manager, CachedConfig

TransferStats = Tuple[int,float,float,float]

transfers = APIRouter(responses={
        401: {"description": "Unauthorized"}})

stamp_re = re.compile(r"[0-9]+\.[0-9]+")

async def get_job(jobid: str, mgr: Manager) -> Path:
    if not stamp_re.match(jobid):
        raise HTTPException(status_code=400, detail="Invalid jobid")
    base = mgr.prefix / jobid
    if not await base.is_dir():
        raise HTTPException(status_code=404, detail="Transfer not found")
    return Path(base)

@transfers.get("")
@transfers.get("/")
async def get_transfers(mgr: Manager,
                   index: int = 0,
                   limit: Optional[int] = None,
                   state: Optional[psik.JobState] = None,
                  ) -> List[JobStepInfo]:
    """
    Get information about transfers.

      - index: the index of the last transfer info to retrieve
               Items are sorted by time, so index 0 is the most recent.
      - limit: (optional) how many JobStepInfo-s to retrieve
      - backend: (optional) the compute resource name
      - state: (optional) filter by job state
    """

    out = []
    async for job in mgr.ls():
        t, ndx, jstate, info = job.history[-1]
        if state is not None and jstate != state:
            continue
        out.append(JobStepInfo(
                    jobid = job.stamp,
                    name = job.spec.name or '',
                    updated = t,
                    jobndx = ndx,
                    state = jstate,
                    info = info))
    out.sort(key = lambda x: -float(x.jobid))
    if index is not None and index > 0:
        if index >= len(out):
            out = []
        else:
            out = out[index:]
    if limit is not None:
        out = out[:limit]
    return out

@transfers.post('')
@transfers.post('/')
async def new_transfer(request: DataRequest,
                       bg_tasks: BackgroundTasks,
                       mgr: Manager) -> str:
    """
    Submit a transfer to run ASAP.

    If successful this api will return the jobid created.
    """
    spec = req_to_jobspec(request)
    try:
        job = await mgr.create(spec)
    except AssertionError as e:
        raise HTTPException(status_code=400,
                            detail=f"Error creating job: {str(e)}")
    bg_tasks.add_task(job.submit)
    #try:
    #    await job.submit()
    #except psik.SubmitException as e:
    #    raise HTTPException(status_code=400,
    #                        detail=f"Error submitting job: {str(e)}")
    return job.stamp

@transfers.get('/{jobid}')
async def get_transfer(jobid: str,
                       mgr: Manager) -> List[JobStepInfo]:
    """Read job
      - jobid: the job's ID string
      - backend: (optional) the job's backend
    """
    pre = await get_job(jobid, mgr)
    try:
        job = await psik.Job(pre)
    except Exception:
        raise HTTPException(status_code=500, detail="Error reading job")

    out = []
    for t, ndx, state, info in job.history:
        out.append(JobStepInfo(
                    jobid = job.stamp,
                    name = job.spec.name or '',
                    updated = t,
                    jobndx = ndx,
                    state = state,
                    info = info))
    return out

@transfers.delete('/{jobid}')
async def cancel_transfer(jobid: str,
                          bg_tasks: BackgroundTasks,
                          mgr: Manager) -> None:
    # Cancel job
    pre = await get_job(jobid, backend)
    try:
        job = await psik.Job(pre)
    except Exception:
        raise HTTPException(status_code=500, detail="Error reading job")
    bg_tasks.add_task(job.cancel)
    return

psana1 = """
source /sdf/group/lcls/ds/ana/sw/conda1/manage/bin/psconda.sh
PREFIX=/sdf/home/r/rogersdd/venvs/psana_local

echo "Running psana_push on $(hostname)"
mpirun -n 64 --map-by ppr:32:node $PREFIX/bin/psana_push
             -e {req.exp}
             -r {req.run}
             -d {req.detector_name}
             -m {req.mode.value}
             -a {req.addr}
             -c {req.access_mode.value}

echo "Completed psana_push on $(hostname)"
"""

psana2 = """
source /sdf/home/r/rogersdd/src/tmo-prefex/env.sh

time mpirun psana2h5 '{req.exp}' '{detectors}' '{req.run}' \
                   --config '{req.config}' \
                   --outdir '{outdir}'

echo 'Completed processing of {req.exp}:{req.run}'
"""
# to add after initial testing:
#                  --dial '{dial}'

def get_outdir(req: DataRequest, cfg: CachedConfig) -> Path:
    """ Compute the output directory name for this
    experiment / req.config pair.
    """
    cfg_hash = req.config.hash(8)
    return Path(cfg.cache_fmt%req.exp) / cfg_hash

def has_cache(req: DataRequest,
              cfg: CachedConfig) -> Optional[Path]:
    """ Return directory+filename prefix containing
    cached h5 files created for this request.

    If available, the h5 files are (return value)*.h5.
    If no cached result is available, None is returned.
    """
    outdir = get_outdir(req, cfg)
    if not outdir.is_dir():
        return None

    prefix = f"{req.exp}.run_{req.run:03d}"
    for child in outdir.iterdir():
        #$expname.run_NNN.step_MM[-rank].JJJ.h5
        if child.name.startswith(prefix) \
                    and child.name.endswith(".h5"):
            return outdir/prefix
    return None

def req_to_jobspec(req: DataRequest,
                   cfg: CachedConfig) -> psik.JobSpec:
    """Create the psik.JobSpec that, when run,
    will either transfer cached h5 data or run psana2h5
    to generate data on-demand.
    """
    pre = has_cache(req, cfg)
    if pre is None:
        return psana2h5_spec(req, cfg)

    script = """
    source /sdf/home/r/rogersdd/src/tmo-prefex/env.sh
    push_h5 --dial {req.dial} {pre}*.h5
    """.format(req=req, pre=pre)
    return psik.JobSpec(
            name="push_h5",
            script=script,
            resources=psik.ResourceSpec(
                duration=60,
                node_count=1,
                processes_per_node=1,
                cpu_cores_per_process=1,
            ),
            #callback="",
            #cb_secret="",
    )

def psana2h5_spec(req: DataRequest,
                  cfg: CachedConfig) -> psik.JobSpec:
    """Create the psik.JobSpec that, when run,
    will fill this request for psana2h5 data.
    """
    assert req.access_mode in [AccessMode.idx, AccessMode.smd], \
            "Access mode should be one of: idx, smd"
     
    #if req.access_mode == AccessMode.idx:
    #    cmd0 = ["psana_push"]
    #script = psana1.format(req=req)
    script = psana2.format(req=req, outdir=get_outdir(req, cfg))
    return psik.JobSpec(
            name="psana2h5",
            script=script,
            resources=psik.ResourceSpec(
                duration=60,
                node_count=1,
                processes_per_node=120,
                cpu_cores_per_process=1,
            ),
            #callback="",
            #cb_secret="",
    )

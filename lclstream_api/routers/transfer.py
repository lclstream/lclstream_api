from typing import Optional, List, Dict
from typing_extensions import Annotated
from pathlib import Path
import socket
import logging
_logger = logging.getLogger(__name__)

from pydantic import BaseModel
from fastapi import (
    APIRouter,
    HTTPException,
    Form,
    Query,
    File,
    BackgroundTasks,
    Depends,
)
import psik

from lclstreamer.models import Parameters

from ..config import to_mgr, load_config, Config
from ..models import TransferStatus, TransferMetrics, JobID

def default_config():
    return load_config()
CachedConfig = Annotated[Config, Depends(default_config)]

def default_mgr(config: CachedConfig):
    return to_mgr(config)
Manager = Annotated[psik.Manager, Depends(default_mgr)]

TransferStats = Tuple[int,float,float,float]

transfers = APIRouter(responses={
        401: {"description": "Unauthorized"}})

async def get_job(jobid: JobID, mgr: Manager) -> Path:
    base = mgr.prefix / jobid
    if not await base.is_dir():
        raise HTTPException(status_code=404, detail="Transfer not found")
    return Path(base)

class PortEntry(BaseModel):
    user: str
    port: int
    internal_url: str
    external_url: str

class PortDatabase: # singleton
    def __init__(self, host: Optional[str] = None, start=30001, end=34000) -> None:
        assert end > start+1, "Need at least 2 ports"
        if host is None:
            self.host = socket.gethostname()
        else:
            self.host = host
        self.open_ports = list(range(start, end, 2))
        # Mapping from jobid to user, port pairs.
        self.jobs : Dict[str, PortEntry] = {}

    def alloc(self) -> Optional[int]:
        """ Allocate a port -- usually called automagically
            during create().
        """
        if len(self.open_ports) == 0:
            _logger.error("No more open ports!")
            return None
        return self.open_ports.pop()

    def free(self, port):
        self.open_ports.append(port)

    def internal_url(self, port: int) -> str:
        # Internal ports are first in sequence
        return f"tcp://{self.host}:{port}"
    def external_url(self, port: int) -> str:
        # External ports are internal+1
        return f"tcp://{self.host}:{port+1}"

    def create(self,
               jobid: str,
               user: str,
               port: Optional[int] = None) -> PortEntry:
        if jobid in self.jobs:
            entry = self.jobs[jobid]
            # Make create idempotent
            if entry.user == user:
                return entry
            raise KeyError(f"Job {jobid} already created by another user!")
        if port is None:
            port = self.alloc()
        if port is None:
            raise RuntimeError("No available ports.")

        entry = PortEntry(
            user = user,
            port = port,
            internal_url = self.internal_url(port),
            external_url = self.external_url(port),
        )
        self.jobs[jobid] = entry
        return entry

    def __getitem__(self, jobid: str) -> PortEntry:
        return self.jobs[jobid]

    def delete(self, jobid: str) -> PortEntry:
        entry = self.jobs.pop(jobid)
        self.free(entry.port)
        return entry

DB = PortDatabase()
def get_database():
    return DB

Database = Annotated[PortDatabase, Depends(get_database)]

@transfers.get("/", include_in_schema=False)
@transfers.get("")
async def get_transfers(mgr: Manager,
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
    async for job in mgr.ls():
        last = job.history[-1]
        if state is not None and state != last.state:
            continue
        try:
            entry = db[job.stamp]
        except KeyError:
            continue
        out.append(TransferStatus(
                    id = job.stamp,
                    name = job.spec.name or '',
                    url = entry.external_url,
                    user = entry.user,
                    updated = last.time,
                    jobndx = last.jobndx,
                    state = last.state,
                    info = last.info))
    out.sort(key = lambda x: -float(x.jobid))
    if index is not None and index > 0:
        if index >= len(out):
            out = []
        else:
            out = out[index:]
    if limit is not None:
        out = out[:limit]
    return out

def replace_data_handler(req: Parameters, url: str) -> None:
    # Replace data handlers entirely to avoid the user outputting
    # somewhere unanticipated by LCLStream-API.
    req.data_handlers = DataHandlerParameters(
        urls: [ url ],
        role: "client",
        library: "nng",
        socket_type: "push",
    )

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

    # TODO: periodically, check on jobs and reap completed jobs
    # from the db using db.delete(jobid)

    internal_url = db.internal_url(port)
    # TODO: additional validation of request should go here.
    replace_data_handler(request, internal_url)

    pre = has_cache(request, cfg)
    if pre is None:
        spec = generate_job(request, internal_url, cfg)
    else:
        spec = replay_job(pre, internal_url, cfg)

    try:
        job = await mgr.create(spec)
    except AssertionError as e:
        db.free(port)
        raise HTTPException(status_code=400,
                            detail=f"Error creating job: {str(e)}")

    # Write lclstreamer spec file to the job directory.
    # NOTE: this file must be thoroughly validated
    # before we should run based on it.
    try:
        (Path(job.spec.directory)/"lclstreamer.json").write_text(
            json.dumps(request, indent=2)
        )

        last = job.history[-1]
        bg_tasks.add_task(job.submit)
        entry = db.create(job.stamp, user, port)
    except Exception as e:
        db.free(port)
        raise HTTPException(status_code=400,
                            detail=f"Error writing job: {str(e)}")
    return TransferStatus(
                    id = job.stamp,
                    name = job.spec.name or '',
                    url = entry.external_url,
                    user = entry.user,
                    updated = last.time,
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
                    name = job.spec.name or '',
                    url = entry.external_url,
                    user = entry.user,
                    updated = last.time,
                    jobndx = last.jobndx,
                    state = last.state,
                    info = last.info))
    return out

@transfers.delete('/{jobid}')
async def cancel_transfer(jobid: JobID,
                          bg_tasks: BackgroundTasks,
                          mgr: Manager) -> None:
    # Cancel job
    pre = await get_job(jobid, mgr)
    try:
        job = await psik.Job(pre)
    except Exception:
        raise HTTPException(status_code=500, detail="Error reading job")
    bg_tasks.add_task(job.cancel)
    return


############# helpers for writing job payload ##################

def get_outdir(req: Parameters, cfg: CachedConfig) -> Path:
    """ Compute the output directory name for this
    experiment / req.config pair.
    """
    # TODO: back-port hash function from tmo-prefex
    cfg_hash = str(hash(json.dumps(req)))
    expt = "tmo_unknown"
    # TODO: check for exact equality of cache_path / lclstreamer.json
    # and search through a sequence of dir-s if not...
    return Path(cfg.cache_fmt % expt) / cfg_hash

def has_cache(req: Parameters,
              cfg: CachedConfig) -> Optional[Path]:
    """ Return directory+filename prefix containing
    cached h5 files created for this request.

    If available, the h5 files are (return value)*.h5.
    If no cached result is available, None is returned.
    """
    # FIXME: for testing, just replay this data.
    return "/sdf/home/r/rogersdd/lclstreamer-output/r0"
    return None

    """ FIXME: revisit server-side caching.
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
    """

def replay_job(pre: str,
               url: str,
               cfg: CachedConfig) -> psik.JobSpec:
    """ Create the psik.JobSpec that, when run,
        will transfer cached h5 data to the url.
    """

    local_push = """
    lclstream push --addr {url} --ndial 1 {pre}*.h5
    """.format(url=url, pre=pre)
    return psik.JobSpec(
                name = "lclstream-push",
                script = local_push,
                resources = psik.ResourceSpec(
                    duration = 60,
                    node_count = 1,
                    processes_per_node = 1,
                    cpu_cores_per_process = 1,
                ),
                #callback="",
                #cb_secret="",
    )

def generate_job(req: Parameters,
                 url: str,
                 cfg: CachedConfig) -> psik.JobSpec:
    """ Create the psik.JobSpec that, when run,
        will run an lclstreamer job sending streaming
        output to the url.
    """

    # Lookup the proper env for the requested event source.
    if req.lclstreamer.event_source == "Psana1EventSource"
        psana_env = "psana1"
    else:
        psana_env = "psana2"

    # Prepare the job's working directory
    template = """
    pixi run -e {psana_env} mpirun -n64 lclstreamer \
                       --config lclstreamer.json
    """
    # to add after initial testing:
    #                  --dial '{dial}'

    script = template.format(req=req)#, outdir=get_outdir(req, cfg))
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

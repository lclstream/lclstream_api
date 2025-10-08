# Helpers for creating job payload.

from typing import Optional
from pathlib import Path

import psik

from .config import Config
from .lclstreamer_param import (
    Parameters,
    DataHandlerParameters,
    BinaryDataStreamingDataHandlerParameters,
)

def replace_data_handler(req: Parameters, url: str) -> None:
    # Replace data handlers entirely to avoid the user outputting
    # somewhere unanticipated by LCLStream-API.
    req.data_handlers = DataHandlerParameters(
        BinaryDataStreamingDataHandler =
          BinaryDataStreamingDataHandlerParameters(
            urls = [ url ],
            role = "client",
            library = "nng",
            socket_type = "push",
          )
    )

def create_job(request: Parameters,
               internal_url: str,
               cfg: Config) -> psik.JobSpec:
    replace_data_handler(request, internal_url)

    pre = has_cache(request, cfg)
    if pre is None:
        spec = generate_job(request, internal_url, cfg)
    else:
        spec = replay_job(pre, internal_url, cfg)
    return spec

def get_outdir(req: Parameters, cfg: Config) -> Path:
    """ Compute the output directory name for this
    experiment / req.config pair.
    """
    # TODO: back-port hash function from tmo-prefex
    cfg_hash = str(hash(req.model_dump_json()))
    expt = "tmo_unknown"
    # TODO: check for exact equality of cache_path / lclstreamer.json
    # and search through a sequence of dir-s if not...
    return Path(cfg.cache_fmt % expt) / cfg_hash

def has_cache(req: Parameters,
              cfg: Config) -> Optional[Path]:
    """ Return directory+filename prefix containing
    cached h5 files created for this request.

    If available, the h5 files are (return value)*.h5.
    If no cached result is available, None is returned.
    """
    # FIXME: for testing, just replay this data.
    #return "/sdf/home/r/rogersdd/lclstreamer-output/r0"
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

def replay_job(pre: Path,
               url: str,
               cfg: Config) -> psik.JobSpec:
    """ Create the psik.JobSpec that, when run,
        will transfer cached h5 data to the url.
    """
    job = cfg.replay_job.model_copy()
    job.script = job.script.format(url=url, pre=pre)
    return job

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
                 cfg: Config) -> psik.JobSpec:
    """ Create the psik.JobSpec that, when run,
        will run an lclstreamer job sending streaming
        output to the url.
    """

    # Lookup the proper env for the requested event source.
    if req.lclstreamer.event_source == "Psana1EventSource":
        psana_env = "psana1"
    else:
        psana_env = "psana2"

    job = cfg.lclstream_job.model_copy()
    job.script = job.script.format(url=url, psana_env=psana_env)
    #job.callback = 
    #job.cb_secret = 
    return job

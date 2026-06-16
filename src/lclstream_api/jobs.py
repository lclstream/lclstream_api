# Helpers for creating job payload.

from typing import Optional
from pathlib import Path
from secrets import token_urlsafe

from pydantic import SecretStr

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
    req.data_handlers = [
        BinaryDataStreamingDataHandlerParameters(
            type="BinaryDataStreamingDataHandler",
            urls=[url],
            role="client",
            library="nng",
            socket_type="push",
        )
    ]


# note: we could also use domain (FastAPI.Request's req.base_url)
#       to construct the callback URL...
def create_job(request: Parameters, internal_url: str, cfg: Config) -> psik.JobSpec:
    replace_data_handler(request, internal_url)

    pre = has_cache(request, cfg)
    if pre is None:
        spec = generate_job(request, internal_url, cfg)
    else:
        spec = replay_job(pre, internal_url, cfg)

    spec.callback = cfg.callback_url
    # psik 1.2.0 is broken (doesn't write the cb_secret), but
    # if we run with certified, it's possible to check if
    # the calling username is lclstream_api itself.
    #
    # spec.cb_secret = SecretStr(token_urlsafe(32))
    # spec.cb_secret = token_urlsafe(32) # or change its type, so it will write?
    return spec


def get_outdir(req: Parameters, cfg: Config) -> Optional[Path]:
    """Compute the output directory name for this
    experiment / req.config pair.
    """
    if cfg.cache_fmt is None:
        return None
    # TODO: back-port hash function from tmo-prefex
    cfg_hash = str(hash(req.model_dump_json()))
    expt = "tmo_unknown"
    # TODO: check for exact equality of cache_path / lclstreamer.json
    # and search through a sequence of dir-s if not...
    return Path(cfg.cache_fmt % expt) / cfg_hash


def has_cache(req: Parameters, cfg: Config) -> Optional[Path]:
    """Return directory+filename prefix containing
    cached h5 files created for this request.

    If available, the h5 files are (return value)*.h5.
    If no cached result is available, None is returned.
    """
    outdir = get_outdir(req, cfg)
    # check that outdir exists and contains h5 files first

    if outdir is None or not outdir.is_dir():
        return None

    for child in outdir.iterdir():
        if child.name.endswith(".h5"):
            return outdir
    return None


def replay_job(pre: Path, url: str, cfg: Config) -> psik.JobSpec:
    """Create the psik.JobSpec that, when run,
    will transfer cached h5 data to the url.
    """
    job = cfg.replay_job.model_copy()
    job.script = job.script.format(url=url, pre=pre)
    return job


def generate_job(req: Parameters, url: str, cfg: Config) -> psik.JobSpec:
    """Create the psik.JobSpec that, when run,
    will run an lclstreamer job sending streaming
    output to the url.
    """

    # Lookup the proper env for the requested event source.
    if req.event_source.type == "Psana1EventSource":
        psana_env = "psana1"
    else:
        psana_env = "psana2"

    job = cfg.lclstream_job.model_copy()
    job.script = job.script.format(url=url, psana_env=psana_env)
    return job

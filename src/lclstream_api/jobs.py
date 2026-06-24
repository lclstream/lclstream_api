# Helpers for creating job payload.

from pathlib import Path
from secrets import token_urlsafe

from pydantic import SecretStr

import psik

from .config import Config
from .models import PortEntry
from .lclstreamer_param import (
    BinaryDataStreamingDataHandlerParameters,
    Parameters,
)


def replace_data_handler(req: Parameters, url: str) -> None:
    # Replace data handlers entirely to avoid the user outputting
    # somewhere unanticipated by LCLStream-API.
    req.data_handlers = [
        BinaryDataStreamingDataHandlerParameters(
            type="BinaryDataStreamingDataHandler",
            urls=[url],
            role="client",
            library="zmq",
            socket_type="push",
        )
    ]


# note: we could also use domain (FastAPI.Request's req.base_url)
#       to construct the callback URL...
def create_producer(request: Parameters, internal_url: str, cfg: Config) -> psik.JobSpec:
    replace_data_handler(request, internal_url)

    pre = has_cache(request, cfg)
    if pre is None:
        spec = generate_job(request, internal_url, cfg)
    else:
        spec = replay_job(pre, internal_url, cfg)

    if cfg.callback_url:
        spec.callback = cfg.callback_url + "/producer"
        spec.cb_secret = SecretStr(token_urlsafe(32))
    return spec

def create_forwarder(entry: PortEntry, cfg: Config) -> psik.JobSpec:
    """Create the message forwarder jobspec for launching with psik.
    """
    spec = cfg.forwarder.jobspec.model_copy()
    spec.script = spec.script.format(internal_url = entry.internal_url,
                                     external_url = entry.external_url,
                                     port = entry.port)

    if cfg.callback_url:
        spec.callback = cfg.callback_url + "/forwarder"
        spec.cb_secret = SecretStr(token_urlsafe(32))
    return spec

def get_outdir(req: Parameters, cfg: Config) -> Path | None:
    """Compute the output directory name for this
       experiment / req.config pair.
    """
    if cfg.replay.cache_fmt is None:
        return None
    # TODO: back-port hash function from tmo-prefex
    cfg_hash = str(hash(req.model_dump_json()))
    expt = "tmo_unknown"
    # TODO: check for exact equality of cache_path / lclstreamer.json
    # and search through a sequence of dir-s if not...
    return Path(cfg.replay.cache_fmt % expt) / cfg_hash


def has_cache(req: Parameters, cfg: Config) -> Path | None:
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
    job = cfg.replay.jobspec.model_copy()
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

    job = cfg.lclstreamer.jobspec.model_copy()
    job.script = job.script.format(url=url, psana_env=psana_env)
    return job

import os
from functools import cache
from pathlib import Path
from typing import Union, Optional

import yaml

import psik
from pydantic import BaseModel

class LCLStreamerConfig(BaseModel):
    jobspec: psik.JobSpec

class ForwarderConfig(BaseModel):
    ip: str
    start_port: int = 30001
    end_port: int = 34000
    jobspec: psik.JobSpec

class ReplayConfig(BaseModel):
    cache_fmt: Optional[str] = None
    jobspec: psik.JobSpec = psik.JobSpec(script="")

class Config(BaseModel):
    psik: psik.Config
    callback_url: str | None  # no default since None breaks callback functionality

    forwarder: ForwarderConfig
    replay: ReplayConfig = ReplayConfig()
    lclstreamer: LCLStreamerConfig

# Other config options we could add...
#
# database_url: str = "sqlite+pysqlite:///:memory:"
# cache_fmt: str = "/sdf/scratch/lcls/ds/tmo/%s/scratch/lclstream_api"
# authz: str = "psik_api.authz:BaseAuthz"
# script="/home/99r/.cache/pypoetry/virtualenvs/lclstream-wj83ZDDz-py3.10/bin/lclstream push --addr {url} --ndial 1 {pre}*.h5",
# script="pixi run -e {psana_env} mpirun -n120 lclstreamer --config lclstreamer.json",
# resources = psik.ResourceSpec(duration=60,
#            node_count=1,
#            processes_per_node = 120,
#            cpu_cores_per_process = 1),
# script="uv run --project /home/99r/src/lclstreamer lclstreamer --config lclstreamer.json",

Pstr = Union[str, os.PathLike]


@cache
def load_config(config_name: Pstr | None = None) -> Config:
    """Load lclstream_api's configuration file.

    Priority order is:
      1. config_name (if not None)
      2. $LCLSTREAM_API_CONFIG (if defined)
      3. $VIRTUAL_ENV/etc/lclstream_api.yaml (if VIRTUAL_ENV defined)
      4. /etc/lclstream_api.yaml

    Args:
      config_name: if defined, the configuration is read from this file

    Raises:
      FileNotFoundError: If the file does not exist.
      IsADirectoryError: Path does not point to a file.
      PermissionError:   If the file cannot be read.
    """
    cfg_name = "lclstream_api.yaml"
    if config_name is not None:
        path = Path(config_name)
    elif "LCLSTREAM_API_CONFIG" in os.environ:
        path = Path(os.environ["LCLSTREAM_API_CONFIG"])
    else:
        path = Path(os.environ.get("VIRTUAL_ENV", "/")) / "etc" / cfg_name
    cfg = yaml.safe_load( path.read_text(encoding="utf-8") )
    return Config.model_validate(cfg)


def to_mgr(cfg: Config) -> psik.JobManager:
    cfg.psik.prefix.mkdir(exist_ok=True, parents=True)
    return psik.JobManager(cfg.psik)

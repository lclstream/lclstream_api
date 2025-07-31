from typing import Union, Optional
import os
#from functools import cache
from pathlib import Path

from pydantic import BaseModel
import psik

class Config(BaseModel):
    database_url: str = "sqlite+pysqlite:///:memory:"
    #cache_fmt: str = "/sdf/scratch/lcls/ds/tmo/%s/scratch/lclstream_api"
    #authz: str = "psik_api.authz:BaseAuthz"
    psik: psik.Config

Pstr = Union[str, os.PathLike]

def load_config(config_name: Optional[Pstr] = None) -> Config:
    """Load lclstream_api's configuration file.

    Priority order is:
      1. config_name (if not None)
      2. $LCLSTREAM_API_CONFIG (if defined)
      3. $VIRTUAL_ENV/etc/lclstream_api.json (if VIRTUAL_ENV defined)
      4. /etc/lclstream_api.json

    Args:
      config_name: if defined, the configuration is read from this file

    Raises:
      FileNotFoundError: If the file does not exist.
      IsADirectoryError: Path does not point to a file.
      PermissionError:   If the file cannot be read.
    """
    cfg_name = "lclstream_api.json"
    if config_name is not None:
        path = Path(config_name)
    elif "LCLSTREAM_API_CONFIG" in os.environ:
        path = Path(os.environ["LCLSTREAM_API_CONFIG"])
    else:
        path = Path(os.environ.get("VIRTUAL_ENV", "/")) / "etc" / cfg_name
    cfg = path.read_text(encoding='utf-8')
    return Config.model_validate_json(cfg)

def to_mgr(cfg: psik.Config) -> psik.JobManager:
    cfg.psik.prefix.mkdir(exist_ok=True, parents=True)
    return psik.JobManager(cfg.psik)

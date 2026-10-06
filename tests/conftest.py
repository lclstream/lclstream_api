import os
import shutil
from pathlib import Path

import pytest
import yaml

from lclstream_api.config import Config

_zmqbuf = os.environ.get("ZMQBUF_PATH") or shutil.which("zmqbuf") or "zmqbuf"

# this config only works if lclstreamer is setup...
cfg_yaml = """
psik:
  prefix: "%(base)s/psik"

callback_url: null
forwarder:
  ip: "127.0.0.1"
  start_port: 11401
  end_port: 11420
  jobspec:
    name: "zmqbuf"
    backend: "default"
    script: "%(zmqbuf)s"

replay:
  cache_fmt: "%(base)s/lclstream_cache/%%s"
  jobspec:
    name: "lclstream-push"
    backend: "default"
    resources:
      duration: 60
      node_count: 1
      processes_per_node: 1
      cpu_cores_per_process: 1
    script: |
      lclstream push --addr {url} --ndial 1 {pre}*.h5

lclstreamer:
  jobspec:
    name: "lclstreamer"
    backend: "default"
    resources:
      duration: 60
      node_count: 1
      processes_per_node: 1
      cpu_cores_per_process: 1
    script: "lclstreamer --config lclstreamer.json"
"""

@pytest.fixture
def config(tmpdir) -> Config:
    x = yaml.safe_load(cfg_yaml % {"base": str(tmpdir), "zmqbuf": _zmqbuf})
    return Config.model_validate(x)


@pytest.fixture
def setup_lclstream_api(config, tmp_path) -> Path:
    fname = tmp_path / "lclstream_api.json"
    fname.write_text(config.model_dump_json())
    os.environ["LCLSTREAM_API_CONFIG"] = str(fname)
    return fname

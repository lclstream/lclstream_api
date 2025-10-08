from pathlib import Path
import os

import pytest

from lclstream_api.config import Config

cfg_json = """{
  "cache_fmt": "%(base)s/lclstream_cache/%%s",
  "psik": {
    "prefix": "%(base)s/psik"
  },
  "replay_job": {
    "name": "lclstream-push",
    "backend": "default",
    "resources": {
      "duration": 60,
      "node_count": 1,
      "processes_per_node": 1,
      "cpu_cores_per_process": 1
    },
    "script": "lclstream push --addr {url} --ndial 1 {pre}*.h5"
  },
  "lclstream_job": {
    "name": "lclstreamer",
    "backend": "default",
    "resources": {
      "duration": 60,
      "node_count": 1,
      "processes_per_node": 1,
      "cpu_cores_per_process": 1
    },
    "script": "lclstreamer --config lclstreamer.json"
  }
}
"""

@pytest.fixture
def config(tmpdir) -> Config:
    return Config.model_validate_json(cfg_json%{"base": str(tmpdir)})

@pytest.fixture
def setup_lclstream_api(config, tmp_path) -> Path:
    fname = tmp_path/"lclstream_api.json"
    fname.write_text(config.model_dump_json())
    os.environ["LCLSTREAM_API_CONFIG"] = str(fname)
    return fname

def test_config(config):
    assert isinstance(config, Config)

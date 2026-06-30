import os
from pathlib import Path

import pytest
import yaml

from lclstream_api.config import Config

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
    script: "/home/99r/src/microservices/nng_stream/zmqbuf"

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

oidc:
    issuer_url: https://dex.slac.stanford.edu
    jwks_uri: https://dex.slac.stanford.edu/keys
    audiencs: s3df
    # Verified emails allowed to use the service (allowlist; all members see all).
    expected_users: "user1@slac.stanford.edu,user2@slac.stanford.edu"
"""

# this config uses lclstream (client) to mimick lclstreamer
# in order to make a self-contained package testable
cfg_yaml2 = """
callback_url: null

psik:
  prefix: "%(base)s/psik"

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
    script: "lclstream push --addr {url} --ndial 1 {pre}*.h5"

forwarder:
  run_cache: "zmqbuf"
  cache_ip: "127.0.0.1"
  start_port: 11401
  end_port: 11420

lclstreamer:
  jobspec:
    name: "lclstreamer"
    backend: "default"
    resources:
      duration: 60
      node_count: 1
      processes_per_node: 1
      cpu_cores_per_process: 1
    script: "lclstream push --addr {url} --ndial 1 *.*"

oidc:
    issuer_url: https://dex.slac.stanford.edu
    jwks_uri: https://dex.slac.stanford.edu/keys
    audiencs: s3df
    # Verified emails allowed to use the service (allowlist; all members see all).
    expected_users: "user1@slac.stanford.edu,user2@slac.stanford.edu"
"""


@pytest.fixture
def config(tmpdir) -> Config:
    x = yaml.safe_load(cfg_yaml % {"base": str(tmpdir)})
    return Config.model_validate(x)


@pytest.fixture
def setup_lclstream_api(config, tmp_path) -> Path:
    fname = tmp_path / "lclstream_api.json"
    fname.write_text(config.model_dump_json())
    os.environ["LCLSTREAM_API_CONFIG"] = str(fname)
    return fname


def test_config(config):
    assert isinstance(config, Config)

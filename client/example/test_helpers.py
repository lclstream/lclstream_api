#!/usr/bin/env python3
"""Check the parsers and the per-job path naming in submit_crystfel_nersc."""

import base64
import json
from datetime import UTC, datetime

from submit_crystfel_nersc import (
    JOB_ID_PATTERN,
    Settings,
    last_log_line,
    log_file,
    token_expiry,
    token_file,
)

NERSC = (
    "{'content': '491623670\\t./adse13_160\\n', 'content_type': 'lines', "
    "'start_position': 1, 'end_position': 2}"
)

assert last_log_line(NERSC) == "491623670\t./adse13_160"
def fake_jwt(exp: int) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode()
    return f"header.{payload.rstrip('=')}.signature"


assert token_expiry(fake_jwt(1_700_000_000)) == datetime.fromtimestamp(
    1_700_000_000, UTC
)
assert token_expiry("not a jwt") is None
assert token_expiry("header.bm90IGpzb24.sig") is None

# Concurrent runs of one exp/run must not share a log or a token: they used
# to, and the first job to exit deleted the other's credential.
cfg = Settings()
assert log_file(cfg, "mfxl1001", "12", "111") != log_file(cfg, "mfxl1001", "12", "222")
assert token_file(cfg, "mfxl1001", "12", "aaa") != token_file(
    cfg, "mfxl1001", "12", "bbb"
)
# Slurm expands %j itself, so the submitted path is not the readable one.
assert log_file(cfg, "mfxl1001", "12", JOB_ID_PATTERN).name.endswith(".%j.log")
assert log_file(cfg, "mfxl1001", "12", "58753172").name == (
    "crystfel.mfxl1001.12.58753172.log"
)

print("ok")

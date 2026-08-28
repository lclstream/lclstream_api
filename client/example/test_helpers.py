#!/usr/bin/env python3
"""Check the two parsers in submit_crystfel_nersc."""

import base64
import json
from datetime import UTC, datetime

from submit_crystfel_nersc import last_log_line, token_expiry

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

print("ok")

"""Dump the v2 app's OpenAPI document, used as input to the client generator."""

import json
import os
from pathlib import Path

# Must run before importing lclstream_api.v2.config (built eagerly at import).
# Mirrors tests/v2/conftest.py's _DUMMY_ENV — these settings have no dev
# defaults (mTLS cert paths), but app.openapi() never reads their contents.
_DUMMY_ENV = {
    "LCLSTREAM_FASTCACHE_TOKEN_FILE": "/dev/null",
    "LCLSTREAM_FASTCACHE_CLIENT_CERT": "/dev/null",
    "LCLSTREAM_FASTCACHE_CLIENT_KEY": "/dev/null",
    "LCLSTREAM_IRI_S3DF_TOKEN_FILE": "/dev/null",
}
for _key, _value in _DUMMY_ENV.items():
    os.environ.setdefault(_key, _value)

from lclstream_api.v2.app import app  # noqa: E402

REPO_ROOT = Path(__file__).parent.parent
OUTPUT = REPO_ROOT / "openapi.json"


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

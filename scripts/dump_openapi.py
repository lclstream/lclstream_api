"""Dump the v2 app's OpenAPI document, used as input to the client generator."""

import json
from pathlib import Path

from lclstream_api.v2.app import app

REPO_ROOT = Path(__file__).parent.parent
OUTPUT = REPO_ROOT / "openapi.json"


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

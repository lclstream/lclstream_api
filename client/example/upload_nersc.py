#!/usr/bin/env python3
"""Copy a local file to NERSC through IRI as psdatmgr.

Same filesystem API submit_crystfel_nersc.py stages the token with, so the
5 MB IRI upload limit applies.

    uv run upload_nersc.py jf16mgeom.geom
    uv run upload_nersc.py jf16mgeom.geom --force       # replace an existing file
    uv run upload_nersc.py jf16mgeom.geom --dest /global/cfs/cdirs/lcls/crystfel-demo
"""

import argparse
from pathlib import Path

from amscrot.facility import FacilityClient
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from sfapi_client import Client as SfapiClient


class Settings(BaseSettings):
    # Shares submit_crystfel_nersc.py's prefix, so one .env configures both.
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_NERSC_", env_file=".env", extra="ignore"
    )

    iri_url: AnyHttpUrl = AnyHttpUrl("https://api.iri.nersc.gov")
    # One file, client id on line 1 and PEM below. A bare name resolves
    # from ~/.superfacility.
    sfapi_key: Path = Path("psdatmgr")
    fs_resource: str = "cfs"
    # psdatmgr's lclstream clone.
    upload_dir: Path = Path("/global/homes/p/psdatmgr/repos/github/lclstream")


# IRI rejects larger uploads; fail before reading the file.
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


def open_facility(cfg: Settings) -> FacilityClient:
    """SFAPI tokens are short-lived, so hand IRI a refreshable provider."""
    # Left open: closing it kills token refresh.
    sfapi = SfapiClient(key=cfg.sfapi_key)
    return FacilityClient(
        endpoint=str(cfg.iri_url),
        token=sfapi.token,
        token_provider=lambda: sfapi.token,
        name="nersc",
    )


def exists(fs, path: str) -> bool:
    try:
        _ = fs.stat(path).result
    except Exception:
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path, help="local file to copy")
    parser.add_argument(
        "--dest", type=Path, help="remote directory; overrides LCLSTREAM_NERSC_UPLOAD_DIR"
    )
    parser.add_argument(
        "--force", action="store_true", help="replace the remote file if it exists"
    )
    args = parser.parse_args()

    local = args.file.expanduser()
    if not local.is_file():
        parser.error(f"not a file: {local}")
    size = local.stat().st_size
    if size > MAX_UPLOAD_BYTES:
        parser.error(f"{local} is {size} bytes; IRI uploads stop at 5 MB")

    cfg = Settings()  # type: ignore[call-arg]
    remote_dir = args.dest or cfg.upload_dir
    remote = str(remote_dir / local.name)

    fs = open_facility(cfg).resource(cfg.fs_resource).fs
    # The destination may be a git clone; don't clobber tracked files silently.
    if not args.force and exists(fs, remote):
        raise SystemExit(f"{remote} exists; pass --force to replace it")
    _ = fs.mkdir(str(remote_dir), parents=True).result
    _ = fs.upload(str(local), remote).result
    print(f"copied {local} to {remote} ({size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

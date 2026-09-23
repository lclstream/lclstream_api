#!/usr/bin/env python3
"""Submit crystfel_nersc.sh to NERSC through IRI as psdatmgr.

Runs at SLAC; this is what the GUI launches. The job runs CrystFEL on a
Perlmutter node against the S3DF cache, then xrdcp's the stream back.

    uv run submit_crystfel_nersc.py --exp mfx101592326 --run 12
    uv run submit_crystfel_nersc.py --exp ... --run ... \
        --crystfel-args "--peaks=msgpack --indexing=mosflm -j 8"
    uv run submit_crystfel_nersc.py --exp ... --run ... \
        --xrd-dest /streams/mfx101592326
    uv run submit_crystfel_nersc.py --exp ... --run ... --status <job_id>
    uv run submit_crystfel_nersc.py --resources         # list resource names
"""

import argparse
import ast
import base64
import contextlib
import json
import shlex
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from amscrot.facility import FacilityClient
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from sfapi_client import Client as SfapiClient


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_NERSC_", env_file=".env", extra="ignore"
    )

    iri_url: AnyHttpUrl = AnyHttpUrl("https://api.iri.nersc.gov")

    # Written by scripts/s3df-login; uploaded for the job to use.
    s3df_token_file: Path = Path("~/.s3df-access-token").expanduser()

    # psdatmgr's SFAPI credential mints the bearer token: one file,
    # client id on line 1 and PEM below. A bare name resolves from
    # ~/.superfacility.
    sfapi_key: Path = Path("psdatmgr")

    # Perlmutter's compute resource is named "compute".
    compute_resource: str = "compute"
    # Logs live on CFS; it stays up when compute is down.
    fs_resource: str = "cfs"

    # Read straight from psdatmgr's clone, so there is no staged copy.
    script: Path = Path(
        "/global/homes/p/psdatmgr/repos/github/lclstream"
        "/lclstream_api/client/example/crystfel_nersc.sh"
    )
    account: str = "lcls"
    queue: str = "regular"
    duration: int = 1800
    # Job cwd on NERSC: .env, geometry, .secrets, logs.
    directory: Path = Path("/global/cfs/cdirs/lcls/crystfel-demo")
    # Where the job xrdcp's the stream; empty keeps it on scratch.
    xrd_dest: str = ""


def source_id(exp: str, run: str) -> str:
    """lclstream_api parses exp and run back out of this string."""
    return f"exp={exp},run={run}"


# Slurm expands this in stdout/stderr paths, so the log carries the job id
# even though we only learn it after submitting.
JOB_ID_PATTERN = "%j"


def log_file(cfg: Settings, exp: str, run: str, job_id: str) -> Path:
    """Keyed by job: two runs of one exp/run would otherwise share a log,
    and the second to finish would overwrite the first."""
    return cfg.directory / f"crystfel.{exp}.{run}.{job_id}.log"


def token_file(cfg: Settings, exp: str, run: str, submission_id: str) -> Path:
    """Keyed per submission rather than per job, because the job id does not
    exist until after submit and the token must be readable before the job
    starts. The job deletes it; a shared name let one run delete another's."""
    return cfg.directory / ".secrets" / f"s3df_token.{exp}.{run}.{submission_id}"


# lclstream_api wants producer duration plus its grace.
TOKEN_FLOOR_S = 4500


def token_expiry(token: str) -> datetime | None:
    """Read exp from the JWT payload without verifying it."""
    with contextlib.suppress(IndexError, ValueError, KeyError):
        payload = base64.urlsafe_b64decode(token.split(".")[1] + "==")
        return datetime.fromtimestamp(json.loads(payload)["exp"], UTC)
    return None


def stage_token(
    cfg: Settings, facility: FacilityClient, exp: str, run: str, submission_id: str
) -> Path:
    """Upload the S3DF token the job needs to create its transfer.

    Keyed per submission: a shared path would let a queued job read a later
    submitter's credential and charge the transfer to them, and let whichever
    run finished first delete it out from under the others.
    """
    try:
        token = cfg.s3df_token_file.read_text().strip()
    except FileNotFoundError:
        raise SystemExit(
            f"No token at {cfg.s3df_token_file}; run ./scripts/s3df-login"
        ) from None
    expiry = token_expiry(token)
    if expiry is not None:
        left = int((expiry - datetime.now(UTC)).total_seconds()) // 60
        print(f"s3df token: {left} min left, expires {expiry:%Y-%m-%d %H:%M} UTC")
        if left * 60 < TOKEN_FLOOR_S:
            print("warning: the job needs 75 minutes of token life")

    remote = token_file(cfg, exp, run, submission_id)
    fs = facility.resource(cfg.fs_resource).fs
    _ = fs.mkdir(str(remote.parent), parents=True).result
    _ = fs.upload_bytes(token.encode(), str(remote)).result
    _ = fs.chmod(str(remote), "600").result
    print(f"staged token at {remote}")
    return remote


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


def submit(
    cfg: Settings,
    facility: FacilityClient,
    exp: str,
    run: str,
    crystfel_args: list[str],
    xrd_dest: str,
) -> None:
    submission_id = uuid4().hex[:8]
    token = stage_token(cfg, facility, exp, run, submission_id)
    log = log_file(cfg, exp, run, JOB_ID_PATTERN)
    job = facility.resource(cfg.compute_resource).submit(
        executable="/bin/bash",
        # The job body exports this as the source identifier.
        # Anything after the token replaces indexamajig's defaults.
        arguments=[str(cfg.script), source_id(exp, run), str(token), *crystfel_args],
        directory=str(cfg.directory),
        name=f"crystfel-{exp}-{run}",
        queue=cfg.queue,
        account=cfg.account,
        duration=cfg.duration,
        nodes=1,
        # Unset leaves the job's .env value in charge.
        environment={"XRD_DEST": xrd_dest} if xrd_dest else None,
        # One merged log; --status tails it.
        stdout_path=str(log),
        stderr_path=str(log),
    )
    print(f"submitted job {job.id}")
    print(f"log {log_file(cfg, exp, run, job.id)}")
    print(
        "status: uv run submit_crystfel_nersc.py "
        f"--exp {exp} --run {run} --status {job.id}"
    )


def last_log_line(raw: str) -> str:
    """NERSC hands back the tail payload as a stringified dict."""
    return str(ast.literal_eval(raw)["content"]).rstrip()


def show_status(
    cfg: Settings, facility: FacilityClient, job_id: str, exp: str, run: str
) -> None:
    job = facility.resource(cfg.compute_resource).job(job_id)
    print(f"state: {job.refresh(historical=True)}")
    log = str(log_file(cfg, exp, run, job_id))
    try:
        tail = facility.resource(cfg.fs_resource).fs.tail(log, lines=1)
        print(f"log: {last_log_line(tail.result)}")
    except Exception:
        # Queued jobs have written nothing yet.
        print(f"log: not readable yet ({log})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exp", help="experiment, e.g. mfx101592326")
    parser.add_argument("--run", help="run number")
    parser.add_argument("--status", metavar="JOB_ID", help="print state and log tail")
    parser.add_argument(
        "--resources", action="store_true", help="list IRI resource names"
    )
    parser.add_argument(
        "--crystfel-args",
        default="",
        help="indexamajig argument string; replaces the script defaults",
    )
    parser.add_argument(
        "--xrd-dest",
        help="xrootd path for the stream",
    )
    parser.add_argument("rest", nargs="*", help="extra indexamajig arguments, after --")
    args = parser.parse_args()

    cfg = Settings()  # type: ignore[call-arg]
    facility = open_facility(cfg)
    if args.resources:
        for resource in facility.resources():
            print(resource)
        return 0
    if not (args.exp and args.run):
        parser.error("--exp and --run are required")
    if args.status:
        show_status(cfg, facility, args.status, args.exp, args.run)
    else:
        submit(
            cfg,
            facility,
            args.exp,
            args.run,
            [*shlex.split(args.crystfel_args), *args.rest],
            args.xrd_dest or cfg.xrd_dest,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "httpx>=0.27",
#     "pydantic-settings>=2.6.0",
#     "remote-cmd",
# ]
#
# [tool.uv.sources]
# remote-cmd = { path = "/sdf/home/v/valmar/Projects/LCLStream/remote-cmd" }
# ///
"""Submit crystfel_lux.sh to OLCF Lux through remote-cmd.

Runs at SLAC; this is what the GUI launches. Lux has no IRI API, so every
step goes through a remote-cmd server reached over an SSH-tunnelled socket:
the token is uploaded with POST /files, and sbatch/squeue/sacct/tail run
through POST /shell. The job runs CrystFEL on one Lux node against the
S3DF cache, then xrdcp's the stream back.

    uv run submit_crystfel_lux.py --exp mfx101592326 --run 12
    uv run submit_crystfel_lux.py --exp ... --run ... \
        --crystfel-args "--peaks=msgpack --indexing=mosflm -j 100"
    uv run submit_crystfel_lux.py --exp ... --run ... \
        --xrd-dest /streams/mfx101592326
    uv run submit_crystfel_lux.py --exp ... --run ... --status <job_id>
    uv run submit_crystfel_lux.py --exp ... --run ... --cancel <job_id>
"""

import argparse
import base64
import contextlib
import json
import re
import shlex
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
from pydantic_settings import BaseSettings, SettingsConfigDict
from remote_cmd.client import get_client


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_LUX_", env_file=".env", extra="ignore"
    )

    # A bare name resolves to /run/user/<uid>/<name>.sock, the local end
    # of the SSH tunnel to the remote-cmd server on a Lux login node.
    remote: str = "local"

    # Written by scripts/s3df-login; uploaded for the job to use.
    s3df_token_file: Path = Path("~/.s3df-access-token").expanduser()

    script: Path = Path(
        "/ccs/home/vmarslac/lclstream_api/client/example/crystfel_lux.sh"
    )
    account: str = "CSC741"
    partition: str = "batch"
    # Lux rejects --cpus-per-task and hands out cores only with GPU slices,
    # and how many per slice varies by node. All 8 GPUs plus --exclusive is
    # the one shape that reliably gets the full 128 cores for indexamajig.
    gpus: int = 8
    duration: int = 1800
    # Job cwd on Lux: .env, geometry, .secrets, logs. remote-cmd only
    # writes under paths listed in Lux's ~/.config/remote_cmd.json.
    directory: Path = Path("/ccs/home/vmarslac/crystfel_demo")
    # Where the job xrdcp's the stream; empty keeps it on Lux.
    xrd_dest: str = ""


class RemoteError(RuntimeError):
    pass


class Remote:
    """The two remote-cmd calls we need, returning output instead of
    printing it and exiting like remote_cmd.client does."""

    def __init__(self, url: str):
        self.client = get_client(url)

    def run(self, cmd: str) -> tuple[int, str]:
        """Run cmd through the remote shell: (exit code, merged output)."""
        lines: list[str] = []
        exit_code = 1
        is_exit_event = False
        with self.client.stream("POST", "/shell", json=cmd, timeout=60.0) as response:
            if response.status_code != 200:
                raise RemoteError(f"shell {response.status_code}: {response.read()!r}")
            for line in response.iter_lines():
                if line.startswith("event: exit"):
                    is_exit_event = True
                elif line.startswith("data: "):
                    content = line[6:]
                    if is_exit_event:
                        with contextlib.suppress(json.JSONDecodeError):
                            exit_code = json.loads(content).get("exit_code", 1)
                        is_exit_event = False
                    else:
                        lines.append(content)
        return exit_code, "\n".join(lines)

    def check(self, cmd: str) -> str:
        """run(), raising on a non-zero exit."""
        code, out = self.run(cmd)
        if code != 0:
            raise RemoteError(f"`{cmd}` exited {code}: {out}")
        return out

    def upload_bytes(self, data: bytes, remote_path: Path) -> None:
        response = self.client.post(
            "/files",
            params={"remote_path": str(remote_path)},
            files={"file": (remote_path.name, data)},
        )
        if response.status_code != 200:
            raise RemoteError(f"upload {response.status_code}: {response.text}")


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
    cfg: Settings, remote: Remote, exp: str, run: str, submission_id: str
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

    path = token_file(cfg, exp, run, submission_id)
    # Create the dir private first so the token is never readable by others.
    remote.check(f"mkdir -p -m 700 {shlex.quote(str(path.parent))}")
    remote.upload_bytes(token.encode(), path)
    remote.check(f"chmod 600 {shlex.quote(str(path))}")
    print(f"staged token at {path}")
    return path


def submit(
    cfg: Settings,
    remote: Remote,
    exp: str,
    run: str,
    crystfel_args: list[str],
    xrd_dest: str,
) -> None:
    submission_id = uuid4().hex[:8]
    token = stage_token(cfg, remote, exp, run, submission_id)
    log = log_file(cfg, exp, run, JOB_ID_PATTERN)
    # Unset leaves the job's .env value in charge.
    export = f"ALL,XRD_DEST={xrd_dest}" if xrd_dest else "ALL"
    # TEMPORARY srun HACK: Lux's job_submit plugin adds SLURM_GET_USER_ENV=1
    # to every sbatch job, so the compute node runs `su - vmarslac` to load
    # the login environment. The account's login shell is /bin/zsh, which
    # compute nodes don't have, so every sbatch job is requeued and held
    # (user_env_retrieval_failed_requeued_held, e.g. job 395284). srun skips
    # that step. Once OLCF switches the shell to bash (requested through
    # help@olcf.ornl.gov), delete the srun block below, uncomment the
    # sbatch lines, and remove wait_for_srun_job().
    # Caveat: the srun process lives on the Lux login node for the whole
    # job; if that node reboots or kills it, the job dies with it.
    #
    # sbatch = [
    #     "sbatch",
    #     "--parsable",
    #     f"--account={cfg.account}",
    #     f"--partition={cfg.partition}",
    #     f"--job-name=crystfel-{exp}-{run}",
    #     "--nodes=1",
    #     "--ntasks=1",
    #     f"--gpus={cfg.gpus}",
    #     "--exclusive",
    #     f"--time={max(1, cfg.duration // 60)}",
    #     f"--chdir={cfg.directory}",
    #     f"--export={export}",
    #     # One merged log; --status tails it.
    #     f"--output={log}",
    #     f"--error={log}",
    #     str(cfg.script),
    #     # The job body exports this as the source identifier.
    #     # Anything after the token replaces indexamajig's defaults.
    #     source_id(exp, run),
    #     str(token),
    #     *crystfel_args,
    # ]
    # try:
    #     out = remote.check(shlex.join(sbatch))
    # except RemoteError:
    #     # Nothing will run to delete it.
    #     remote.run(f"rm -f {shlex.quote(str(token))}")
    #     raise
    # # --parsable prints "<id>" or "<id>;<cluster>".
    # job_id = out.strip().splitlines()[-1].split(";")[0]
    # srun prints no job id, so the name carries the submission id and
    # squeue maps it back.
    job_name = f"crystfel-{exp}-{run}-{submission_id}"
    srun = [
        "srun",
        f"--account={cfg.account}",
        f"--partition={cfg.partition}",
        f"--job-name={job_name}",
        "--nodes=1",
        "--ntasks=1",
        f"--gpus={cfg.gpus}",
        "--exclusive",
        f"--time={max(1, cfg.duration // 60)}",
        f"--chdir={cfg.directory}",
        f"--export={export}",
        # One merged log; --status tails it.
        f"--output={log}",
        f"--error={log}",
        str(cfg.script),
        # The job body exports this as the source identifier.
        # Anything after the token replaces indexamajig's defaults.
        source_id(exp, run),
        str(token),
        *crystfel_args,
    ]
    # srun's own messages (allocation errors) go here, not to the job log.
    launch_log = cfg.directory / f".srun.{exp}.{run}.{submission_id}.out"
    pid = ""
    try:
        # Detached and fully redirected, or the remote shell would hold
        # the stream open until the job ends.
        pid = remote.check(
            f"nohup {shlex.join(srun)} </dev/null "
            f">{shlex.quote(str(launch_log))} 2>&1 & echo $!"
        ).strip()
        job_id = wait_for_srun_job(remote, job_name, pid, launch_log)
    except RemoteError:
        # Nothing will run to delete it, and a straggling srun would start
        # a job with no token.
        if pid:
            remote.run(f"kill {shlex.quote(pid)} 2>/dev/null")
        remote.run(f"rm -f {shlex.quote(str(token))}")
        raise
    print(f"submitted job {job_id}")
    print(f"log {log_file(cfg, exp, run, job_id)}")
    print(
        "status: uv run submit_crystfel_lux.py "
        f"--exp {exp} --run {run} --status {job_id}"
    )

    watch_job(cfg, remote, job_id, exp, run)


SRUN_START_TIMEOUT_S = 60


def wait_for_srun_job(remote: Remote, job_name: str, pid: str, launch_log: Path) -> str:
    """Part of the temporary srun hack in submit(): find the id of the job
    srun just requested, failing fast if srun exited without one."""
    name = shlex.quote(job_name)
    deadline = time.time() + SRUN_START_TIMEOUT_S
    while time.time() < deadline:
        _, out = remote.run(
            f"squeue -h -u $USER -n {name} -o %i | head -n 1; "
            f"kill -0 {shlex.quote(pid)} 2>/dev/null && echo alive"
        )
        lines = out.split()
        if lines and lines[0].isdigit():
            return lines[0]
        if "alive" not in lines:
            # Gone from squeue and not running: srun was refused, or the
            # job already ended. sacct still knows about the latter.
            _, done = remote.run(f"sacct -n -X -P -u $USER --name={name} -o JobID")
            if done.strip():
                return done.split()[0]
            _, why = remote.run(f"cat {shlex.quote(str(launch_log))}")
            raise RemoteError(f"srun exited without a job: {why}")
        time.sleep(2)
    raise RemoteError(f"no job named {job_name} after {SRUN_START_TIMEOUT_S}s")


def poll_job_status(
    cfg: Settings, remote: Remote, job_id: str, exp: str, run: str
) -> tuple[str, str]:
    """One status check in one round trip: (state, log tail line).

    squeue knows live jobs; once a job leaves the queue only sacct does.
    """
    log = shlex.quote(str(log_file(cfg, exp, run, job_id)))
    job = shlex.quote(job_id)
    cmd = (
        f"s=$(squeue -h -j {job} -o %T 2>/dev/null); "
        f'[ -n "$s" ] || s=$(sacct -n -X -P -j {job} -o State 2>/dev/null | head -n 1); '
        'echo "STATE=${s:-UNKNOWN}"; '
        f"tail -n 1 {log} 2>/dev/null || echo 'not readable yet ({log})'"
    )
    _, out = remote.run(cmd)
    state_line, _, log_line = out.partition("\n")
    # sacct says e.g. "CANCELLED by 12345".
    state = state_line.removeprefix("STATE=").split()[0] if state_line else "UNKNOWN"
    return state, log_line.rstrip()


def show_status(cfg: Settings, remote: Remote, job_id: str, exp: str, run: str) -> None:
    state, line = poll_job_status(cfg, remote, job_id, exp, run)
    print(f"state: {state}")
    print(f"log: {line}")


def staged_token_of(cfg: Settings, remote: Remote, job_id: str) -> Path | None:
    """The token path sbatch was handed, read back from Slurm's record of
    the submit line: it is keyed per submission, not by job id."""
    out = remote.check(f"sacct -n -X -P -j {shlex.quote(job_id)} -o SubmitLine")
    argv = shlex.split(out.strip().splitlines()[0]) if out.strip() else []
    with contextlib.suppress(ValueError, IndexError):
        # The token follows the script and the source identifier.
        path = Path(argv[argv.index(str(cfg.script)) + 2])
        # Never delete anything outside our own secrets dir.
        if path.parent == cfg.directory / ".secrets":
            return path
    return None


def cancel(cfg: Settings, remote: Remote, job_id: str) -> None:
    """scancel, then delete the staged token. A job that dies before its
    body starts (held, requeued, cancelled while pending) never runs its
    own cleanup, so the token would otherwise stay on disk."""
    token = staged_token_of(cfg, remote, job_id)
    remote.check(f"scancel {shlex.quote(job_id)}")
    print(f"cancelled job {job_id}")
    if token is None:
        print("no staged token found for this job")
        return
    remote.check(f"rm -f {shlex.quote(str(token))}")
    print(f"removed token {token}")


# indexamajig prints progress like:
#   "1000 images processed, 345 hits (34.5%), 112.3 images/sec, ..."
PROGRESS_PATTERN = re.compile(
    r"(?P<frames>\d+)\s+images processed,\s*(?P<hits>\d+)\s+hits"
)

POLL_INTERVAL_S = 10
OUTPUT_LOG = Path("crystfel.out")
STATUS_FILE = Path("status.txt")

TERMINAL_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "TIMEOUT",
}


def format_elapsed(seconds: float) -> str:
    total = int(seconds)
    hrs, rem = divmod(total, 3600)
    mins, secs = divmod(rem, 60)
    return f"{hrs}hr {mins:02d}min {secs:02d}sec"


def write_status_file(
    state: str, log_line: str, start: float, now: float, frames: str, hits: str
) -> None:
    """Mirror the Cheetah status.txt format."""
    content = (
        "# CrystFEL status\n"
        f"Update time: {datetime.fromtimestamp(now).strftime('%a %b %d %H:%M:%S %Y')}\n"
        f"Elapsed time: {format_elapsed(now - start)}\n"
        f"Status: {state}\n"
        f"Frames processed: {frames}\n"
        f"Number of hits: {hits}\n"
    )
    STATUS_FILE.write_text(content)


def watch_job(cfg: Settings, remote: Remote, job_id: str, exp: str, run: str) -> None:
    """Poll the job every 10s, logging to crystfel.out and refreshing
    status.txt, until Slurm reports a terminal state."""
    start_wall = time.time()
    last_frames: str | None = None
    last_hits: str | None = None

    # Seed status.txt immediately so a reader never finds it missing.
    write_status_file("SUBMITTED", "", start_wall, start_wall, "0", "0")

    with OUTPUT_LOG.open("a") as out:
        while True:
            now_wall = time.time()
            try:
                state, log_line = poll_job_status(cfg, remote, job_id, exp, run)
            except Exception as exc:  # keep polling through tunnel hiccups
                state, log_line = "UNKNOWN", f"status check failed: {exc}"

            timestamp = datetime.now(UTC).isoformat(timespec="seconds")
            out_line = f"{timestamp} state={state} log={log_line}"
            print(out_line)
            out.write(out_line + "\n")
            out.flush()

            match = PROGRESS_PATTERN.search(log_line)
            if match:
                last_frames, last_hits = match.group("frames"), match.group("hits")

            finished = state in TERMINAL_STATES

            # Skip mid-run writes until frames/hits are known, but always
            # write the final state so status.txt reflects the exit.
            if finished or (last_frames is not None and last_hits is not None):
                frames = last_frames if last_frames is not None else "0"
                hits = last_hits if last_hits is not None else "0"
                write_status_file(state, log_line, start_wall, now_wall, frames, hits)

            if finished:
                break

            time.sleep(POLL_INTERVAL_S)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exp", required=True, help="experiment, e.g. mfx101592326")
    parser.add_argument("--run", required=True, help="run number")
    parser.add_argument("--status", metavar="JOB_ID", help="print state and log tail")
    parser.add_argument(
        "--cancel", metavar="JOB_ID", help="scancel and delete the staged token"
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
    try:
        remote = Remote(cfg.remote)
    except RuntimeError as exc:
        raise SystemExit(f"remote-cmd tunnel not available: {exc}") from None
    try:
        if args.cancel:
            cancel(cfg, remote, args.cancel)
        elif args.status:
            show_status(cfg, remote, args.status, args.exp, args.run)
        else:
            submit(
                cfg,
                remote,
                args.exp,
                args.run,
                [*shlex.split(args.crystfel_args), *args.rest],
                args.xrd_dest or cfg.xrd_dest,
            )
    except (RemoteError, httpx.HTTPError) as exc:
        raise SystemExit(f"remote-cmd: {exc}") from None
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

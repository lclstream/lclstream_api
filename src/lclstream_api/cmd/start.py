import sys
import re
from typing import List
import asyncio
# from aiowire import EventLoop, Wire, Call

import psik

from psik.models import JobState
from psik.job import runcmd


def get_state(ss) -> JobState:
    """Decode SLURM's response on a job state to an enum."""
    ss = ss.split(" ", 1)[0]
    if ss in ["RUNNING"]:
        return JobState.active
    if ss in ["COMPLETED"]:
        return JobState.completed
    if ss in ["PENDING", "REQUEUED", "RESIZING"]:
        return JobState.queued
    if ss in ["REVOKED", "PREEMPTED", "CANCELLED"]:
        return JobState.canceled
    if ss in [
        "FAILED",
        "BOOT_FAIL",
        "TIMEOUT",
        "SUSPENDED",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "DEADLINE",
    ]:
        return JobState.failed
    print(f"Unknown SLURM job state: {ss}")
    return JobState.failed


# Query SLURM for job's state.
async def slurm_state(jobid: int) -> JobState:
    """Lookup the state of a Slurm job on psana"""
    cmd = f"ssh psana sacct -j {jobid} -X -p --delimiter , --format JobID,State,ExitCode".split()
    ret, out, err = await runcmd(*cmd, expect_ok=True)
    if ret != 0:
        print("Error retrieving job state:")
        print(err)
        return JobState.failed
    # JobID,State,ExitCode,
    # 63254802,RUNNING,0:0,
    for line in out.split("\n"):
        tok = line.split(",")
        if len(tok) < 3:
            continue
        if tok[0] == str(jobid):
            return get_state(tok[1])

    print("Job state not found!")
    return JobState.canceled


async def start_slurm(script, *args) -> int:
    """Run a job-script on psana.
    Return the jobid, or -1 on error.
    """
    cmd = ["ssh", "psana", "sbatch", script] + list(args)
    ret, out, err = await runcmd(*cmd, expect_ok=True)
    if ret != 0:
        print("Error Starting Job")
        print(err)
        return -1
    for line in out.split("\n"):
        m = re.match(r".*[ \t]([0-9][0-9]*)", line)
        if m:
            return int(m[1])
    print("Error Starting Job -- invalid output:")
    print(out)
    return -1


async def kill_slurm(n: int) -> None:
    """Cancel the Slurm jobid on psana."""
    cmd = ["ssh", "psana", "scancel", str(n)]
    ret, out, err = await runcmd(*cmd, expect_ok=True)
    if ret != 0:
        print("Error Stopping Job")
        print(err)


# <start> --> <cache running> -> <producer running>
#
#


async def print_stream(s: asyncio.StreamReader) -> None:
    """Print the stream as it is generated.
    Note: we should just remove the stderr pipe from popen.
    """
    async for data in s:
        line = data.decode("utf-8")
        print(line, file=sys.stderr)


async def parse_logs(s: asyncio.StreamReader) -> None:
    """Parse the logs present in stdout."""
    async for data in s:
        line = data.decode("utf-8")
        print(line, end="", flush=True)


async def watch_cmd(proc) -> None:
    """Print the proc's output as it is generated."""
    t1 = print_stream(proc.stderr)
    t2 = parse_logs(proc.stdout)
    for task in asyncio.as_completed([t1, t2]):
        await task


class SlurmJob:
    """A SLURM job that can be run() and kill()-ed"""

    def __init__(self):
        self.jobid = None
        self.state = JobState.new

    async def run(self, cmd):
        jobid = await start_slurm(cmd)
        self.jobid = jobid
        dt = 0.0
        for i in range(33):  # wait 60 seconds for job start
            await asyncio.sleep(dt)
            dt = min(2.0, dt + 0.5)
            self.state = await slurm_state(jobid)
            if self.state != JobState.queued:
                break
        else:
            return await self.kill()

        if self.state.is_final():
            return self.done()

        # print(f"Job {jobid} is {self.state.value}.")

        dt = 5.0
        for i in range(1000):  # max time ~ hrs.
            await asyncio.sleep(dt)
            # poll logarithmically up to 2 minute intervals for job completion
            dt = min(120.0, dt * 2.0)
            self.state = await slurm_state(jobid)
            if self.state.is_final():
                print(f"Job {jobid} is {self.state.value}.")
                break
        else:
            return await self.kill()
        return self.done()

    def done(self) -> JobState:
        self.jobid = None
        return self.state

    async def kill(self) -> JobState:
        if self.jobid is not None:
            await kill_slurm(self.jobid)
            self.jobid = None
        self.state = JobState.canceled
        return self.state


# def handle_cmd_error(ev: EventLoop, exc: Exception):
#    raise RuntimeError("Error running command.") from exc


async def stream_job(fname, port):
    """Main process to

    1. start the cache (local process)
    2. start the producer (via slurm)
    3. monitor the joint process

    fname: name of file to send from psana node
    port:  port on this server to serve TCP nng-push stream
    """

    # /sdf/home/r/rogersdd/src/nng_stream/nng_cache -vv tcp://$addr:$recv tcp://$addr:$send &

    # submit_job() {
    #  ssh psana sbatch /sdf/home/r/rogersdd/venvs/run_file_push $fname \
    #                        tcp://$addr:$recv \
    #    | sed -n 's/.*[ \t]\([0-9][0-9]*\).*/\1/p'
    #
    recv = port - 1
    proc = await asyncio.create_subprocess_exec(
        # ["/sdf/home/r/rogersdd/src/nng_stream/nng_cache", "-v",
        #   f"tcp://134.79.23.43:{recv}",
        #   f"tcp://134.79.23.43:{port}"],
        [
            "/home/99r/src/microservices/nng_stream/nng_cache",
            "-v",
            f"tcp://127.0.0.1:{recv}",
            f"tcp://127.0.0.1:{port}",
        ],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    S = SlurmJob()

    cache = watch_cmd(proc)
    slurm = S.run("/sdf/home/r/rogersdd/venvs/run_file_push", fname, uri)
    slurm_done = False
    for task in asyncio.as_completed([cache, slurm]):
        ans = await task
        if task == slurm:
            slurm_done = True
            if ans != JobState.complete:
                await proc.kill()
                raise RuntimeError(f"Slurm job exited at state {ans.value}")
        elif task == cache:
            if not slurm_done:
                await S.kill()
                print("Warning: Cache completed before SLURM job.")
            break

        else:
            raise KeyError(f"Unknown task: {task}")

    # Wait for the subprocess exit.
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"Cache returned {proc.returncode}")
    return 0


if __name__ == "__main__":
    argv = sys.argv
    fname = argv[1]
    port = int(argv[2])
    sys.exit(asyncio.run(stream_job(fname, port)))

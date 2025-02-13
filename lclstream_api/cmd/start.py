import sys
import re
from typing import List
import asyncio
from aiowire import EventLoop, Wire, Call

import psik

from psik.models import JobState
from psik.job import runcmd

def get_state(ss) -> JobState:
    """Decode SLURM's response on a job state to an enum.
    """
    ss = ss.split(' ', 1)[0]
    if ss in ["RUNNING"]:
        return JobState.active
    if ss in ["COMPLETED"]:
        return JobState.completed
    if ss in ["PENDING", "REQUEUED", "RESIZING"]:
        return JobState.queued
    if ss in ["REVOKED", "PREEMPTED", "CANCELLED"]:
        return JobState.canceled
    if ss in ["FAILED", "BOOT_FAIL", "TIMEOUT", "SUSPENDED", "OUT_OF_MEMORY", "NODE_FAIL", "DEADLINE"]:
        return JobState.failed
    print(f"Unknown SLURM job state: {ss}")
    return JobState.failed

# Query SLURM for job's state.
async def slurm_state(jobid: int) -> JobState:
    """Lookup the state of a Slurm job on psana
    """
    cmd = f"ssh psana sacct -j {jobid} -X -p --delimiter , --format JobID,State,ExitCode".split()
    ret, out, err = await runcmd(*cmd, expect_ok=True)
    if ret != 0:
        print("Error retrieving job state:")
        print(err)
        return JobState.failed
    # JobID,State,ExitCode,
    # 63254802,RUNNING,0:0,
    for line in out.split('\n'):
        tok = line.split(',')
        if len(tok) < 3:
            continue
        if tok[0] == str(jobid):
            return get_state(tok[1])

    print("Job state not found!")
    return JobState.canceled

async def start_slurm(script) -> int:
    """Run a job-script on psana.
    Return the jobid, or -1 on error.
    """
    cmd = ["ssh", "psana", "sbatch", script]
    ret, out, err = await runcmd(*cmd, expect_ok=True)
    if ret != 0:
        print("Error Starting Job")
        print(err)
        return -1
    for line in out.split('\n'):
        m = re.match(r'.*[ \t]([0-9][0-9]*)', line)
        if m:
            return int(m[1])
    print("Error Starting Job -- invalid output:")
    print(out)
    return -1

async def kill_slurm(n: int) -> None:
    """Cancel the Slurm jobid on psana.
    """
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
        line = data.decode('utf-8')
        print(line, file=sys.stderr)

async def watch_cmd(ev: EventLoop, proc) -> None:
    """Print the proc's output as it is generated.
    """
    ev.start( Call(print_stream, proc.stderr) )
    # Read one line of output.
    async for data in proc.stdout:
        line = data.decode('utf-8')
        print(line, end='', flush=True)

class SlurmJob:
    """A SLURM job that can be run() and kill()-ed
    """
    def __init__(self):
        self.jobid = None
    def kill(self):
        if self.jobid is not None:
            kill_slurm(self.jobid)
            self.jobid = None
    async def run(self, cmd):
        jobid = await start_slurm(cmd)
        self.jobid = jobid
        dt = 0.0
        for i in range(33): # wait 60 seconds for job start
            await asyncio.sleep(dt)
            dt = min(2.0, dt+0.5)
            state = await slurm_state(jobid)
            if state != JobState.queued:
                break
        else:
            kill_slurm(jobid)
            return self.done(state)

        if state.is_final():
            return self.done(state)

        print(f"Job {jobid} is {state.value}.")

        dt = 5.0
        for i in range(1000):
            await asyncio.sleep(dt)
            # poll every 2 minutes for job completion
            dt = min(120.0, dt*2.0)
            state = await slurm_state(jobid)
            if state.is_final():
                print(f"Job {jobid} is {state.value}.")
                break
        else:
            kill_slurm(jobid)
            return self.done(JobState.canceled)
        return self.done(state)

    def done(self, state):
        self.jobid = None
        return state

def handle_cmd_error(ev: EventLoop, exc: Exception):
    raise RuntimeError("Error running command.") from exc


async def stream_job():
    """ Main process to

    1. start the cache (local process)
    2. start the producer (via slurm)
    3. monitor the joint process
    """

    proc = await asyncio.create_subprocess_exec(
            ["ls", "/root"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
    err = False

    S = SlurmJob()
    async with EventLoop() as event:
        cache = watch_cmd(ev, proc)
        slurm = S.run("/sdf/home/r/rogersdd/sleep.sh")
        async for task in asyncio.as_completed([cache, slurm]):
            ans = await task
            if task == slurm:
                if ans != JobState.complete:
                    proc.kill()
                    raise RuntimeError(f"Slurm job exited at state {ans.value}")
            elif task == cache:
                await proc.wait()
                if proc.returncode != 0:
                    S.kill()
                    raise RuntimeError(f"Command returned {proc.returncode}")
            else:
                raise KeyError(f"Unknown task: {task}")

    # Wait for the subprocess exit.

if __name__=="__main__":
    asyncio.run( stream_job() )

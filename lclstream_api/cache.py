# Functions to manage a locally running cache process.

import asyncio
from pydantic import ValidationError

from .models import PortEntry, CacheMetrics, JobState
from .config import Config

async def parse_logs(s: asyncio.StreamReader, port: PortEntry) -> None:
    """ Parse the logs present in nng_cache's stdout.
        Stash updates in port.cache_state, port.cache_metrics
    """
    print("Waiting for stdout...", flush=True)
    while True:
        data = await s.readline()
        if not data:
            break
    #async for data in s:
        #print(f"read: {data.decode('utf-8')}", flush=True)
        try:
            val = CacheMetrics.model_validate_json(data)
        except ValidationError:
            continue
        if port.cache_state == JobState.new:
            port.cache_state = JobState.active
        #line = data.decode('utf-8')
        #print(line, end='', flush=True)
        port.cache_metrics = val

async def watch_cmd(*args, **kws) -> None:
    """ Run the following command and use parse_logs
        to read its stdout as it is generated.
    """
    proc = None
    port = kws['port']
    try:
        proc = await asyncio.create_subprocess_exec(*args,
                stdout=asyncio.subprocess.PIPE)
                # can use a proc. group to ensure child processes are killed too
                # start_new_session = True
        if proc.stdout:
            await parse_logs(proc.stdout, **kws)

        # proc has now closed stdout
        returncode = await proc.wait()

        # Parse cache's return code into a final state.
        if returncode == 0:
            port.cache_state = JobState.completed
        else:
            port.cache_state = JobState.failed
    except asyncio.CancelledError:
        if proc:
            # can use os.killpg(proc.pid, signal.SIGTERM) or SIGKILL 
            # to kill entire process group
            proc.terminate()
            # Give it a moment to terminate (optional)
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except (asyncio.TimeoutError, ProcessLookupError):
                # Force kill if it didn't terminate in time
                # TODO: send to _logger.
                print("Subprocess did not terminate quickly. Killing.")
                proc.kill()

            port.cache_state = JobState.canceled

async def cache_process(run_cache: str, port: PortEntry) -> asyncio.Task:
    """ Start the cache (usu. run_cache = nng_cache or nz_cache)
        process in the background and collect its status
        inside port.cache_*.

        Returns a running Task.
    """
    cache = watch_cmd(run_cache, "-v",
                      port.internal_url,
                      port.external_url, port=port)
    return asyncio.create_task(cache, name=run_cache)

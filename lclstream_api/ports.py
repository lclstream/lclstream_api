from typing import Optional, Dict
from typing_extensions import Annotated
import asyncio
import logging
_logger = logging.getLogger(__name__)

from pydantic import BaseModel
from fastapi import Depends

from .models import PortEntry, JobState
from .cache import cache_process
from .config import Config, load_config

CachedConfig = Annotated[Config, Depends(load_config)]

class PortDatabase: # singleton
    def __init__(self, host: str, run_cache: str, start=30001, end=34000) -> None:
        assert end > start+1, "Need at least 2 ports"
        self.host = host
        self.run_cache = run_cache

        self.open_ports = list(range(start, end, 2))
        # Mapping from jobid to user, port pairs.
        self.jobs: Dict[str, PortEntry] = {}
        self.tasks: Dict[str, asyncio.Task] = {}

    def items(self):
        return self.jobs.items()

    def alloc(self) -> Optional[int]:
        """ Allocate a port -- usually called automagically
            during create().
        """
        if len(self.open_ports) == 0:
            _logger.error("No more open ports!")
            return None
        return self.open_ports.pop()

    def free(self, port):
        self.open_ports.append(port)

    def internal_url(self, port: int) -> str:
        # Internal ports are first in sequence
        return f"tcp://{self.host}:{port}"
    def external_url(self, port: int) -> str:
        # External ports are internal+1
        return f"tcp://{self.host}:{port+1}"

    async def create(self,
                     jobid: str,
                     user: str,
                     port: Optional[int] = None) -> PortEntry:
        if jobid in self.jobs:
            entry = self.jobs[jobid]
            # Make create idempotent
            if entry.user == user:
                return entry
            raise KeyError(f"Job {jobid} already created by another user!")
        if port is None:
            port = self.alloc()
        if port is None:
            raise RuntimeError("No available ports.")

        entry = PortEntry(
            user = user,
            port = port,
            internal_url = self.internal_url(port),
            external_url = self.external_url(port),
        )
        self.jobs[jobid] = entry
        self.tasks[jobid] = await cache_process(self.run_cache, entry)
        return entry

    def __getitem__(self, jobid: str) -> PortEntry:
        return self.jobs[jobid]

    async def delete(self, jobid: str) -> PortEntry:
        entry = self.jobs.pop(jobid)
        self.free(entry.port)
        task = self.tasks[jobid]
        if not task.done():
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return entry

DB: PortDatabase = None # type: ignore[assignment]
def get_database(config: CachedConfig) -> PortDatabase:
    # initialize on first access (allows db to be configurable)
    global DB
    if DB is None:
        DB = PortDatabase(config.cache_ip,
                          config.run_cache,
                          config.start_port,
                          config.end_port)
    return DB

Database = Annotated[PortDatabase, Depends(get_database)]

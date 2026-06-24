"""This file implements a "PortEntry" table, which essentially has
the format:

- id (tracked with self.sequence)
- user
- port
- internal_url
- external_url
++ foreign_key to xfer (not present here, linked FROM xfer_db)
"""

import logging
from typing import Annotated

from fastapi import Depends

from .config import Config, ForwarderConfig, load_config
from .models import PortEntry

_logger = logging.getLogger(__name__)

CachedConfig = Annotated[Config, Depends(load_config)]


class PortDatabase:  # singleton
    def __init__(self, forwarder: ForwarderConfig) -> None:
        assert forwarder.end_port > forwarder.start_port + 1, "Need at least 2 ports"
        self.host = forwarder.ip
        self.sequence = 1  # sequential index number

        self.open_ports = list(range(forwarder.start_port, forwarder.end_port, 2))
        # Mapping from eid to user, port pairs.
        self.entries: dict[int, PortEntry] = {}

    def items(self):
        return self.entries.items()

    def alloc(self) -> int | None:
        """Allocate a port -- usually called automagically
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
        return f"tcp://{self.host}:{port + 1}"

    def create(self, user: str) -> PortEntry:
        eid = self.sequence
        self.sequence += 1

        # if eid in self.entries:
        #    entry = self.entries[eid]
        #    # Make create idempotent
        #    if entry.user == user:
        #        return entry
        #    raise KeyError(f"PortEntry {eid} already created by another user!")
        port = self.alloc()
        if port is None:
            raise RuntimeError("No available ports.")

        entry = PortEntry(
            eid=eid,
            user=user,
            port=port,
            internal_url=self.internal_url(port),
            external_url=self.external_url(port),
        )
        self.entries[eid] = entry

        return entry

    def __getitem__(self, eid: int) -> PortEntry:
        return self.entries[eid]

    def delete(self, eid: int) -> PortEntry:
        entry = self.entries.pop(eid)
        self.free(entry.port)
        return entry


DB: PortDatabase = None  # type: ignore[assignment]


def get_portusage(config: CachedConfig) -> PortDatabase:
    # initialize on first access (allows db to be configurable)
    global DB
    if DB is None:
        DB = PortDatabase(config.forwarder)
    return DB


PortUsage = Annotated[PortDatabase, Depends(get_portusage)]

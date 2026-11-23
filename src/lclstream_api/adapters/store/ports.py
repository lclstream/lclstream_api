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

from pydantic import BaseModel

from ..psik.config import ForwarderConfig

_logger = logging.getLogger(__name__)


class PortEntry(BaseModel):
    id: int  # serial number
    user: str
    port: int
    internal_url: str
    external_url: str


class PortDatabase:  # singleton
    def __init__(self, forwarder: ForwarderConfig) -> None:
        assert forwarder.end_port > forwarder.start_port + 1, "Need at least 2 ports"
        self.host = forwarder.ip
        self.sequence = 1  # sequential index number

        self.open_ports = list(range(forwarder.start_port, forwarder.end_port, 2))
        # Mapping from transfer_id to user, port pairs.
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
        transfer_id = self.sequence
        self.sequence += 1

        # if transfer_id in self.entries:
        #    entry = self.entries[transfer_id]
        #    # Make create idempotent
        #    if entry.user == user:
        #        return entry
        #    raise KeyError(f"PortEntry {transfer_id} already created by another user!")
        port = self.alloc()
        if port is None:
            raise RuntimeError("No available ports.")

        entry = PortEntry(
            id=transfer_id,
            user=user,
            port=port,
            internal_url=self.internal_url(port),
            external_url=self.external_url(port),
        )
        self.entries[transfer_id] = entry

        return entry

    def __getitem__(self, transfer_id: int) -> PortEntry:
        return self.entries[transfer_id]

    def delete(self, transfer_id: int) -> PortEntry:
        entry = self.entries.pop(transfer_id)
        self.free(entry.port)
        return entry

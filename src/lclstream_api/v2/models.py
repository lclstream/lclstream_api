from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict

from ..lclstreamer_param import Parameters as LCLStreamerParameters
from .core.enums import (
    CacheMode,
    CacheState,
    ConsumerSocket,
    TransferState,
    TransitionSource,
)
from .core.logs import LogStream
from .core.producer import JobSpec

if TYPE_CHECKING:
    from .tables import Transfer


class Message(BaseModel):
    message: str


class TransferCancelOutcome(StrEnum):
    already_final = "already_final"
    canceling = "canceling"  # cancellation recorded, teardown to follow


class ConsumerConnectionInfo(BaseModel):
    """The cache socket the consumer connects to, and how to dial it."""

    host: str
    port: int
    uri: str
    socket: ConsumerSocket

    @classmethod
    def from_endpoint(
        cls, host: str, port: int, socket: ConsumerSocket
    ) -> ConsumerConnectionInfo:
        return cls(host=host, port=port, uri=f"tcp://{host}:{port}", socket=socket)


class TransferCreate(BaseModel):
    parameters: LCLStreamerParameters
    cache_mode: CacheMode = CacheMode.per_transfer
    consumer_socket: ConsumerSocket = ConsumerSocket.pull
    job_spec_override: JobSpec | None = None


class TransitionPublic(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    created_at: AwareDatetime
    state: TransferState
    info: str | None = None
    source: TransitionSource | None = None


class TransferPublic(BaseModel):
    id: UUID
    created_at: AwareDatetime
    requested_by: str
    state: TransferState
    experiment: str
    run: str
    cache_mode: CacheMode
    connection_info: ConsumerConnectionInfo | None = None

    @classmethod
    def from_transfer(
        cls, transfer: Transfer, consumer_hosts: Mapping[str, str] | None = None
    ) -> TransferPublic:
        connection_info: ConsumerConnectionInfo | None = None
        if (
            transfer.state == TransferState.ready
            and transfer.cache_hostname is not None
            and transfer.push_port is not None
        ):
            hostname = (consumer_hosts or {}).get(
                transfer.cache_hostname, transfer.cache_hostname
            )
            connection_info = ConsumerConnectionInfo.from_endpoint(
                hostname,
                transfer.push_port,
                ConsumerSocket(transfer.consumer_socket),
            )
        return cls(
            id=transfer.id,
            created_at=transfer.created_at,
            requested_by=transfer.owner_email,
            state=TransferState(transfer.state),
            experiment=transfer.experiment,
            run=transfer.run,
            cache_mode=CacheMode(transfer.cache_mode),
            connection_info=connection_info,
        )


class TransferDetail(TransferPublic):
    transitions: list[TransitionPublic] = []

    @classmethod
    def from_transfer(
        cls, transfer: Transfer, consumer_hosts: Mapping[str, str] | None = None
    ) -> TransferDetail:
        base = TransferPublic.from_transfer(transfer, consumer_hosts)
        return cls(
            **base.model_dump(),
            transitions=[
                TransitionPublic.model_validate(t) for t in transfer.transitions
            ],
        )


class TransfersPublic(BaseModel):
    data: list[TransferPublic]
    count: int


class TransferLogStreamInfo(BaseModel):
    """One log stream's resolved location and (best-effort) availability."""

    stream: LogStream
    path: Path
    available: bool
    size: int | None = None
    modified_at: AwareDatetime | None = None


class TransferLogIndex(BaseModel):
    transfer_id: UUID
    streams: list[TransferLogStreamInfo]


class CacheStatusPublic(BaseModel):
    id: UUID
    state: CacheState


class CachesPublic(BaseModel):
    data: list[CacheStatusPublic]

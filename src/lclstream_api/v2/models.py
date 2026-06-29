from enum import StrEnum
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict

from ..lclstreamer_param import Parameters as LCLStreamerParameters

if TYPE_CHECKING:
    from .tables import Transfer


class Message(BaseModel):
    message: str


class TransferState(StrEnum):
    provisioning = "provisioning"  # spinning up cache + producer
    ready = "ready"  # both active, connection_info available
    canceling = "canceling"  # cancel requested, teardown in progress
    canceled = "canceled"
    completed = "completed"
    failed = "failed"

    def is_final(self) -> bool:
        return self in (
            TransferState.canceled,
            TransferState.completed,
            TransferState.failed,
        )


class TransitionSource(StrEnum):
    producer = "producer"
    cache = "cache"
    user = "user"
    orchestrator = "orchestrator"  # from lclstream_api


class TransferCancelOutcome(StrEnum):
    already_final = "already_final"
    canceling = "canceling"  # cancellation recorded, teardown to follow


class ConsumerConnectionInfo(BaseModel):
    """The cache push socket the consumer connects to."""

    host: str
    port: int
    uri: str

    @classmethod
    def from_endpoint(cls, host: str, port: int) -> ConsumerConnectionInfo:
        return cls(host=host, port=port, uri=f"tcp://{host}:{port}")


class TransferCreate(BaseModel):
    parameters: LCLStreamerParameters


class TransitionPublic(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    created_at: AwareDatetime
    state: TransferState
    info: str | None = None
    source: TransitionSource | None = None


class TransferPublic(BaseModel):
    id: UUID
    created_at: AwareDatetime
    requested_by: str  # user who requested the transfer
    state: TransferState
    connection_info: ConsumerConnectionInfo | None = None

    @classmethod
    def from_transfer(cls, transfer: Transfer) -> TransferPublic:
        connection_info: ConsumerConnectionInfo | None = None
        if (
            transfer.state == TransferState.ready
            and transfer.cache_hostname is not None
            and transfer.push_port is not None
        ):
            connection_info = ConsumerConnectionInfo.from_endpoint(
                transfer.cache_hostname, transfer.push_port
            )
        return cls(
            id=transfer.id,
            created_at=transfer.created_at,
            requested_by=transfer.user,
            state=TransferState(transfer.state),
            connection_info=connection_info,
        )


class TransferDetail(TransferPublic):
    transitions: list[TransitionPublic] = []

    @classmethod
    def from_transfer(cls, transfer: Transfer) -> TransferDetail:
        base = TransferPublic.from_transfer(transfer)
        return cls(
            **base.model_dump(),
            transitions=[
                TransitionPublic.model_validate(t) for t in transfer.transitions
            ],
        )


class TransfersPublic(BaseModel):
    data: list[TransferPublic]
    count: int

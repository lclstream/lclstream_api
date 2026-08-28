from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)

from .models import TransferState


class Base(DeclarativeBase):
    pass


class UpdatedAtMixin:
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CreatedAtMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class DTMixin(CreatedAtMixin, UpdatedAtMixin):
    pass


class Transfer(DTMixin, Base):
    __tablename__ = "transfers"

    id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    user: Mapped[str] = mapped_column(doc="User that created this transfer")

    # Denormalized current state; the transitions table is the source of truth.
    state: Mapped[str] = mapped_column(
        default=TransferState.provisioning,
        doc="Current lifecycle state; stored as the TransferState string value",
    )

    # Full lclstreamer request payload (includes experiment/run).
    parameters: Mapped[dict[str, Any]] = mapped_column(
        JSON, doc="Full lclstreamer Parameters payload"
    )

    # Cache reference (lives in fastcache_api's DB on the DTN).
    cache_id: Mapped[UUID | None] = mapped_column(
        default=None, doc="Soft link to fastcache_api Cache.id"
    )
    cache_hostname: Mapped[str | None] = mapped_column(
        default=None, doc="DTN hostname running the cache; routes follow-up calls"
    )

    # ZMQ port pair allocated by fastcache_api.
    pull_port: Mapped[int | None] = mapped_column(
        default=None, doc="Producer/internal side port"
    )
    push_port: Mapped[int | None] = mapped_column(
        default=None, doc="Consumer/external side port (= pull_port + 1)"
    )

    # Producer reference (IRI job, submitted via amscrot).
    producer_job_id: Mapped[str | None] = mapped_column(
        default=None, doc="IRI job id of the producer"
    )

    # Used to reject out-of-order observations so a slow reconcile cannot
    # regress state. ``None`` until the first observation is applied.
    last_polled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        doc="Observation timestamp of the newest applied reconcile decision",
    )

    transitions: Mapped[list[Transition]] = relationship(
        back_populates="transfer",
        cascade="all, delete-orphan",
        order_by="Transition.created_at",
    )


class Transition(CreatedAtMixin, Base):
    __tablename__ = "transitions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    transfer_id: Mapped[UUID] = mapped_column(
        ForeignKey("transfers.id", ondelete="CASCADE")
    )
    state: Mapped[str] = mapped_column(doc="TransferState entered at this transition")
    info: Mapped[str | None] = mapped_column(
        default=None, doc="Message / failure detail"
    )
    source: Mapped[str | None] = mapped_column(
        default=None,
        doc="Which side drove it (TransitionSource): producer/cache/user/orchestrator",
    )

    transfer: Mapped[Transfer] = relationship(back_populates="transitions")

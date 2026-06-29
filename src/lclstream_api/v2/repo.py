import logging
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .core import transfer as tcore
from .models import TransferState, TransitionSource
from .tables import Transfer, Transition

logger = logging.getLogger(__name__)

_FINAL_STATES = frozenset(s for s in TransferState if s.is_final())


async def insert_transfer(
    session: AsyncSession,
    *,
    transfer_id: UUID,
    user: str,
    parameters: dict[str, Any],
) -> Transfer:
    transfer = Transfer(
        id=transfer_id,
        user=user,
        state=TransferState.provisioning,
        parameters=parameters,
    )
    session.add(transfer)
    session.add(
        Transition(
            transfer_id=transfer_id,
            state=TransferState.provisioning,
            source=TransitionSource.orchestrator,
        )
    )
    await session.flush()
    await session.refresh(transfer)
    return transfer


async def get_transfer(session: AsyncSession, transfer_id: UUID) -> Transfer | None:
    return await session.get(Transfer, transfer_id)


async def get_transfer_with_transitions(
    session: AsyncSession, transfer_id: UUID
) -> Transfer | None:
    result = await session.execute(
        select(Transfer)
        .where(Transfer.id == transfer_id)
        .options(selectinload(Transfer.transitions))
    )
    return result.scalar_one_or_none()


async def list_transfers(
    session: AsyncSession,
    *,
    skip: int = 0,
    limit: int = 100,
    state: TransferState | None = None,
) -> tuple[list[Transfer], int]:
    query = select(Transfer)
    if state is not None:
        query = query.where(Transfer.state == state)
    count_result = await session.execute(
        select(func.count()).select_from(query.subquery())
    )
    count = count_result.scalar_one()
    result = await session.execute(
        query.order_by(Transfer.created_at.desc()).offset(skip).limit(limit)
    )
    return list(result.scalars().all()), count


async def list_unsettled_transfers(session: AsyncSession) -> list[Transfer]:
    result = await session.execute(
        select(Transfer).where(Transfer.state.not_in(_FINAL_STATES))
    )
    return list(result.scalars().all())


async def set_cache_endpoints(
    session: AsyncSession,
    transfer_id: UUID,
    *,
    cache_id: UUID,
    hostname: str,
    pull_port: int,
    push_port: int,
) -> None:
    """Record the cache location returned by fastcache_api on the transfer."""

    transfer = await session.get(Transfer, transfer_id)
    if transfer is None:
        raise LookupError(f"transfer {transfer_id} not found")
    transfer.cache_id = cache_id
    transfer.cache_hostname = hostname
    transfer.pull_port = pull_port
    transfer.push_port = push_port


async def set_producer_job(
    session: AsyncSession, transfer_id: UUID, producer_job_id: str
) -> None:
    transfer = await session.get(Transfer, transfer_id)
    if transfer is None:
        raise LookupError(f"transfer {transfer_id} not found")
    transfer.producer_job_id = producer_job_id


async def record_state(
    session: AsyncSession,
    transfer_id: UUID,
    state: TransferState,
    *,
    info: str | None = None,
    source: TransitionSource | None = None,
    observed_at: AwareDatetime | None = None,  # reconcile loop observation time
) -> bool:
    transfer = await session.get(Transfer, transfer_id)
    if transfer is None:
        raise LookupError(f"transfer {transfer_id} not found")

    current = TransferState(transfer.state)

    # we reject out-of-order observations so slow reconcile cannot regress state
    if tcore.is_stale_observation(observed_at, transfer.last_polled_at):
        return False

    if state != current and not tcore.can_transition(current, state):
        logger.debug(
            "illegal transition %s -> %s rejected on transfer %s",
            current,
            state,
            transfer_id,
        )
        return False

    # advance the observation watermark for any accepted observation
    if observed_at is not None:
        transfer.last_polled_at = observed_at
    if state == current:
        return False

    transfer.state = state
    session.add(
        Transition(
            transfer_id=transfer_id,
            state=state,
            info=info,
            source=source,
        )
    )
    return True

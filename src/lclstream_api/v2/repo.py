import logging
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .core import transfer as tcore
from .models import TransferState, TransitionSource
from .tables import Transfer, Transition, UserCredential

logger = logging.getLogger(__name__)

_FINAL_STATES = frozenset(s for s in TransferState if s.is_final())


async def insert_transfer(
    session: AsyncSession,
    *,
    transfer_id: UUID,
    owner_issuer: str,
    owner_subject: str,
    owner_email: str,
    parameters: dict[str, Any],
    experiment: str,
    run: str,
    job_spec: dict[str, Any],
    cache_mode: tcore.CacheMode = tcore.CacheMode.per_transfer,
) -> Transfer:
    transfer = Transfer(
        id=transfer_id,
        owner_issuer=owner_issuer,
        owner_subject=owner_subject,
        owner_email=owner_email,
        state=TransferState.provisioning,
        parameters=parameters,
        experiment=experiment,
        run=run,
        cache_mode=cache_mode,
        job_spec=job_spec,
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


async def get_owned_transfer(
    session: AsyncSession,
    transfer_id: UUID,
    *,
    owner_issuer: str,
    owner_subject: str,
) -> Transfer | None:
    result = await session.execute(
        select(Transfer).where(
            Transfer.id == transfer_id,
            Transfer.owner_issuer == owner_issuer,
            Transfer.owner_subject == owner_subject,
        )
    )
    return result.scalar_one_or_none()


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


async def find_latest_shared_transfer_cache(
    session: AsyncSession, experiment: str
) -> UUID | None:
    """Most recent shared-mode cache_id created for this experiment, if any."""

    result = await session.execute(
        select(Transfer.cache_id)
        .where(
            Transfer.experiment == experiment,
            Transfer.cache_mode == tcore.CacheMode.shared,
            Transfer.cache_id.is_not(None),
        )
        .order_by(Transfer.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def count_active_transfers_by_cache(session: AsyncSession, cache_id: UUID) -> int:
    result = await session.execute(
        select(func.count())
        .select_from(Transfer)
        .where(Transfer.cache_id == cache_id, Transfer.state.not_in(_FINAL_STATES))
    )
    return result.scalar_one()


async def get_user_credential(
    session: AsyncSession, issuer: str, subject: str
) -> UserCredential | None:
    return await session.get(UserCredential, (issuer, subject))


async def upsert_user_credential(
    session: AsyncSession,
    *,
    issuer: str,
    subject: str,
    email: str,
    encrypted_token: bytes,
    expires_at: AwareDatetime,
) -> bool:
    """Atomically retain only the latest-expiring credential for a principal."""

    insert_statement = insert(UserCredential).values(
        issuer=issuer,
        subject=subject,
        email=email,
        encrypted_token=encrypted_token,
        expires_at=expires_at,
    )
    statement = insert_statement.on_conflict_do_update(
        index_elements=[UserCredential.issuer, UserCredential.subject],
        set_={
            UserCredential.email: insert_statement.excluded.email,
            UserCredential.encrypted_token: insert_statement.excluded.encrypted_token,
            UserCredential.expires_at: insert_statement.excluded.expires_at,
            UserCredential.updated_at: func.now(),
        },
        where=insert_statement.excluded.expires_at > UserCredential.expires_at,
    ).returning(UserCredential.expires_at)
    result = await session.execute(statement)
    return result.scalar_one_or_none() is not None


async def purge_expired_user_credentials(
    session: AsyncSession, *, now: AwareDatetime
) -> int:
    statement = (
        delete(UserCredential)
        .where(UserCredential.expires_at <= now)
        .returning(UserCredential.issuer)
    )
    result = await session.execute(statement)
    return len(result.scalars().all())

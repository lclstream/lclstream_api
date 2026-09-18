"""Application service: the router-facing API (commands + queries).

Async functions the HTTP routers call, each taking the request-scoped
``AsyncSession``. Reads map the ORM row to a frozen pydantic model so an ORM
object never escapes this layer. Writes own their transaction boundary via
``async with session.begin()``.

``create_transfer`` also starts the durable ``provision_transfer`` workflow,
committing the insert first so the workflow's first read sees the row.
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from dbos import DBOS, SetWorkflowID
from sqlalchemy.ext.asyncio import AsyncSession

from ..lclstreamer_param import Parameters
from . import repo, workflows
from .exceptions import NotFound
from .models import (
    TransferCancelOutcome,
    TransferDetail,
    TransferPublic,
    TransfersPublic,
    TransferState,
    TransitionSource,
)


async def create_transfer(
    session: AsyncSession, user: str, parameters: Parameters
) -> TransferPublic:
    transfer_id = uuid4()
    async with session.begin():
        transfer = await repo.insert_transfer(
            session,
            transfer_id=transfer_id,
            user=user,
            parameters=parameters.model_dump(mode="json"),
        )
        public = TransferPublic.from_transfer(transfer)
    # here we just start the workflow
    with SetWorkflowID(str(transfer_id)):
        await DBOS.start_workflow_async(workflows.provision_transfer, transfer_id)
    return public


async def list_transfers(
    session: AsyncSession,
    *,
    skip: int = 0,
    limit: int = 100,
    state: TransferState | None = None,
) -> TransfersPublic:
    transfers, count = await repo.list_transfers(
        session, skip=skip, limit=limit, state=state
    )
    return TransfersPublic(
        data=[TransferPublic.from_transfer(transfer) for transfer in transfers],
        count=count,
    )


async def get_transfer_detail(
    session: AsyncSession, transfer_id: UUID
) -> TransferDetail:
    transfer = await repo.get_transfer_with_transitions(session, transfer_id)
    if transfer is None:
        raise NotFound(f"transfer {transfer_id} not found")
    return TransferDetail.from_transfer(transfer)


async def cancel_transfer(
    session: AsyncSession, transfer_id: UUID
) -> TransferCancelOutcome:
    """Request cancellation and start reconcile workflow."""

    async with session.begin():
        transfer = await repo.get_transfer(session, transfer_id)
        if transfer is None:
            raise NotFound(f"transfer {transfer_id} not found")
        if TransferState(transfer.state).is_final():
            return TransferCancelOutcome.already_final
        await repo.record_state(
            session,
            transfer_id,
            TransferState.canceling,
            source=TransitionSource.user,
        )
    await DBOS.start_workflow_async(
        workflows.reconcile_now, transfer_id, datetime.now(UTC)
    )
    return TransferCancelOutcome.canceling

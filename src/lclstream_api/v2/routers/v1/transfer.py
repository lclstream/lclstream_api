from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from ... import service
from ...auth import CurrentUser
from ...core import producer as pcore
from ...db import get_session
from ...models import (
    Message,
    TransferCancelOutcome,
    TransferCreate,
    TransferDetail,
    TransferPublic,
    TransfersPublic,
    TransferState,
)

router = APIRouter(prefix="/transfers", tags=["transfers"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.get("/", response_model=TransfersPublic)
async def get_transfers(
    session: SessionDep,
    user: CurrentUser,
    skip: int = 0,
    limit: int = 100,
    state: TransferState | None = None,
) -> TransfersPublic:
    return await service.list_transfers(session, skip=skip, limit=limit, state=state)


@router.get("/{transfer_id}", response_model=TransferDetail)
async def get_transfer(
    transfer_id: UUID, session: SessionDep, user: CurrentUser
) -> TransferDetail:
    return await service.get_transfer_detail(session, transfer_id)


@router.post("/", response_model=TransferPublic, status_code=status.HTTP_201_CREATED)
async def new_transfer(
    body: TransferCreate, user: CurrentUser, session: SessionDep
) -> TransferPublic:
    exp, run = pcore.parse_exp_run(body.parameters.source_identifier)
    if exp is None or run is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Could not determine exp and run: encode them in "
                "parameters.source_identifier "
                "(e.g. 'exp=mfx100852324,run=355')."
            ),
        )
    return await service.create_transfer(session, user, body.parameters)


@router.delete("/{transfer_id}", response_model=Message)
async def cancel_transfer(
    transfer_id: UUID, session: SessionDep, user: CurrentUser
) -> Message:
    outcome = await service.cancel_transfer(session, transfer_id)
    if outcome == TransferCancelOutcome.already_final:
        return Message(message="Transfer is already in a final state")
    return Message(message="Cancellation requested")

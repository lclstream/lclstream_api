from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ... import service
from ...auth import CurrentUser
from ...db import get_session
from ...models import CacheShutdownConflict, CachesPublic, Message

router = APIRouter(prefix="/caches", tags=["caches"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.get("/", response_model=CachesPublic)
async def get_caches(
    session: SessionDep,
    user: CurrentUser,
    experiment: Annotated[str, Query()],
) -> CachesPublic:
    return await service.list_caches_for_experiment(session, experiment)


@router.delete(
    "/{cache_id}",
    response_model=Message,
    responses={409: {"model": CacheShutdownConflict}},
)
async def shutdown_cache(
    cache_id: UUID, session: SessionDep, user: CurrentUser, force: bool = False
) -> Message:
    await service.shutdown_cache(session, cache_id, force=force)
    return Message(message="Cache shutdown requested")

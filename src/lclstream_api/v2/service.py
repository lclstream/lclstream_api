"""Application service: the router-facing API (commands + queries).

Async functions the HTTP routers call, each taking the request-scoped
``AsyncSession``. Reads map the ORM row to a frozen pydantic model so an ORM
object never escapes this layer. Writes own their transaction boundary via
``async with session.begin()``.

``create_transfer`` also starts the durable ``provision_transfer`` workflow,
committing the insert first so the workflow's first read sees the row.
"""

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

from dbos import DBOS, SetWorkflowID
from sqlalchemy.ext.asyncio import AsyncSession

from ..lclstreamer_param import Parameters
from . import config, repo, workflows
from .clients import iri
from .core import logs as lcore, producer as pcore
from .exceptions import NotFound, UpstreamError
from .models import (
    TransferCancelOutcome,
    TransferDetail,
    TransferLogIndex,
    TransferLogStreamInfo,
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


# ---------------------------------------------------------------------------
# Log access
# ---------------------------------------------------------------------------


async def _resolve_exp_run(session: AsyncSession, transfer_id: UUID) -> tuple[str, str]:
    transfer = await repo.get_transfer(session, transfer_id)
    if transfer is None:
        raise NotFound(f"transfer {transfer_id} not found")
    try:
        return pcore.resolve_exp_run(Parameters.model_validate(transfer.parameters))
    except ValueError as exc:
        raise NotFound(
            f"transfer {transfer_id} has no resolvable exp/run for logs"
        ) from exc


async def read_transfer_log(
    session: AsyncSession,
    transfer_id: UUID,
    stream: lcore.LogStream,
    *,
    mode: lcore.LogReadMode = lcore.LogReadMode.tail,
    lines: int | None = None,
    bytes_: int | None = None,
) -> str:
    """Return the head/tail of a single log stream as decoded text."""
    exp, run = await _resolve_exp_run(session, transfer_id)
    path = lcore.log_stream_path(stream, config.producer, exp, run, transfer_id)
    client = iri.client()
    try:
        if mode is lcore.LogReadMode.head:
            return await client.head(path, lines=lines, bytes_=bytes_)
        return await client.tail(path, lines=lines, bytes_=bytes_)
    except iri.FilesystemError as exc:
        # Ambiguous failure: classify via stat. Missing file => 404; otherwise
        # the upstream filesystem itself failed => 502.
        stat = await client.stat(path)
        if not stat.exists:
            raise NotFound(
                f"log {stream.value} not found for transfer {transfer_id}"
            ) from exc
        raise UpstreamError(
            f"failed to read {stream.value} log for transfer {transfer_id}: {exc}"
        ) from exc


async def list_transfer_logs(
    session: AsyncSession, transfer_id: UUID
) -> TransferLogIndex:
    """Index every log stream for a transfer with its resolved path and, when
    the file exists, its size and last-modified time."""
    exp, run = await _resolve_exp_run(session, transfer_id)
    client = iri.client()
    paths = [
        (stream, lcore.log_stream_path(stream, config.producer, exp, run, transfer_id))
        for stream in lcore.LogStream
    ]
    stats = await asyncio.gather(*(client.stat(path) for _, path in paths))
    streams = [
        TransferLogStreamInfo(
            stream=stream,
            path=str(path),
            available=stat.exists,
            size=stat.size,
            modified_at=stat.modified_at,
        )
        for (stream, path), stat in zip(paths, stats, strict=True)
    ]
    return TransferLogIndex(transfer_id=transfer_id, streams=streams)

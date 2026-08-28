"""Application service: the router-facing API (commands + queries).

Async functions the HTTP routers call, each taking the request-scoped
``AsyncSession``. Reads map the ORM row to a frozen pydantic model so an ORM
object never escapes this layer. Writes own their transaction boundary via
``async with session.begin()``.

``create_transfer`` also starts the durable ``provision_transfer`` workflow,
committing the insert first so the workflow's first read sees the row.
"""

import asyncio
import logging
import socket
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
from dbos import DBOS, SetWorkflowID
from sqlalchemy.ext.asyncio import AsyncSession

from ..lclstreamer_param import Parameters
from . import config, repo, workflows
from .auth import AuthenticatedUser
from .clients import fastcache, iri
from .core import logs as lcore, producer as pcore
from .core.producer import JobSpec
from .exceptions import (
    CacheShutdownBlocked,
    DelegatedCredentialRejected,
    InsufficientTokenLifetime,
    NotFound,
    UpstreamError,
)
from .models import (
    CacheMode,
    CachesPublic,
    CacheStatusPublic,
    ConsumerSocket,
    TransferCancelOutcome,
    TransferDetail,
    TransferLogIndex,
    TransferLogStreamInfo,
    TransferPublic,
    TransfersPublic,
    TransferState,
    TransitionSource,
)

logger = logging.getLogger(__name__)


async def _consumer_hosts(*hostnames: str | None) -> dict[str, str]:
    """Resolve cache hosts to addresses for off-site consumers.

    We run inside SLAC, so our resolver knows the DTN names; the
    consumer's may not. Resolves the distinct names, not one per row.
    """
    if not config.get_fastcache().resolve_consumer_host:
        return {}
    resolved: dict[str, str] = {}
    for name in {host for host in hostnames if host}:
        try:
            resolved[name] = await asyncio.to_thread(socket.gethostbyname, name)
        except OSError:
            logger.warning("cache host %s did not resolve", name)
    return resolved


async def create_transfer(
    session: AsyncSession,
    user: AuthenticatedUser,
    parameters: Parameters,
    cache_mode: CacheMode = CacheMode.per_transfer,
    *,
    experiment: str,
    run: str,
    consumer_socket: ConsumerSocket = ConsumerSocket.pull,
    job_spec_override: JobSpec | None = None,
) -> TransferPublic:
    transfer_id = uuid4()
    job_spec = pcore.build_job_spec(
        parameters,
        config.get_producer(),
        name=pcore.producer_job_name(transfer_id),
        exp=experiment,
        run=run,
        transfer_id=transfer_id,
        username=user.username,
        job_spec_override=job_spec_override,
    )
    required_seconds = pcore.required_token_lifetime_seconds(
        job_spec, config.get_credentials().lifecycle_grace_seconds
    )
    remaining_seconds = max(
        0, int((user.expires_at - datetime.now(UTC)).total_seconds())
    )
    if remaining_seconds < required_seconds:
        raise InsufficientTokenLifetime(
            required_seconds=required_seconds,
            remaining_seconds=remaining_seconds,
        )
    async with session.begin():
        transfer = await repo.insert_transfer(
            session,
            transfer_id=transfer_id,
            owner_issuer=user.issuer,
            owner_subject=user.subject,
            owner_email=user.email,
            owner_username=user.username,
            parameters=parameters.model_dump(mode="json"),
            experiment=experiment,
            run=run,
            cache_mode=cache_mode,
            consumer_socket=consumer_socket,
            job_spec=job_spec.model_dump(mode="json"),
        )
        # Still provisioning, so it carries no connection info.
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
    hosts = await _consumer_hosts(*(transfer.cache_hostname for transfer in transfers))
    return TransfersPublic(
        data=[TransferPublic.from_transfer(transfer, hosts) for transfer in transfers],
        count=count,
    )


async def get_transfer_detail(
    session: AsyncSession, transfer_id: UUID
) -> TransferDetail:
    transfer = await repo.get_transfer_with_transitions(session, transfer_id)
    if transfer is None:
        raise NotFound(f"transfer {transfer_id} not found")
    hosts = await _consumer_hosts(transfer.cache_hostname)
    return TransferDetail.from_transfer(transfer, hosts)


async def cancel_transfer(
    session: AsyncSession, transfer_id: UUID, user: AuthenticatedUser
) -> TransferCancelOutcome:
    """Request owner cancellation and prompt durable reconciliation."""

    async with session.begin():
        transfer = await repo.get_owned_transfer(
            session,
            transfer_id,
            owner_issuer=user.issuer,
            owner_subject=user.subject,
        )
        if transfer is None:
            raise NotFound(f"transfer {transfer_id} not found")
        if TransferState(transfer.state).is_final():
            return TransferCancelOutcome.already_final
        producer_job_id = transfer.producer_job_id
        cache_id = transfer.cache_id
        cache_mode = CacheMode(transfer.cache_mode)
        await repo.record_state(
            session,
            transfer_id,
            TransferState.canceling,
            source=TransitionSource.user,
        )

    # Cancel now with the caller's own token.
    # The reconciler gets only the transfer id.
    if producer_job_id is not None:
        try:
            await iri.client().cancel_job(
                producer_job_id, user.token.get_secret_value()
            )
        except Exception:
            logger.exception(
                "immediate producer cancellation failed for %s", transfer_id
            )
    if cache_id is not None and cache_mode is CacheMode.per_transfer:
        try:
            await fastcache.client().delete_cache(cache_id)
        except httpx.HTTPError:
            logger.exception("immediate cache cancellation failed for %s", transfer_id)
    await DBOS.start_workflow_async(
        workflows.reconcile_now, transfer_id, datetime.now(UTC)
    )
    return TransferCancelOutcome.canceling


# ---------------------------------------------------------------------------
# Log access
# ---------------------------------------------------------------------------


async def _resolve_transfer_context(
    session: AsyncSession, transfer_id: UUID
) -> tuple[str, str, CacheMode, str]:
    transfer = await repo.get_transfer(session, transfer_id)
    if transfer is None:
        raise NotFound(f"transfer {transfer_id} not found")
    username = transfer.owner_username
    return transfer.experiment, transfer.run, CacheMode(transfer.cache_mode), username


async def read_transfer_log(
    session: AsyncSession,
    transfer_id: UUID,
    stream: lcore.LogStream,
    user: AuthenticatedUser,
    *,
    mode: lcore.LogReadMode = lcore.LogReadMode.tail,
    lines: int | None = None,
    bytes_: int | None = None,
) -> str:
    """Return the head/tail of a single log stream as decoded text."""
    exp, run, cache_mode, username = await _resolve_transfer_context(
        session, transfer_id
    )
    path = lcore.log_stream_path(
        stream,
        config.get_producer(),
        exp,
        run,
        transfer_id,
        username,
        cache_mode=cache_mode,
    )
    client = iri.client()
    try:
        if mode is lcore.LogReadMode.head:
            return await client.head(
                path, user.token.get_secret_value(), lines=lines, bytes_=bytes_
            )
        return await client.tail(
            path, user.token.get_secret_value(), lines=lines, bytes_=bytes_
        )
    except iri.IriAuthenticationError as exc:
        raise DelegatedCredentialRejected(str(exc)) from exc
    except iri.FilesystemError as exc:
        # TODO: stat collapses any failure (missing file or broken upstream) to
        # exists=False, so we still can't tell the two apart here.
        # so this is the right idea but it would be better to have a more specific
        # exception...
        stat = await client.stat(path, user.token.get_secret_value())
        if not stat.exists:
            raise NotFound(
                f"log {stream.value} not found for transfer {transfer_id}"
            ) from exc
        raise UpstreamError(
            f"failed to read {stream.value} log for transfer {transfer_id}: {exc}"
        ) from exc


async def list_transfer_logs(
    session: AsyncSession, transfer_id: UUID, user: AuthenticatedUser
) -> TransferLogIndex:
    """Index every log stream for a transfer with its resolved path and, when
    the file exists, its size and last-modified time."""
    exp, run, cache_mode, username = await _resolve_transfer_context(
        session, transfer_id
    )
    client = iri.client()
    paths = [
        (
            stream,
            lcore.log_stream_path(
                stream,
                config.get_producer(),
                exp,
                run,
                transfer_id,
                username,
                cache_mode=cache_mode,
            ),
        )
        for stream in lcore.LogStream
    ]
    try:
        stats = await asyncio.gather(
            *(client.stat(path, user.token.get_secret_value()) for _, path in paths)
        )
    except iri.IriAuthenticationError as exc:
        raise DelegatedCredentialRejected(str(exc)) from exc
    streams = [
        TransferLogStreamInfo(
            stream=stream,
            path=path,
            available=stat.exists,
            size=stat.size,
            modified_at=stat.modified_at,
        )
        for (stream, path), stat in zip(paths, stats, strict=True)
    ]
    return TransferLogIndex(transfer_id=transfer_id, streams=streams)


async def list_caches_for_experiment(
    session: AsyncSession, experiment: str
) -> CachesPublic:
    """The experiment's active cache, if any."""
    cache_id = await repo.find_active_cache(session, experiment)
    if cache_id is None:
        return CachesPublic(data=[])
    cache = await fastcache.client().get_cache(cache_id)
    if cache is None:
        return CachesPublic(data=[])
    return CachesPublic(data=[CacheStatusPublic(id=cache.id, state=cache.state)])


async def shutdown_cache(session: AsyncSession, cache_id: UUID) -> None:
    async with session.begin():
        cache = await repo.lock_active_cache(session, cache_id)
        if cache is None:
            raise NotFound(f"cache {cache_id} not found")
        count = await repo.count_active_transfers_by_cache(session, cache_id)
        if count > 0:
            raise CacheShutdownBlocked(count)

        # Keep the registry row locked across deletion. Provisioning either
        # attached before this count (and blocks shutdown), or waits and sees
        # retired_at after commit, so it cannot attach to a deleted cache.
        await fastcache.client().delete_cache(cache_id)
        await repo.retire_cache(session, cache, retired_at=datetime.now(UTC))

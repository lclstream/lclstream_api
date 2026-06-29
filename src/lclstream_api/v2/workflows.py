"""DBOS durable workflows for the transfer lifecycle (the imperative shell).

* ``provision_transfer`` — a saga that creates the cache then submits the
  producer. Each external call is an idempotent ``@DBOS.step``; on failure the
  completed steps are compensated in reverse order. It records ``provisioning``
  and exits; it does not poll.
* ``reconcile_transfers`` — a scheduled workflow that drives status after
  setup. It observes the cache and producer, runs the pure
  ``decide_state_with_timeout``, writes on change only, and tears down
  resources on terminal/cancel.

DB access goes through ``@db.transaction`` functions, recorded for exactly-once
execution inside a workflow. Each one injects the datasource-tx session and maps
the ORM row to a frozen pydantic model before returning. External IO goes
through ``@DBOS.step`` wrappers around the shell clients; pure decisions come
from ``core``.
"""

import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from amsc_iri.models import JobSpec, JobState
from dbos import DBOS
from pydantic import AwareDatetime

from ..lclstreamer_param import Parameters
from . import config, db, repo
from .clients.fastcache import FastcacheClient
from .clients.iri import IriClient
from .core import producer as pcore, transfer as tcore
from .models import TransferState, TransitionSource

logger = logging.getLogger(__name__)

# Statuses for which the reconciler should (idempotently) tear down resources.
_TEARDOWN_STATES = frozenset(
    {
        TransferState.failed,
        TransferState.completed,
        TransferState.canceling,
        TransferState.canceled,
    }
)


RECONCILE_SCHEDULE = "*/10 * * * * *"
MAX_PROVISIONING_AGE_S = 1800.0

_fastcache: FastcacheClient | None = None
_iri: IriClient | None = None


def startup() -> None:
    """Init the clients (call at app startup)."""
    global _fastcache, _iri
    _fastcache = FastcacheClient(config.fastcache)
    _iri = IriClient(config.iri)


async def shutdown() -> None:
    """Close the clients (call at app shutdown)."""
    global _fastcache, _iri
    if _fastcache is not None:
        await _fastcache.aclose()
    _fastcache = None
    _iri = None


def _fc() -> FastcacheClient:
    if _fastcache is None:
        raise RuntimeError("workflows not configured; call startup() at app startup")
    return _fastcache


def _iri_client() -> IriClient:
    if _iri is None:
        raise RuntimeError("workflows not configured; call startup() at app startup")
    return _iri


# ---------------------------------------------------------------------------
# External IO steps
# ---------------------------------------------------------------------------


@DBOS.step()
async def _create_cache(transfer_id: UUID, requested_by: str) -> tcore.CacheEndpoint:
    # TODO: not idempotent yet !
    cache = await _fc().create_cache(transfer_id, requested_by)
    return tcore.CacheEndpoint.from_uris(
        cache.id,
        cache.config.hostname,
        str(cache.config.pull_uri),
        str(cache.config.push_uri),
    )


@DBOS.step()
async def _submit_producer(jobspec: JobSpec) -> str:
    return await _iri_client().submit_job(jobspec)


# The upload/delete are quick (small YAML + a couple of polled IRI tasks), so
# retry fast on transient failures instead of failing the whole saga.
@DBOS.step(retries_allowed=True, interval_seconds=1.0, max_attempts=5, backoff_rate=2.0)
async def _upload_config(path: Path, content: str) -> None:
    await _iri_client().upload_file(path, content)


@DBOS.step(retries_allowed=True, interval_seconds=1.0, max_attempts=5, backoff_rate=2.0)
async def _delete_config(path: Path) -> None:
    await _iri_client().delete(path)


@DBOS.step()
async def _get_cache_state(cache_id: UUID) -> tcore.CacheState | None:
    cache = await _fc().get_cache(cache_id)
    return cache.state if cache is not None else None


@DBOS.step()
async def _get_producer_state(job_id: str) -> JobState | None:
    return await _iri_client().get_job(job_id)


@DBOS.step()
async def _delete_cache(cache_id: UUID) -> None:
    await _fc().delete_cache(cache_id)


@DBOS.step()
async def _cancel_producer(job_id: str) -> None:
    await _iri_client().cancel_job(job_id)


# ---------------------------------------------------------------------------
# Database transactions
# ---------------------------------------------------------------------------


@db.transaction()
async def _save_cache(transfer_id: UUID, endpoint: tcore.CacheEndpoint) -> None:
    await repo.set_cache_endpoints(
        db.sql_session(),
        transfer_id,
        cache_id=endpoint.cache_id,
        hostname=endpoint.hostname,
        pull_port=endpoint.pull_port,
        push_port=endpoint.push_port,
    )


@db.transaction()
async def _load_requested_by(transfer_id: UUID) -> str:
    transfer = await repo.get_transfer(db.sql_session(), transfer_id)
    if transfer is None:
        raise LookupError(f"transfer {transfer_id} disappeared during setup")
    return transfer.user


@db.transaction()
async def _save_producer(transfer_id: UUID, job_id: str) -> None:
    await repo.set_producer_job(db.sql_session(), transfer_id, job_id)


@db.transaction()
async def _record_state(
    transfer_id: UUID,
    state: TransferState,
    info: str | None,
    source: TransitionSource | None,
    observed_at: AwareDatetime | None = None,
) -> bool:
    return await repo.record_state(
        db.sql_session(),
        transfer_id,
        state,
        info=info,
        source=source,
        observed_at=observed_at,
    )


@db.transaction()
async def _load_producer_inputs(
    transfer_id: UUID, endpoint: tcore.CacheEndpoint
) -> tcore.ProducerInputs | None:
    transfer = await repo.get_transfer(db.sql_session(), transfer_id)
    if transfer is None:
        return None
    parameters = Parameters.model_validate(transfer.parameters)
    exp, run = pcore.parse_exp_run(parameters.source_identifier)
    if exp is None or run is None:
        raise LookupError(f"transfer {transfer_id} has no resolvable exp/run")
    return tcore.ProducerInputs(
        parameters=parameters, endpoint=endpoint, exp=exp, run=run
    )


@db.transaction()
async def _load_refs(transfer_id: UUID) -> tcore.TransferRefs | None:
    transfer = await repo.get_transfer(db.sql_session(), transfer_id)
    if transfer is None:
        return None
    return tcore.TransferRefs(
        state=TransferState(transfer.state),
        cache_id=transfer.cache_id,
        producer_job_id=transfer.producer_job_id,
        created_at=transfer.created_at,
    )


@db.transaction()
async def _list_unsettled() -> list[UUID]:
    return [
        transfer.id
        for transfer in await repo.list_unsettled_transfers(db.sql_session())
    ]


# ---------------------------------------------------------------------------
# Shell glue (no IO, but not core: reads runtime config / composes core)
# ---------------------------------------------------------------------------


async def _run_compensation(progress: tcore.ProvisionProgress) -> None:
    """Undo the completed setup steps using the resource refs captured in the
    workflow as each external step succeeded."""

    for comp in progress.compensation():
        match comp:
            case tcore.CancelProducer(job_id=job_id):
                await _cancel_producer(job_id)
            case tcore.DeleteConfig(config_path=config_path):
                await _delete_config(config_path)
            case tcore.DeleteCache(cache_id=cache_id):
                await _delete_cache(cache_id)


async def _teardown(refs: tcore.TransferRefs) -> None:
    """Idempotently release a transfer's resources."""

    if refs.producer_job_id:
        await _cancel_producer(refs.producer_job_id)
    if refs.cache_id:
        await _delete_cache(refs.cache_id)


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------


@DBOS.workflow()
async def provision_transfer(transfer_id: UUID) -> None:
    progress = tcore.ProvisionProgress()
    try:
        requested_by = await _load_requested_by(transfer_id)
        endpoint = await _create_cache(transfer_id, requested_by)
        progress = progress.with_cache(endpoint.cache_id)
        await _save_cache(transfer_id, endpoint)

        inputs = await _load_producer_inputs(transfer_id, endpoint)
        if inputs is None:
            raise LookupError(f"transfer {transfer_id} disappeared during setup")
        plan = pcore.plan_producer(inputs, config.producer, transfer_id)

        await _upload_config(plan.config_path, plan.config_yaml)
        progress = progress.with_config(plan.config_path)

        job_id = await _submit_producer(plan.jobspec)
        progress = progress.with_producer(job_id)
        await _save_producer(transfer_id, job_id)
    except Exception as exc:
        logger.exception("provisioning failed for transfer %s", transfer_id)
        await _run_compensation(progress)
        await _record_state(
            transfer_id,
            TransferState.failed,
            f"provisioning failed: {exc}",
            TransitionSource.orchestrator,
        )
        raise


async def _reconcile_one(transfer_id: UUID, now: AwareDatetime) -> None:
    refs = await _load_refs(transfer_id)
    if refs is None:
        return
    current = refs.state
    if current.is_final():
        return

    cache_state = await _get_cache_state(refs.cache_id) if refs.cache_id else None
    producer_state = (
        await _get_producer_state(refs.producer_job_id)
        if refs.producer_job_id
        else None
    )

    decision = tcore.decide_state_with_timeout(
        cache_state,
        producer_state,
        current,
        now=now,
        created_at=refs.created_at,
        max_provisioning_age_s=MAX_PROVISIONING_AGE_S,
    )

    await _record_state(
        transfer_id,
        decision.state,
        decision.reason,
        decision.source,
        observed_at=now,
    )

    if decision.state in _TEARDOWN_STATES:
        await _teardown(refs)


@DBOS.workflow()
async def reconcile_transfers(scheduled_time: AwareDatetime, context: Any) -> None:
    """Scheduled driver: advance every unsettled transfer one step."""

    for transfer_id in await _list_unsettled():
        try:
            await _reconcile_one(transfer_id, scheduled_time)
        except Exception:
            logger.exception("reconcile failed for transfer %s", transfer_id)


@DBOS.workflow()
async def reconcile_now(transfer_id: UUID, now: AwareDatetime) -> None:
    await _reconcile_one(transfer_id, now)


def register_schedules() -> None:
    DBOS.apply_schedules(
        [
            {
                "schedule_name": "reconcile-transfers",
                "workflow_fn": reconcile_transfers,
                "schedule": RECONCILE_SCHEDULE,
                "context": None,
            }
        ]
    )

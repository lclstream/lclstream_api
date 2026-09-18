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
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from amsc_iri.models import JobSpec, JobState
from dbos import DBOS
from pydantic import AwareDatetime

from ..lclstreamer_param import Parameters
from . import config, db, repo
from .clients import fastcache, iri
from .core import logs, producer as pcore, transfer as tcore
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


# ---------------------------------------------------------------------------
# External IO steps
# ---------------------------------------------------------------------------

DEFAULT_RETRY_SETTINGS: dict[str, Any] = {
    "retries_allowed": True,
    "interval_seconds": 1.0,
    "max_attempts": 5,
    "backoff_rate": 2.0,
}


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _create_cache(
    requested_by: str,
    cache_log_path: Path,
    key: str,
    idle_timeout_ms: int | None,
) -> tcore.CacheEndpoint:
    cache = await fastcache.client().create_cache(
        key=key,
        requested_by=requested_by,
        log_path=cache_log_path,
        idle_timeout_ms=idle_timeout_ms,
    )
    return tcore.CacheEndpoint.from_uris(
        cache.id,
        cache.config.hostname,
        str(cache.config.pull_uri),
        str(cache.config.push_uri),
    )


# TODO: this is not idempotent so we cannot retry
@DBOS.step()
async def _submit_producer(jobspec: JobSpec) -> str:
    return await iri.client().submit_job(jobspec)


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _upload_config(path: Path, content: str) -> None:
    await iri.client().upload_file(path, content)


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _delete_config(path: Path) -> None:
    await iri.client().delete(path)


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _delete_work_dir(work_dir: Path) -> None:
    # Provisioning rollback only: remove the whole per-transfer scratch dir
    # (config + any partial artifacts). Normal teardown leaves it intact.
    await iri.client().delete(work_dir)


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _get_cache_state(cache_id: UUID) -> tcore.CacheState | None:
    cache = await fastcache.client().get_cache(cache_id)
    return cache.state if cache is not None else None


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _get_producer_state(job_id: str) -> JobState | None:
    return await iri.client().get_job(job_id)


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _delete_cache(cache_id: UUID) -> None:
    await fastcache.client().delete_cache(cache_id)


@DBOS.step(**DEFAULT_RETRY_SETTINGS)
async def _cancel_producer(job_id: str) -> None:
    await iri.client().cancel_job(job_id)


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
async def _load_setup_inputs(transfer_id: UUID) -> tcore.TransferSetup:
    transfer = await repo.get_transfer(db.sql_session(), transfer_id)
    if transfer is None:
        raise LookupError(f"transfer {transfer_id} disappeared during setup")
    return tcore.TransferSetup(
        requested_by=transfer.user,
        exp=transfer.experiment,
        run=transfer.run,
        cache_mode=tcore.CacheMode(transfer.cache_mode),
    )


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
    return tcore.ProducerInputs(
        parameters=parameters,
        endpoint=endpoint,
        exp=transfer.experiment,
        run=transfer.run,
    )


@db.transaction()
async def _load_transfer_state(
    transfer_id: UUID,
) -> tuple[tcore.TransferSnapshot, tcore.TransferResourceRefs] | None:
    transfer = await repo.get_transfer(db.sql_session(), transfer_id)
    if transfer is None:
        return None
    snapshot = tcore.TransferSnapshot(
        state=TransferState(transfer.state),
        created_at=transfer.created_at,
        last_polled_at=transfer.last_polled_at,
    )
    resources = tcore.TransferResourceRefs(
        cache_id=transfer.cache_id,
        cache_mode=tcore.CacheMode(transfer.cache_mode),
        producer_job_id=transfer.producer_job_id,
    )
    return snapshot, resources


@db.transaction()
async def _list_unsettled() -> list[UUID]:
    return [
        transfer.id
        for transfer in await repo.list_unsettled_transfers(db.sql_session())
    ]


async def _compensate(comps: Iterable[tcore.Compensation]) -> None:
    """Run the undo actions for a transfer."""

    failures: list[Exception] = []
    for comp in comps:
        try:
            match comp:
                case tcore.CancelProducer(job_id=job_id):
                    await _cancel_producer(job_id)
                case tcore.DeleteConfig(config_path=config_path):
                    await _delete_config(config_path)
                case tcore.DeleteCache(cache_id=cache_id):
                    await _delete_cache(cache_id)
                case tcore.DeleteWorkDir(work_dir=work_dir):
                    await _delete_work_dir(work_dir)
        except Exception as failure:
            logger.exception("compensation step %r failed", comp)
            failures.append(failure)

    if failures:
        raise ExceptionGroup("compensation failed", failures)


async def _teardown(resources: tcore.TransferResourceRefs) -> None:
    """Reclaim the producer job and the cache.

    A shared cache is left alone -- other transfers may still push into it, so
    only an operator stops it.
    """

    comps: list[tcore.Compensation] = []
    if resources.producer_job_id:
        comps.append(tcore.CancelProducer(job_id=resources.producer_job_id))
    if resources.cache_id and resources.cache_mode is tcore.CacheMode.per_transfer:
        comps.append(tcore.DeleteCache(cache_id=resources.cache_id))
    await _compensate(comps)


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------


@DBOS.workflow()
async def provision_transfer(transfer_id: UUID) -> None:
    progress = tcore.ProvisionProgress()
    try:
        setup = await _load_setup_inputs(transfer_id)
        cache_log_path = logs.cache_log_path(
            config.get_producer(),
            setup.exp,
            setup.run,
            transfer_id,
            cache_mode=setup.cache_mode,
        )
        key = (
            setup.exp
            if setup.cache_mode is tcore.CacheMode.shared
            else str(transfer_id)
        )
        endpoint = await _create_cache(
            setup.requested_by,
            cache_log_path,
            key,
            pcore.cache_idle_timeout_ms(setup.cache_mode),
        )
        work_dir = pcore.transfer_work_dir(
            config.get_producer(), setup.exp, setup.run, transfer_id
        )
        # _create_cache succeeding means the work dir exists
        progress = progress.with_work_dir(work_dir).with_cache(
            endpoint.cache_id, mode=setup.cache_mode
        )
        await _save_cache(transfer_id, endpoint)

        inputs = await _load_producer_inputs(transfer_id, endpoint)
        if inputs is None:
            raise LookupError(f"transfer {transfer_id} disappeared during setup")
        plan = pcore.plan_producer(inputs, config.get_producer(), transfer_id)

        await _upload_config(plan.config_path, plan.config_yaml)
        progress = progress.with_config(plan.config_path)

        job_id = await _submit_producer(plan.jobspec)
        progress = progress.with_producer(job_id)
        await _save_producer(transfer_id, job_id)
    except Exception as exc:
        logger.exception("provisioning failed for transfer %s", transfer_id)

        # Release what we created before finalizing.
        await _compensate(progress.compensation())
        await _record_state(
            transfer_id,
            TransferState.failed,
            f"provisioning failed: {exc}",
            TransitionSource.orchestrator,
        )
        raise


async def _observe(
    transfer_id: UUID, resources: tcore.TransferResourceRefs
) -> tcore.TransferObservation:
    """Fetch live cache/producer state for one reconcile pass."""

    ok = True

    cache_state: tcore.CacheState | None = None
    if resources.cache_id:
        try:
            cache_state = await _get_cache_state(resources.cache_id)
        except httpx.HTTPError:
            logger.exception("failed to fetch cache state for transfer %s", transfer_id)
            ok = False

    producer_state: JobState | None = None
    if resources.producer_job_id:
        try:
            producer_state = await _get_producer_state(resources.producer_job_id)
        except Exception:
            logger.exception(
                "failed to fetch producer state for transfer %s", transfer_id
            )
            ok = False

    return tcore.TransferObservation(
        cache_state=cache_state, producer_state=producer_state, ok=ok
    )


async def _reconcile_one(transfer_id: UUID, now: AwareDatetime) -> None:
    loaded = await _load_transfer_state(transfer_id)
    if loaded is None:
        return
    snapshot, resources = loaded
    if snapshot.state.is_final():
        return

    observation = await _observe(transfer_id, resources)
    decision = tcore.decide_state_with_timeout(observation, snapshot, now=now)

    # Only a pass that actually observed fresh upstream state advances the
    # staleness watermark...
    observed_at = now if observation.ok else None

    # Tear down before persisting a final state: next reconcile pass retries it
    # instead of leaking the cache/producer forever.
    if decision.state in _TEARDOWN_STATES:
        await _teardown(resources)

    await _record_state(
        transfer_id,
        decision.state,
        decision.reason,
        decision.source,
        observed_at=observed_at,
    )


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

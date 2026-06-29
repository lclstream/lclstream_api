from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlparse
from uuid import UUID

from amsc_iri.models import JobState
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ...lclstreamer_param import Parameters
from ..models import TransferState, TransitionSource


class CacheState(StrEnum):
    """Observed fastcache cache lifecycle."""

    new = "new"
    queued = "queued"
    active = "active"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"


# Cache states that mean the cache is no longer running.
_CACHE_TERMINAL = frozenset(
    {CacheState.completed, CacheState.failed, CacheState.canceled}
)
# Cache states that mean the cache crashed / was killed (not a clean drain).
_CACHE_FAILED = frozenset({CacheState.failed, CacheState.canceled})

# Producer states that mean the IRI job is no longer running.
_PRODUCER_TERMINAL = frozenset({JobState.COMPLETED, JobState.FAILED, JobState.CANCELED})


def _cache_torn_down(cache_state: CacheState | None) -> bool:
    return cache_state is None or cache_state in _CACHE_TERMINAL


def _producer_torn_down(producer_state: JobState | None) -> bool:
    return producer_state is None or producer_state in _PRODUCER_TERMINAL


def _decide_state(
    cache_state: CacheState | None,
    producer_state: JobState | None,
    current: TransferState,
) -> tuple[TransferState, TransitionSource]:
    """Live observation of services -> (transfer state, responsible side)."""

    # 1. A user cancel is in progress: hold until both sides are confirmed
    #    torn down, then settle to canceled.
    if current == TransferState.canceling:
        if _cache_torn_down(cache_state) and _producer_torn_down(producer_state):
            return TransferState.canceled, TransitionSource.orchestrator
        return TransferState.canceling, TransitionSource.orchestrator

    # 2. Cache crash.
    if cache_state in _CACHE_FAILED:
        return TransferState.failed, TransitionSource.cache

    # 3-4. Producer is the authority for failure. External IRI cancel
    #    (preempt/walltime) surfaces as failed; ``canceled`` is reserved for
    #    user-initiated cancels.
    if producer_state in (JobState.FAILED, JobState.CANCELED):
        return TransferState.failed, TransitionSource.producer

    # 5. Success: producer finished AND the cache drained.
    if producer_state == JobState.COMPLETED and cache_state == CacheState.completed:
        return TransferState.completed, TransitionSource.orchestrator

    # 6. Producer done but cache still draining to the consumer -> stay ready.
    if producer_state == JobState.COMPLETED:
        return TransferState.ready, TransitionSource.orchestrator

    # 7. Steady state: both running.
    if producer_state == JobState.ACTIVE and cache_state == CacheState.active:
        return TransferState.ready, TransitionSource.orchestrator

    # 8. Once ready, the only exits are terminal (handled
    #    above) or canceling, so a idle-timeout or a lagging
    #    notification can't regress it to provisioning.
    if current == TransferState.ready:
        return TransferState.ready, TransitionSource.orchestrator

    # 9. Still spinning up.
    return TransferState.provisioning, TransitionSource.orchestrator


def _age_exceeded(
    now: AwareDatetime, created: AwareDatetime, max_seconds: float
) -> bool:
    """Whether ``now - created`` exceeds ``max_seconds``."""

    return (now - created).total_seconds() > max_seconds


def is_stale_observation(
    observed_at: AwareDatetime | None, last_polled_at: AwareDatetime | None
) -> bool:
    if observed_at is None or last_polled_at is None:
        return False
    return observed_at < last_polled_at


_LEGAL_TRANSITIONS: dict[TransferState, frozenset[TransferState]] = {
    TransferState.provisioning: frozenset(
        {
            TransferState.ready,
            TransferState.completed,
            TransferState.failed,
            TransferState.canceling,
        }
    ),
    TransferState.ready: frozenset(
        {
            TransferState.completed,
            TransferState.failed,
            TransferState.canceling,
        }
    ),
    TransferState.canceling: frozenset({TransferState.canceled}),
    # Final states (canceled/completed/failed) are omitted: they have no
    # out-edges, so can_transition's .get default returns False for them.
    # TransferState.is_final() is the single source of truth for finality.
}


def can_transition(current: TransferState, target: TransferState) -> bool:
    return target in _LEGAL_TRANSITIONS.get(current, frozenset())


class StateDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: TransferState
    source: TransitionSource
    reason: str | None = None


def decide_state_with_timeout(
    cache_state: CacheState | None,
    producer_state: JobState | None,
    current: TransferState,
    *,
    now: AwareDatetime,
    created_at: AwareDatetime,
    max_provisioning_age_s: float,
) -> StateDecision:
    """Decide the state, failing transfers stuck in provisioning too long.

    A transfer that never leaves ``provisioning`` past ``max_provisioning_age_s``
    is forced to ``failed``.
    """

    state, source = _decide_state(cache_state, producer_state, current)
    if (
        current == TransferState.provisioning
        and state == TransferState.provisioning
        and _age_exceeded(now, created_at, max_provisioning_age_s)
    ):
        return StateDecision(
            state=TransferState.failed,
            source=TransitionSource.orchestrator,
            reason="exceeded max provisioning age",
        )
    return StateDecision(state=state, source=source)


class CancelProducer(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["cancel_producer"] = "cancel_producer"
    job_id: str


class DeleteConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["delete_config"] = "delete_config"
    config_path: Path


class DeleteCache(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["delete_cache"] = "delete_cache"
    cache_id: UUID


# Each variant carries exactly the ref its undo needs; the shell matches on type.
Compensation = Annotated[
    CancelProducer | DeleteConfig | DeleteCache,
    Field(discriminator="kind"),
]


class ProvisionProgress(BaseModel):
    """Ledger for the provisioning workflow."""

    model_config = ConfigDict(frozen=True)

    steps: tuple[Compensation, ...] = ()

    # New instances are returned, since we want immutability baked in.
    def with_cache(self, cache_id: UUID) -> ProvisionProgress:
        return ProvisionProgress(steps=(*self.steps, DeleteCache(cache_id=cache_id)))

    def with_config(self, config_path: Path) -> ProvisionProgress:
        return ProvisionProgress(
            steps=(*self.steps, DeleteConfig(config_path=config_path))
        )

    def with_producer(self, job_id: str) -> ProvisionProgress:
        return ProvisionProgress(steps=(*self.steps, CancelProducer(job_id=job_id)))

    def compensation(self) -> tuple[Compensation, ...]:
        """The recorded undo actions, in reverse (compensation) order."""
        return tuple(reversed(self.steps))


class CacheEndpoint(BaseModel):
    """The transfer-domain projection of a freshly created cache.

    The workflow step pulls the raw fields off the fastcache ``CachePublic``
    and calls :meth:`from_uris`, so ``repo``/``db`` never see a client model.
    The ZMQ ports are parsed out of the cache's pull/push URIs.
    """

    model_config = ConfigDict(frozen=True)

    cache_id: UUID
    hostname: str
    pull_port: int
    push_port: int

    @property
    def pull_uri(self) -> str:
        """The ZMQ socket the producer pushes its stream into."""
        return f"tcp://{self.hostname}:{self.pull_port}"

    @classmethod
    def from_uris(
        cls, cache_id: UUID, hostname: str, pull_uri: str, push_uri: str
    ) -> CacheEndpoint:
        pull_port = urlparse(pull_uri).port
        push_port = urlparse(push_uri).port
        if pull_port is None or push_port is None:
            raise ValueError(f"cache {cache_id} returned config without ports")
        return cls(
            cache_id=cache_id,
            hostname=hostname,
            pull_port=pull_port,
            push_port=push_port,
        )


class TransferRefs(BaseModel):
    """Reconciler/teardown snapshot."""

    model_config = ConfigDict(frozen=True)

    state: TransferState
    cache_id: UUID | None
    producer_job_id: str | None
    created_at: AwareDatetime


class ProducerInputs(BaseModel):
    """Everything the producer builder needs to render a jobspec."""

    model_config = ConfigDict(frozen=True)

    parameters: Parameters
    # The allocated cache the producer pushes to (hostname + ZMQ ports).
    endpoint: CacheEndpoint
    # Resolved at request time (body override or parsed source_identifier);
    # used to place the per-transfer job directory and the account.
    exp: str
    run: str

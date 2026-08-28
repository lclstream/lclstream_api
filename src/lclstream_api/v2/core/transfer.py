from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlparse
from uuid import UUID

from amsc_iri.models import JobSpec, JobState
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ...lclstreamer_param import Parameters
from .enums import (
    CacheMode,
    CacheState,
    ConsumerSocket,
    TransferState,
    TransitionSource,
)

_CACHE_TERMINAL = frozenset(
    {CacheState.completed, CacheState.failed, CacheState.canceled}
)
# Crashed or killed, not a clean drain.
_CACHE_FAILED = frozenset({CacheState.failed, CacheState.canceled})

_PRODUCER_TERMINAL = frozenset({JobState.COMPLETED, JobState.FAILED, JobState.CANCELED})


def _cache_torn_down(cache_state: CacheState | None) -> bool:
    return cache_state is None or cache_state in _CACHE_TERMINAL


def _producer_torn_down(producer_state: JobState | None) -> bool:
    return producer_state is None or producer_state in _PRODUCER_TERMINAL


def _decide_state(
    observation: TransferObservation,
    current: TransferState,
) -> tuple[TransferState, TransitionSource]:
    """Live observation of services -> (transfer state, responsible side)."""

    cache_state = observation.cache_state
    producer_state = observation.producer_state

    if current == TransferState.canceling:
        if _cache_torn_down(cache_state) and _producer_torn_down(producer_state):
            return TransferState.canceled, TransitionSource.orchestrator
        return TransferState.canceling, TransitionSource.orchestrator

    if cache_state in _CACHE_FAILED:
        return TransferState.failed, TransitionSource.cache

    # IRI-side cancel means failed; canceled is user-initiated.
    if producer_state in (JobState.FAILED, JobState.CANCELED):
        return TransferState.failed, TransitionSource.producer

    if producer_state == JobState.COMPLETED and cache_state == CacheState.completed:
        return TransferState.completed, TransitionSource.orchestrator

    # Cache still draining to the consumer.
    if producer_state == JobState.COMPLETED:
        return TransferState.ready, TransitionSource.orchestrator

    if producer_state == JobState.ACTIVE and cache_state == CacheState.active:
        return TransferState.ready, TransitionSource.orchestrator

    # Ready never regresses on a lagging notification.
    if current == TransferState.ready:
        return TransferState.ready, TransitionSource.orchestrator

    return TransferState.provisioning, TransitionSource.orchestrator


def _age_exceeded(
    now: AwareDatetime, created: AwareDatetime, max_seconds: float
) -> bool:
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
    TransferState.canceling: frozenset({TransferState.canceled, TransferState.failed}),
    # Final states omitted: they have no out-edges.
}


def can_transition(current: TransferState, target: TransferState) -> bool:
    return target in _LEGAL_TRANSITIONS.get(current, frozenset())


class StateDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: TransferState
    source: TransitionSource
    reason: str | None = None


# Stuck past this never reached `ready`.
DEFAULT_MAX_PROVISIONING_AGE_S = 1800.0
# Unobserved this long: fail instead of wedging.
DEFAULT_MAX_READY_STALE_S = 1800.0


def decide_state_with_timeout(
    observation: TransferObservation,
    snapshot: TransferSnapshot,
    *,
    now: AwareDatetime,
    max_provisioning_age_s: float = DEFAULT_MAX_PROVISIONING_AGE_S,
    max_ready_stale_s: float = DEFAULT_MAX_READY_STALE_S,
) -> StateDecision:
    """Decide the state, forcing stuck transfers to ``failed``.

    Fail-safes apply only when the state does not advance, so a
    progressing or already-final transfer is never touched.
    """

    current = snapshot.state
    state, source = _decide_state(observation, current)

    if state == current == TransferState.provisioning and _age_exceeded(
        now, snapshot.created_at, max_provisioning_age_s
    ):
        return StateDecision(
            state=TransferState.failed,
            source=TransitionSource.orchestrator,
            reason="exceeded max provisioning age",
        )

    if (
        state == current == TransferState.ready
        and snapshot.last_polled_at is not None
        and _age_exceeded(now, snapshot.last_polled_at, max_ready_stale_s)
    ):
        return StateDecision(
            state=TransferState.failed,
            source=TransitionSource.orchestrator,
            reason="ready state stale for too long without a successful observation",
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


class DeleteWorkDir(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["delete_work_dir"] = "delete_work_dir"
    work_dir: Path


class DeleteCache(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal["delete_cache"] = "delete_cache"
    cache_id: UUID


Compensation = Annotated[
    CancelProducer | DeleteConfig | DeleteWorkDir | DeleteCache,
    Field(discriminator="kind"),
]


class ProvisionProgress(BaseModel):
    """Ledger for the provisioning workflow."""

    model_config = ConfigDict(frozen=True)

    steps: tuple[Compensation, ...] = ()

    def with_cache(self, cache_id: UUID, *, mode: CacheMode) -> ProvisionProgress:
        if mode is not CacheMode.per_transfer:
            return self
        return ProvisionProgress(steps=(*self.steps, DeleteCache(cache_id=cache_id)))

    def with_config(self, config_path: Path) -> ProvisionProgress:
        return ProvisionProgress(
            steps=(*self.steps, DeleteConfig(config_path=config_path))
        )

    def with_work_dir(self, work_dir: Path) -> ProvisionProgress:
        return ProvisionProgress(steps=(*self.steps, DeleteWorkDir(work_dir=work_dir)))

    def with_producer(self, job_id: str) -> ProvisionProgress:
        return ProvisionProgress(steps=(*self.steps, CancelProducer(job_id=job_id)))

    def compensation(self) -> tuple[Compensation, ...]:
        """The recorded undo actions, in reverse (compensation) order."""
        return tuple(reversed(self.steps))


class CacheEndpoint(BaseModel):
    """Transfer-domain view of an allocated cache."""

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


class TransferSnapshot(BaseModel):
    """Point-in-time read of a transfer's lifecycle state."""

    model_config = ConfigDict(frozen=True)

    state: TransferState
    created_at: AwareDatetime
    last_polled_at: AwareDatetime | None


class Principal(BaseModel):
    """OIDC identity (issuer, subject) that owns a delegated credential."""

    model_config = ConfigDict(frozen=True)

    issuer: str
    subject: str


class TransferResourceRefs(BaseModel):
    """External resource ids for a transfer, for teardown and
    live-state fetches."""

    model_config = ConfigDict(frozen=True)

    owner: Principal
    cache_id: UUID | None
    cache_mode: CacheMode
    producer_job_id: str | None


class TransferSetup(BaseModel):
    """Inputs provisioning needs before it starts."""

    model_config = ConfigDict(frozen=True)

    owner: Principal
    requested_by: str
    username: str
    exp: str
    run: str
    cache_mode: CacheMode
    consumer_socket: ConsumerSocket


class TransferObservation(BaseModel):
    """Live cache/producer state fetched for one reconcile pass."""

    model_config = ConfigDict(frozen=True)

    cache_state: CacheState | None
    producer_state: JobState | None
    ok: bool = True  # false if the state fetch failed


class ProducerInputs(BaseModel):
    """Everything the producer builder needs to render a jobspec."""

    model_config = ConfigDict(frozen=True)

    parameters: Parameters
    endpoint: CacheEndpoint
    # From a body override or the parsed source_identifier.
    exp: str
    run: str
    job_spec: JobSpec

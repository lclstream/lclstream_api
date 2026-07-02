"""Transfer state machine core tests."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from amsc_iri.models import JobState

from lclstream_api.v2.core.transfer import (
    CacheEndpoint,
    CacheState,
    CancelProducer,
    DeleteCache,
    DeleteConfig,
    ProvisionProgress,
    can_transition,
    decide_state_with_timeout,
    is_stale_observation,
)
from lclstream_api.v2.models import TransferState, TransitionSource

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
# Large enough that the provisioning-age fail-safe never fires unless a test
# deliberately shrinks it.
NEVER_TIMEOUT = 10_000.0


# ---------------------------------------------------------------------------
# decide_state_with_timeout: the live-observation -> (state, source) matrix
# ---------------------------------------------------------------------------


def check_decision(
    cache: CacheState | None,
    producer: JobState | None,
    current: TransferState,
    *,
    expected_state: TransferState,
    expected_source: TransitionSource,
) -> None:
    """Route a single observation through the decider and assert the outcome.

    Created-at is pinned to ``NOW`` and the age limit is huge, so this isolates
    the pure decision matrix from the timeout fail-safe (tested separately).
    """

    decision = decide_state_with_timeout(
        cache,
        producer,
        current,
        now=NOW,
        created_at=NOW,
        max_provisioning_age_s=NEVER_TIMEOUT,
    )
    assert decision.state is expected_state
    assert decision.source is expected_source


# (id, cache_state, producer_state, current) -> (state, source)
_DECISION_CASES = [
    # 1. User cancel in flight, both sides confirmed torn down -> settle.
    (
        "canceling-both-down-settles",
        None,
        JobState.COMPLETED,
        TransferState.canceling,
        TransferState.canceled,
        TransitionSource.orchestrator,
    ),
    (
        "canceling-both-terminal-settles",
        CacheState.completed,
        JobState.CANCELED,
        TransferState.canceling,
        TransferState.canceled,
        TransitionSource.orchestrator,
    ),
    # 1b. Cancel in flight but a side is still running -> hold in canceling.
    (
        "canceling-producer-still-active-holds",
        CacheState.active,
        JobState.ACTIVE,
        TransferState.canceling,
        TransferState.canceling,
        TransitionSource.orchestrator,
    ),
    (
        "canceling-cache-down-producer-up-holds",
        None,
        JobState.ACTIVE,
        TransferState.canceling,
        TransferState.canceling,
        TransitionSource.orchestrator,
    ),
    # 2. Cache crash is attributed to the cache.
    (
        "cache-failed-blames-cache",
        CacheState.failed,
        JobState.ACTIVE,
        TransferState.provisioning,
        TransferState.failed,
        TransitionSource.cache,
    ),
    (
        "cache-canceled-blames-cache",
        CacheState.canceled,
        JobState.ACTIVE,
        TransferState.ready,
        TransferState.failed,
        TransitionSource.cache,
    ),
    # 3. Producer failure (incl. external IRI cancel) is attributed to producer.
    (
        "producer-failed-blames-producer",
        CacheState.active,
        JobState.FAILED,
        TransferState.ready,
        TransferState.failed,
        TransitionSource.producer,
    ),
    (
        "producer-canceled-blames-producer",
        CacheState.active,
        JobState.CANCELED,
        TransferState.provisioning,
        TransferState.failed,
        TransitionSource.producer,
    ),
    # 4. Clean success: producer finished AND cache drained.
    (
        "producer-done-cache-drained-completes",
        CacheState.completed,
        JobState.COMPLETED,
        TransferState.ready,
        TransferState.completed,
        TransitionSource.orchestrator,
    ),
    # 5. Producer done but cache still draining -> stay ready.
    (
        "producer-done-cache-draining-stays-ready",
        CacheState.active,
        JobState.COMPLETED,
        TransferState.provisioning,
        TransferState.ready,
        TransitionSource.orchestrator,
    ),
    # 6. Steady state: both active -> ready.
    (
        "both-active-is-ready",
        CacheState.active,
        JobState.ACTIVE,
        TransferState.provisioning,
        TransferState.ready,
        TransitionSource.orchestrator,
    ),
    # 7. Readiness is sticky: a lagging poll can't regress ready -> provisioning.
    (
        "ready-is-sticky-on-stale-poll",
        CacheState.new,
        JobState.NEW,
        TransferState.ready,
        TransferState.ready,
        TransitionSource.orchestrator,
    ),
    # 8. Still spinning up.
    (
        "spinning-up-stays-provisioning",
        CacheState.queued,
        JobState.QUEUED,
        TransferState.provisioning,
        TransferState.provisioning,
        TransitionSource.orchestrator,
    ),
    (
        "no-observations-yet-provisioning",
        None,
        None,
        TransferState.provisioning,
        TransferState.provisioning,
        TransitionSource.orchestrator,
    ),
]


@pytest.mark.parametrize(
    ("cache", "producer", "current", "expected_state", "expected_source"),
    [case[1:] for case in _DECISION_CASES],
    ids=[case[0] for case in _DECISION_CASES],
)
def test_decision_matrix(
    cache: CacheState | None,
    producer: JobState | None,
    current: TransferState,
    expected_state: TransferState,
    expected_source: TransitionSource,
) -> None:
    check_decision(
        cache,
        producer,
        current,
        expected_state=expected_state,
        expected_source=expected_source,
    )


def test_cancel_takes_priority_over_a_cache_crash() -> None:
    """A user cancel in flight outranks a cache failure: we settle to
    ``canceled`` (user intent), not ``failed``."""

    check_decision(
        CacheState.failed,
        None,
        TransferState.canceling,
        expected_state=TransferState.canceled,
        expected_source=TransitionSource.orchestrator,
    )


# ---------------------------------------------------------------------------
# Provisioning-age fail-safe (the orchestrator's own watchdog)
# ---------------------------------------------------------------------------


def test_provisioning_timeout_forces_failure() -> None:
    decision = decide_state_with_timeout(
        CacheState.queued,
        JobState.QUEUED,
        TransferState.provisioning,
        now=NOW + timedelta(seconds=61),
        created_at=NOW,
        max_provisioning_age_s=60.0,
    )
    assert decision.state is TransferState.failed
    assert decision.source is TransitionSource.orchestrator
    assert decision.reason == "exceeded max provisioning age"


def test_provisioning_within_age_is_not_failed() -> None:
    decision = decide_state_with_timeout(
        CacheState.queued,
        JobState.QUEUED,
        TransferState.provisioning,
        now=NOW + timedelta(seconds=59),
        created_at=NOW,
        max_provisioning_age_s=60.0,
    )
    assert decision.state is TransferState.provisioning
    assert decision.reason is None


def test_timeout_does_not_fire_once_ready() -> None:
    """The watchdog only fires while *still* provisioning; a transfer that has
    advanced is immune even past the age limit."""

    decision = decide_state_with_timeout(
        CacheState.active,
        JobState.ACTIVE,
        TransferState.ready,
        now=NOW + timedelta(seconds=10_000),
        created_at=NOW,
        max_provisioning_age_s=60.0,
    )
    assert decision.state is TransferState.ready


# ---------------------------------------------------------------------------
# Legal transition graph
# ---------------------------------------------------------------------------

# Every (current, target) pair the graph permits. Anything not listed must be
# rejected, which keeps this table honest against silent graph widening.
_LEGAL_PAIRS = {
    (TransferState.provisioning, TransferState.ready),
    (TransferState.provisioning, TransferState.completed),
    (TransferState.provisioning, TransferState.failed),
    (TransferState.provisioning, TransferState.canceling),
    (TransferState.ready, TransferState.completed),
    (TransferState.ready, TransferState.failed),
    (TransferState.ready, TransferState.canceling),
    (TransferState.canceling, TransferState.canceled),
}


@pytest.mark.parametrize("current", list(TransferState))
@pytest.mark.parametrize("target", list(TransferState))
def test_transition_graph_is_exactly_the_allowed_set(
    current: TransferState, target: TransferState
) -> None:
    assert can_transition(current, target) is ((current, target) in _LEGAL_PAIRS)


def test_terminal_states_are_dead_ends() -> None:
    for terminal in (
        TransferState.completed,
        TransferState.failed,
        TransferState.canceled,
    ):
        for target in TransferState:
            assert can_transition(terminal, target) is False


# ---------------------------------------------------------------------------
# Stale-observation guard
# ---------------------------------------------------------------------------


def test_observation_older_than_last_poll_is_stale() -> None:
    assert is_stale_observation(NOW, NOW + timedelta(seconds=1)) is True


def test_observation_newer_than_last_poll_is_fresh() -> None:
    assert is_stale_observation(NOW + timedelta(seconds=1), NOW) is False


def test_missing_timestamps_are_never_stale() -> None:
    assert is_stale_observation(None, NOW) is False
    assert is_stale_observation(NOW, None) is False


# ---------------------------------------------------------------------------
# CacheEndpoint.from_uris: ZMQ port parsing
# ---------------------------------------------------------------------------

_CACHE_ID = UUID("00000000-0000-0000-0000-0000000000aa")


def test_cache_endpoint_parses_ports_from_uris() -> None:
    endpoint = CacheEndpoint.from_uris(
        _CACHE_ID,
        "drp-host",
        pull_uri="tcp://drp-host:5001",
        push_uri="tcp://drp-host:5002",
    )
    assert endpoint == CacheEndpoint(
        cache_id=_CACHE_ID,
        hostname="drp-host",
        pull_port=5001,
        push_port=5002,
    )


@pytest.mark.parametrize(
    ("pull_uri", "push_uri"),
    [
        ("tcp://drp-host", "tcp://drp-host:5002"),
        ("tcp://drp-host:5001", "tcp://drp-host"),
    ],
    ids=["pull-missing-port", "push-missing-port"],
)
def test_cache_endpoint_rejects_uri_without_port(pull_uri: str, push_uri: str) -> None:
    with pytest.raises(ValueError, match="without ports"):
        CacheEndpoint.from_uris(_CACHE_ID, "drp-host", pull_uri, push_uri)


# ---------------------------------------------------------------------------
# ProvisionProgress: saga ledger accumulation -> compensation plan
# ---------------------------------------------------------------------------


def test_provision_progress_starts_empty() -> None:
    progress = ProvisionProgress()
    assert progress.steps == ()
    assert progress.compensation() == ()


def test_provision_progress_accumulates_steps_in_reverse() -> None:
    cache_id = uuid4()
    config_path = Path("/scratch/lclstreamer/lclstreamer.yaml")

    progress = (
        ProvisionProgress()
        .with_cache(cache_id)
        .with_config(config_path)
        .with_producer("job-1")
    )
    assert progress.compensation() == (
        CancelProducer(job_id="job-1"),
        DeleteConfig(config_path=config_path),
        DeleteCache(cache_id=cache_id),
    )


def test_provision_progress_compensates_partial_failure() -> None:
    """A crash after the cache exists but before the producer is submitted only
    undoes what was recorded."""

    cache_id = uuid4()
    config_path = Path("/x.yaml")
    progress = ProvisionProgress().with_cache(cache_id).with_config(config_path)
    assert progress.compensation() == (
        DeleteConfig(config_path=config_path),
        DeleteCache(cache_id=cache_id),
    )


def test_provision_progress_is_immutable() -> None:
    base = ProvisionProgress()
    base.with_cache(uuid4())
    assert base.steps == ()

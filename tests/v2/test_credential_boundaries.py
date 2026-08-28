"""Security boundaries for delegated credentials and durable execution."""

from collections.abc import Mapping
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from inspect import signature
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import ANY, AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from lclstream_api.v2 import service, workflows
from lclstream_api.v2.auth import AuthenticatedUser
from lclstream_api.v2.clients.fastcache import FastcacheClient
from lclstream_api.v2.core import logs as lcore, producer as pcore, transfer as tcore
from lclstream_api.v2.exceptions import (
    CacheShutdownBlocked,
    InsufficientTokenLifetime,
    NotFound,
)
from lclstream_api.v2.models import CacheMode, TransferCancelOutcome, TransferState

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
TRANSFER_ID = UUID("00000000-0000-0000-0000-000000000042")
RAW_TOKEN = "request-only-secret-token"


def _user(
    *,
    issuer: str = "https://issuer.example",
    subject: str = "subject-1",
    email: str = "user@example.org",
    username: str = "user",
    token: str = RAW_TOKEN,
) -> AuthenticatedUser:
    return AuthenticatedUser(
        issuer=issuer,
        subject=subject,
        email=email,
        username=username,
        token=token,
        expires_at=datetime.now(UTC) + timedelta(hours=12),
    )


def _assert_not_durable(value: object) -> None:
    """Reject request credentials anywhere in a captured DBOS invocation."""

    if isinstance(value, (tuple, list)):
        for item in value:
            _assert_not_durable(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_not_durable(key)
            _assert_not_durable(item)
        return
    assert not isinstance(value, AuthenticatedUser)
    assert value != RAW_TOKEN


def _await_args(mock: AsyncMock) -> tuple[tuple[Any, ...], Mapping[str, Any]]:
    """Return an awaited mock's (args, kwargs), narrowed from Optional."""
    call = mock.await_args
    assert call is not None
    return call.args, call.kwargs


@pytest.fixture
def io(monkeypatch: pytest.MonkeyPatch, make_producer_settings) -> SimpleNamespace:
    """Pre-patch every module-level collaborator `service.py`/`workflows.py`
    reach through. Tests set only the handles they care about."""
    mocks = SimpleNamespace(
        insert_transfer=AsyncMock(),
        get_owned_transfer=AsyncMock(return_value=None),
        get_transfer=AsyncMock(return_value=None),
        record_state=AsyncMock(return_value=True),
        lock_active_cache=AsyncMock(return_value=None),
        count_active_transfers_by_cache=AsyncMock(return_value=0),
        retire_cache=AsyncMock(),
        start_workflow_async=AsyncMock(),
        cancel_job=AsyncMock(),
        tail=AsyncMock(),
        submit_job=AsyncMock(),
        delete_cache=AsyncMock(),
        token_for=AsyncMock(return_value=RAW_TOKEN),
    )
    for name in (
        "insert_transfer",
        "get_owned_transfer",
        "get_transfer",
        "record_state",
        "lock_active_cache",
        "count_active_transfers_by_cache",
        "retire_cache",
    ):
        monkeypatch.setattr(service.repo, name, getattr(mocks, name))
    monkeypatch.setattr(service, "uuid4", lambda: TRANSFER_ID)
    monkeypatch.setattr(service, "SetWorkflowID", lambda _: nullcontext())
    monkeypatch.setattr(service.config, "get_producer", make_producer_settings)
    monkeypatch.setattr(
        service.config,
        "get_credentials",
        lambda: SimpleNamespace(lifecycle_grace_seconds=0),
    )
    monkeypatch.setattr(
        service.DBOS, "start_workflow_async", mocks.start_workflow_async
    )
    # service.iri and workflows.iri are the same module.
    monkeypatch.setattr(
        service.iri,
        "client",
        lambda: SimpleNamespace(
            cancel_job=mocks.cancel_job,
            tail=mocks.tail,
            submit_job=mocks.submit_job,
        ),
    )
    monkeypatch.setattr(
        service.fastcache,
        "client",
        lambda: SimpleNamespace(delete_cache=mocks.delete_cache),
    )
    monkeypatch.setattr(workflows, "_token_for", mocks.token_for)
    return mocks


@pytest.mark.asyncio
async def test_transfer_creation_starts_workflow_without_request_credential(
    io: SimpleNamespace, fake_session: AsyncSession, make_params
) -> None:
    user = _user()

    async def insert_transfer(_session, **values):
        return SimpleNamespace(
            id=values["transfer_id"],
            created_at=NOW,
            owner_email=values["owner_email"],
            state=TransferState.provisioning,
            experiment=values["experiment"],
            run=values["run"],
            cache_mode=values["cache_mode"],
            cache_hostname=None,
            push_port=None,
        )

    io.insert_transfer.side_effect = insert_transfer

    await service.create_transfer(
        fake_session, user, make_params(), experiment="mfxl1001", run="42"
    )

    io.start_workflow_async.assert_awaited_once_with(
        workflows.provision_transfer, TRANSFER_ID
    )
    args, kwargs = _await_args(io.start_workflow_async)
    _assert_not_durable(args)
    _assert_not_durable(kwargs)


@pytest.mark.asyncio
async def test_transfer_creation_rejects_short_lived_token_before_persistence(
    io: SimpleNamespace, fake_session: AsyncSession, make_params
) -> None:
    user = AuthenticatedUser(
        issuer="https://issuer.example",
        subject="subject-1",
        email="user@example.org",
        username="user",
        token=RAW_TOKEN,
        expires_at=datetime.now(UTC) + timedelta(seconds=100),
    )

    with pytest.raises(InsufficientTokenLifetime) as exc_info:
        await service.create_transfer(
            fake_session, user, make_params(), experiment="mfxl1001", run="42"
        )

    assert exc_info.value.required_seconds == 3600
    assert exc_info.value.remaining_seconds <= 100
    io.start_workflow_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_owner_cancel_cannot_reuse_owner_credential(
    io: SimpleNamespace, fake_session: AsyncSession
) -> None:
    caller = _user(subject="intruder")

    with pytest.raises(NotFound):
        await service.cancel_transfer(fake_session, TRANSFER_ID, caller)

    io.get_owned_transfer.assert_awaited_once_with(
        ANY, TRANSFER_ID, owner_issuer=caller.issuer, owner_subject=caller.subject
    )
    io.cancel_job.assert_not_awaited()
    io.start_workflow_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_owner_cancel_uses_request_token_but_not_in_workflow_args(
    io: SimpleNamespace, fake_session: AsyncSession
) -> None:
    owner = _user()
    io.get_owned_transfer.return_value = SimpleNamespace(
        state=TransferState.ready,
        producer_job_id="producer-42",
        cache_id=None,
        cache_mode=CacheMode.per_transfer,
    )

    outcome = await service.cancel_transfer(fake_session, TRANSFER_ID, owner)

    assert outcome is TransferCancelOutcome.canceling
    io.cancel_job.assert_awaited_once_with("producer-42", RAW_TOKEN)
    assert io.start_workflow_async.await_count == 1
    args, kwargs = _await_args(io.start_workflow_async)
    _assert_not_durable(args)
    _assert_not_durable(kwargs)


@pytest.mark.asyncio
async def test_log_read_uses_current_caller_token_not_owner_credential(
    io: SimpleNamespace, fake_session: AsyncSession
) -> None:
    caller = _user(subject="observer", token="observer-current-token")
    io.get_transfer.return_value = SimpleNamespace(
        experiment="mfxl1001",
        run="42",
        cache_mode=CacheMode.per_transfer,
        owner_subject="different-owner",
        owner_email="owner@example.org",
        owner_username="owner",
    )
    io.tail.return_value = "log output"

    result = await service.read_transfer_log(
        fake_session, TRANSFER_ID, lcore.LogStream.producer_stdout, caller, lines=20
    )

    assert result == "log output"
    path, token = _await_args(io.tail)[0]
    assert isinstance(path, Path)
    assert token == "observer-current-token"
    assert _await_args(io.tail)[1] == {"lines": 20, "bytes_": None}


@pytest.mark.asyncio
async def test_iri_step_resolves_token_internally_and_returns_only_result(
    io: SimpleNamespace,
) -> None:
    io.submit_job.return_value = "producer-42"

    owner = tcore.Principal(issuer="https://issuer.example", subject="subject-1")
    result = await workflows._submit_producer(pcore.DEFAULT_JOB_SPEC, owner)

    assert result == "producer-42"
    assert result != RAW_TOKEN
    io.token_for.assert_awaited_once_with(owner)
    io.submit_job.assert_awaited_once_with(pcore.DEFAULT_JOB_SPEC, RAW_TOKEN)

    iri_steps = (
        workflows._submit_producer,
        workflows._upload_config,
        workflows._delete_config,
        workflows._delete_work_dir,
        workflows._get_producer_state,
        workflows._cancel_producer,
    )
    for step in iri_steps:
        parameters = signature(step).parameters
        assert "token" not in parameters
        assert "user" not in parameters


def test_fastcache_control_plane_has_no_bearer_credential_parameter() -> None:
    for method in (
        FastcacheClient.create_cache,
        FastcacheClient.get_cache,
        FastcacheClient.delete_cache,
    ):
        parameters = signature(method).parameters
        assert "token" not in parameters
        assert "authorization" not in parameters


@pytest.mark.asyncio
async def test_shutdown_rejects_unknown_or_non_shared_cache(
    io: SimpleNamespace, fake_session: AsyncSession
) -> None:
    with pytest.raises(NotFound):
        await service.shutdown_cache(fake_session, TRANSFER_ID)

    io.delete_cache.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_blocks_active_shared_cache_under_registry_lock(
    io: SimpleNamespace, fake_session: AsyncSession
) -> None:
    io.lock_active_cache.return_value = SimpleNamespace(id=TRANSFER_ID, retired_at=None)
    io.count_active_transfers_by_cache.return_value = 2

    with pytest.raises(CacheShutdownBlocked):
        await service.shutdown_cache(fake_session, TRANSFER_ID)

    io.delete_cache.assert_not_awaited()
    io.retire_cache.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_retires_shared_cache_in_delete_transaction(
    io: SimpleNamespace, fake_session: AsyncSession
) -> None:
    cache = SimpleNamespace(id=TRANSFER_ID, retired_at=None)
    io.lock_active_cache.return_value = cache

    await service.shutdown_cache(fake_session, TRANSFER_ID)

    io.delete_cache.assert_awaited_once_with(TRANSFER_ID)
    io.retire_cache.assert_awaited_once()
    args, kwargs = _await_args(io.retire_cache)
    assert args == (ANY, cache)
    assert kwargs["retired_at"].tzinfo is UTC


@pytest.mark.asyncio
async def test_shared_cache_attachment_rejects_retired_registry_row() -> None:
    transfer = SimpleNamespace(
        cache_mode=CacheMode.shared,
        experiment="mfxl1001",
        cache_id=None,
        cache_hostname=None,
        pull_port=None,
        push_port=None,
    )
    retired = SimpleNamespace(
        experiment="mfxl1001",
        retired_at=NOW,
    )
    result = SimpleNamespace(scalar_one=lambda: retired)
    session = cast(
        AsyncSession,
        SimpleNamespace(
            get=AsyncMock(return_value=transfer),
            execute=AsyncMock(side_effect=[None, result]),
        ),
    )

    with pytest.raises(LookupError, match="has been shut down"):
        await service.repo.set_cache_endpoints(
            session,
            TRANSFER_ID,
            cache_id=TRANSFER_ID,
            hostname="dtn.example",
            pull_port=5000,
            push_port=5001,
        )

    assert transfer.cache_id is None

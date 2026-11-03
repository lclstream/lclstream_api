"""EJFAT: the URI holds a token, so it is never stored, logged, echoed, or sent to DBOS."""

import json
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from lclstream_api.v2 import service, workflows
from lclstream_api.v2.auth import AuthenticatedUser
from lclstream_api.v2.clients.fastcache import CacheConfig, CachePublic, FastcacheClient
from lclstream_api.v2.core.enums import CacheState, ConsumerSocket
from lclstream_api.v2.exceptions import UpstreamError, register_exception_handlers
from lclstream_api.v2.models import TransferCreate, TransferState

TRANSFER_ID = UUID("00000000-0000-0000-0000-0000000000ef")
CACHE_ID = UUID("00000000-0000-0000-0000-0000000000ca")
TOKEN = "instance-secret-token"
URI = f"ejfats://{TOKEN}@lb.example:18008/lb/7?data=10.0.0.1"
NOW = datetime(2026, 11, 3, tzinfo=UTC)


def _user() -> AuthenticatedUser:
    return AuthenticatedUser(
        issuer="https://issuer.example",
        subject="subject-1",
        email="user@example.org",
        username="user",
        token="bearer",
        expires_at=datetime.now(UTC) + timedelta(hours=12),
    )


def _cache() -> CachePublic:
    return CachePublic(
        id=CACHE_ID,
        key=str(TRANSFER_ID),
        user="user@example.org",
        state=CacheState.active,
        log_path=Path("/sdf/cache-logs/x/cache.log"),
        config=CacheConfig(
            hostname="dtn-01",
            pull_uri="tcp://dtn-01:30000",
            push_uri="tcp://dtn-01:30001",
            type=9,
        ),
    )


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"consumer_socket": "ejfat", "ejfat_uri": URI}, None),
        ({"consumer_socket": "ejfat"}, "goes with"),
        ({"ejfat_uri": URI}, "goes with"),
        (
            {"consumer_socket": "ejfat", "ejfat_uri": f"https://{TOKEN}@x"},
            "must start with",
        ),
        (
            {"consumer_socket": "ejfat", "ejfat_uri": URI, "cache_mode": "shared"},
            "cache_mode=per_transfer",
        ),
    ],
)
def test_transfer_create_validates_ejfat(
    make_params, overrides: dict, error: str | None
) -> None:
    body = {"parameters": make_params().model_dump(mode="json"), **overrides}
    if error is None:
        TransferCreate.model_validate(body)
        return
    with pytest.raises(ValidationError) as exc_info:
        TransferCreate.model_validate(body)
    assert error in str(exc_info.value)


@pytest.mark.asyncio
async def test_invalid_request_does_not_echo_the_uri(make_params) -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.post("/t")
    async def _create(body: TransferCreate) -> None:
        return None

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as http:
        resp = await http.post(
            "/t",
            json={
                "parameters": make_params().model_dump(mode="json"),
                "consumer_socket": "ejfat",
                "ejfat_uri": f"https://{TOKEN}@x",
            },
        )

    assert resp.status_code == 422
    assert "must start with" in resp.text
    assert TOKEN not in resp.text


@pytest.mark.asyncio
async def test_fastcache_client_sends_the_raw_uri() -> None:
    """model_dump masks SecretStr; fastcache needs the real value."""
    sent: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return httpx.Response(201, json=_cache().model_dump(mode="json"))

    client = object.__new__(FastcacheClient)
    client._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://cache.example"
    )
    await client.create_cache(
        key="k", requested_by="u", output="ejfat", ejfat_uri=SecretStr(URI)
    )

    assert sent[0]["ejfat_uri"] == URI


@pytest.fixture
def io(monkeypatch: pytest.MonkeyPatch, make_producer_settings) -> SimpleNamespace:
    async def insert_transfer(_session, **values):
        return SimpleNamespace(
            id=values["transfer_id"],
            created_at=NOW,
            owner_email=values["owner_email"],
            state=TransferState.provisioning,
            experiment=values["experiment"],
            run=values["run"],
            cache_mode=values["cache_mode"],
            consumer_socket=values["consumer_socket"],
            cache_hostname=None,
            push_port=None,
        )

    mocks = SimpleNamespace(
        insert_transfer=AsyncMock(side_effect=insert_transfer),
        set_cache_endpoints=AsyncMock(),
        create_cache=AsyncMock(return_value=_cache()),
        start_workflow_async=AsyncMock(),
    )
    monkeypatch.setattr(service.repo, "insert_transfer", mocks.insert_transfer)
    monkeypatch.setattr(service.repo, "set_cache_endpoints", mocks.set_cache_endpoints)
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
    monkeypatch.setattr(
        service.fastcache,
        "client",
        lambda: SimpleNamespace(create_cache=mocks.create_cache),
    )
    return mocks


async def _create(session: AsyncSession, make_params) -> None:
    await service.create_transfer(
        session,
        _user(),
        make_params(),
        experiment="mfxl1001",
        run="42",
        consumer_socket=ConsumerSocket.ejfat,
        ejfat_uri=SecretStr(URI),
    )


@pytest.mark.asyncio
async def test_ejfat_cache_starts_in_the_request(
    io: SimpleNamespace, fake_session: AsyncSession, make_params
) -> None:
    await _create(fake_session, make_params)

    kwargs = io.create_cache.await_args.kwargs
    assert kwargs["output"] == "ejfat"
    assert kwargs["ejfat_uri"].get_secret_value() == URI
    assert io.set_cache_endpoints.await_args.args[2].cache_id == CACHE_ID
    # Nothing the URI could leak through is persisted.
    assert TOKEN not in repr(io.insert_transfer.await_args)
    assert TOKEN not in repr(io.set_cache_endpoints.await_args)
    io.start_workflow_async.assert_awaited_once_with(
        workflows.provision_transfer, TRANSFER_ID
    )


@pytest.mark.asyncio
async def test_fastcache_failure_is_a_bad_gateway(
    io: SimpleNamespace, fake_session: AsyncSession, make_params
) -> None:
    io.create_cache.side_effect = httpx.ConnectError("refused")

    with pytest.raises(UpstreamError):
        await _create(fake_session, make_params)

    io.insert_transfer.assert_not_awaited()


@pytest.mark.asyncio
async def test_fastcache_rejection_hides_the_internal_url(
    io: SimpleNamespace, fake_session: AsyncSession, make_params
) -> None:
    request = httpx.Request("POST", "https://internal.example/caches/")
    response = httpx.Response(503, text="no free cache ports", request=request)
    io.create_cache.side_effect = httpx.HTTPStatusError(
        "fastcache_api 503", request=request, response=response
    )

    with pytest.raises(UpstreamError) as exc_info:
        await _create(fake_session, make_params)

    assert "no free cache ports" in str(exc_info.value)
    assert "internal.example" not in str(exc_info.value)

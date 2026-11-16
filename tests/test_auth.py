"""Tests for the pluggable authentication backends."""

import asyncio
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import jwt
import psik
import pytest
import svcs
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.requests import Request

from lclstream_api.auth import (
    MTLS,
    Anonymous,
    AuthBackend,
    AuthChain,
    ForwardedJWT,
    TrustedHeaders,
)
from lclstream_api.config import Config
from lclstream_api.models import CacheMetrics, ClientName, PortEntry, PortTransition
from lclstream_api.ports import PortDatabase
from lclstream_api.server import app as main_app
from lclstream_api.xfer_db import XferDatabase

# An `auth:` block as written in YAML, e.g. {"type": "mtls"}.
AuthBlock = dict[str, Any]

# Returns the test client, now authenticated as the given user.
AsUser = Callable[[str], TestClient]


def connection(headers: dict[str, str] | None = None) -> Request:
    """A bare request carrying headers."""
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request({"type": "http", "headers": raw, "client": ("10.0.0.5", 1234)})


def user_of(backend: AuthBackend, headers: dict[str, str] | None = None) -> str | None:
    """The user backend finds on a request carrying headers, or None."""
    principal = asyncio.run(backend.authenticate(connection(headers)))
    if principal is None:
        return None
    return principal.identity


# 32 bytes, the minimum PyJWT accepts without warning for HS256.
PROXY_KEY = "proxy-only-secret-32-bytes-long!"


def bearer(claims: dict[str, Any]) -> str:
    """A JWT carrying claims, signed with a key the app never sees."""
    return "Bearer " + jwt.encode(claims, PROXY_KEY, algorithm="HS256")


# --- Choosing a backend -----------------------------------------------------


def auth_from(config: Config, auth: AuthBlock) -> AuthBackend:
    """The backend Config builds from an `auth` block."""
    return Config.model_validate({**config.model_dump(), "auth": auth}).auth


@pytest.mark.parametrize(
    ("auth", "headers", "expected"),
    [
        ({"type": "anonymous", "username": "alice"}, {}, "alice"),
        ({"type": "trusted_headers"}, {"X-Auth-Request-User": "alice"}, "alice"),
        ({"type": "forwarded_jwt"}, {"Authorization": bearer({"email": "a@x"})}, "a@x"),
        ({"type": "mtls"}, {"X-Auth-Request-User": "alice"}, None),
        (
            {"type": "chain", "backends": [{"type": "trusted_headers"}]},
            {"X-Auth-Request-User": "alice"},
            "alice",
        ),
    ],
)
def test_config_picks_backend(
    config: Config, auth: AuthBlock, headers: dict[str, str], expected: str | None
) -> None:
    assert user_of(auth_from(config, auth), headers=headers) == expected


def test_config_requires_auth(config: Config) -> None:
    data = config.model_dump()
    del data["auth"]
    with pytest.raises(ValidationError, match="auth"):
        Config.model_validate(data)


@pytest.mark.parametrize(
    "auth",
    [
        {"type": "nope"},
        {"type": "trusted_headers", "user_hedaer": "X"},
    ],
)
def test_config_rejects_bad_auth(config: Config, auth: AuthBlock) -> None:
    with pytest.raises(ValidationError):
        auth_from(config, auth)


def test_anonymous_app(client: TestClient) -> None:
    response = client.get("/transfers")
    assert response.status_code == 200


def test_config_selects_backend(
    config: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config.auth = TrustedHeaders()
    path = tmp_path / "lclstream_api.json"
    path.write_text(config.model_dump_json())
    monkeypatch.setenv("LCLSTREAM_API_CONFIG", str(path))

    with TestClient(main_app, base_url="http://testserver/v1") as client:
        assert client.get("/transfers").status_code == 401
        response = client.get("/transfers", headers={"X-Auth-Request-User": "alice"})
        assert response.status_code == 200


# --- Anonymous --------------------------------------------------------------


def test_anonymous() -> None:
    assert user_of(Anonymous()) == "none"
    assert user_of(Anonymous(username="alice")) == "alice"


# --- TrustedHeaders ---------------------------------------------------------


def test_headers_give_user() -> None:
    headers = {"X-Auth-Request-User": "jdoe"}
    assert user_of(TrustedHeaders(), headers=headers) == "jdoe"


def test_headers_need_user() -> None:
    assert user_of(TrustedHeaders()) is None
    assert user_of(TrustedHeaders(), headers={"X-Auth-Request-User": ""}) is None


def test_custom_user_header() -> None:
    backend = TrustedHeaders(user_header="X-Remote-User")
    assert user_of(backend, headers={"X-Remote-User": "alice"}) == "alice"
    assert user_of(backend, headers={"X-Auth-Request-User": "alice"}) is None


# --- ForwardedJWT -----------------------------------------------------------


def test_jwt_gives_email() -> None:
    headers = {"Authorization": bearer({"email": "alice@example.com"})}
    assert user_of(ForwardedJWT(), headers=headers) == "alice@example.com"


def test_jwt_claims_in_order() -> None:
    claims = {"email": "", "preferred_username": "alice"}
    headers = {"Authorization": bearer(claims)}
    assert user_of(ForwardedJWT(), headers=headers) == "alice"


@pytest.mark.parametrize(
    "authorization", [None, "Basic abc", "Bearer not-a-jwt", "Bearer a.%%%.c"]
)
def test_jwt_unusable_is_none(authorization: str | None) -> None:
    headers = {"Authorization": authorization} if authorization else {}
    assert user_of(ForwardedJWT(), headers=headers) is None


# --- MTLS -------------------------------------------------------------------


def test_mtls_without_cert() -> None:
    # No client cert: certified only knows the address, which proves nothing.
    headers = {"X-Auth-Request-User": "test-user@example.com"}
    assert user_of(MTLS(), headers=headers) is None


def test_mtls_needs_certified(config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("lclstream_api.auth.mtls.get_clientname", None)
    with pytest.raises(ValidationError, match="needs the certified package"):
        auth_from(config, {"type": "mtls"})


# --- AuthChain --------------------------------------------------------------

OAUTH2_PROXY_OR_MTLS = AuthChain(backends=[TrustedHeaders(), ForwardedJWT(), MTLS()])


def test_chain_first_match_wins() -> None:
    headers = {
        "X-Auth-Request-User": "alice",
        "Authorization": bearer({"email": "bob@example.com"}),
    }
    assert user_of(OAUTH2_PROXY_OR_MTLS, headers=headers) == "alice"


def test_chain_falls_through() -> None:
    headers = {"Authorization": bearer({"email": "bob@example.com"})}
    assert user_of(OAUTH2_PROXY_OR_MTLS, headers=headers) == "bob@example.com"


# ---------------------------------------------------------------------------
# Cross-user ownership tests (reworked from 2396ece for this architecture)
#
# 2396ece tested ownership via the v2 SQLAlchemy/DBOS service layer.  Here we
# test the same invariants at the router level, registering controlled PortDB
# and XferDB state in the app's svcs registry and swapping in an Anonymous
# backend for each user, without touching certificates.
# ---------------------------------------------------------------------------


class _MockPortDB:
    def __init__(self, entries: dict[int, PortEntry]) -> None:
        self._entries = entries

    def items(self):
        return self._entries.items()

    def __getitem__(self, eid: int) -> PortEntry:
        return self._entries[eid]


class _MockXferDB:
    def __init__(self, jobs: dict) -> None:
        self._jobs = jobs

    def __getitem__(self, eid: int):
        return self._jobs[eid]


def _port_entry(eid: int, user: str) -> PortEntry:
    port = 11400 + eid * 2
    return PortEntry(
        eid=eid,
        user=user,
        port=port,
        internal_url=f"tcp://127.0.0.1:{port}",
        external_url=f"tcp://127.0.0.1:{port + 1}",
    )


def _mock_transfer() -> SimpleNamespace:
    log_entry = PortTransition(
        time=time.time(),
        client=ClientName.cache,
        state=psik.JobState.active,
        info="",
        jobndx=0,
    )
    return SimpleNamespace(
        states={ClientName.cache: psik.JobState.active},
        log=[log_entry],
        cache_metrics=CacheMetrics(
            time=time.time(), producers=1, recvd=0, sent=0, buffered=0
        ),
        cancel_job=AsyncMock(),
    )


@pytest.fixture
def transfer_client(setup_lclstream_api: Path) -> Iterator[AsUser]:
    """Yield a factory that returns a TestClient authenticated as a given user.

    Two transfers exist: alice owns eid=1, bob owns eid=2.
    """
    alice_entry = _port_entry(1, "alice@example.com")
    bob_entry = _port_entry(2, "bob@example.com")

    # Entering the client runs the lifespan; then swap in the mock tables.
    with TestClient(main_app, base_url="http://testserver/v1") as client:
        registry = svcs.fastapi.get_registry(client)
        registry.register_value(
            PortDatabase, _MockPortDB({1: alice_entry, 2: bob_entry})
        )
        registry.register_value(
            XferDatabase, _MockXferDB({1: _mock_transfer(), 2: _mock_transfer()})
        )

        def _as(user: str) -> TestClient:
            registry.register_value(AuthBackend, Anonymous(username=user))
            return client

        yield _as


def test_non_owner_get_transfer_is_403(transfer_client):
    response = transfer_client("bob@example.com").get("/transfers/1")
    assert response.status_code == 403


def test_non_owner_cancel_transfer_is_403(transfer_client):
    response = transfer_client("bob@example.com").delete("/transfers/1")
    assert response.status_code == 403


def test_owner_get_transfer_is_200(transfer_client):
    response = transfer_client("alice@example.com").get("/transfers/1")
    assert response.status_code == 200
    assert response.json()["user"] == "alice@example.com"


def test_list_transfers_scoped_to_owner(transfer_client):
    alice_resp = transfer_client("alice@example.com").get("/transfers")
    assert alice_resp.status_code == 200
    assert all(t["user"] == "alice@example.com" for t in alice_resp.json())

    bob_resp = transfer_client("bob@example.com").get("/transfers")
    assert bob_resp.status_code == 200
    assert all(t["user"] == "bob@example.com" for t in bob_resp.json())


def test_no_token_fields_on_port_entry():
    """PortEntry records ownership but stores no bearer token or credential."""
    forbidden = {"token", "authorization", "credential", "bearer", "secret"}
    assert not (set(PortEntry.model_fields) & forbidden)

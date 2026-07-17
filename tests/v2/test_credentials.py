from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast
from unittest.mock import ANY, AsyncMock

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from lclstream_api.v2 import auth, repo
from lclstream_api.v2.auth import AuthenticatedUser
from lclstream_api.v2.config import CredentialsSettings

NOW = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def fernet(monkeypatch: pytest.MonkeyPatch) -> Fernet:
    key = Fernet.generate_key()
    monkeypatch.setattr(
        auth.config,
        "get_credentials",
        lambda: CredentialsSettings(fernet_key=key.decode()),
    )
    return Fernet(key)


def _session() -> AsyncSession:
    """Fake session; these tests only exercise mocked repo calls."""
    return cast(AsyncSession, SimpleNamespace())


def test_validate_configuration_rejects_invalid_key_at_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        auth.config,
        "get_credentials",
        lambda: CredentialsSettings(fernet_key="not-a-fernet-key"),
    )

    with pytest.raises(ValueError):
        auth.validate_configuration()


@pytest.mark.asyncio
async def test_capture_encrypts_token_and_uses_composite_identity(
    monkeypatch: pytest.MonkeyPatch, fernet: Fernet
) -> None:
    upsert = AsyncMock(return_value=True)
    monkeypatch.setattr(auth.repo, "upsert_user_credential", upsert)
    user = AuthenticatedUser(
        issuer="https://issuer.example",
        subject="subject-1",
        email="user@example.org",
        token="raw-token",
        expires_at=NOW + timedelta(hours=1),
    )

    assert await auth.capture_token(_session(), user) is True

    call = upsert.await_args
    assert call is not None
    kwargs = call.kwargs
    assert kwargs["issuer"] == user.issuer
    assert kwargs["subject"] == user.subject
    assert kwargs["email"] == user.email
    assert kwargs["expires_at"] == user.expires_at
    assert kwargs["encrypted_token"] != user.token.get_secret_value().encode()
    assert (
        fernet.decrypt(kwargs["encrypted_token"])
        == user.token.get_secret_value().encode()
    )


@pytest.mark.asyncio
async def test_get_valid_token_decrypts_unexpired_credential(
    monkeypatch: pytest.MonkeyPatch, fernet: Fernet
) -> None:
    encrypted = fernet.encrypt(b"stored-token")
    get = AsyncMock(
        return_value=SimpleNamespace(
            expires_at=NOW + timedelta(seconds=1), encrypted_token=encrypted
        )
    )
    monkeypatch.setattr(auth.repo, "get_user_credential", get)

    token = await auth.get_valid_token(_session(), "issuer", "subject", now=NOW)

    assert token == "stored-token"
    get.assert_awaited_once_with(ANY, "issuer", "subject")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        None,
        SimpleNamespace(expires_at=NOW, encrypted_token=b"unused"),
        SimpleNamespace(
            expires_at=NOW + timedelta(seconds=1), encrypted_token=b"not-fernet"
        ),
    ],
    ids=["missing", "expired", "invalid-ciphertext"],
)
async def test_get_valid_token_rejects_unusable_credentials(
    monkeypatch: pytest.MonkeyPatch, fernet: Fernet, row: object
) -> None:
    monkeypatch.setattr(auth.repo, "get_user_credential", AsyncMock(return_value=row))

    assert await auth.get_valid_token(_session(), "issuer", "subject", now=NOW) is None


@pytest.mark.asyncio
async def test_upsert_is_one_conditional_postgres_statement() -> None:
    result = SimpleNamespace(scalar_one_or_none=lambda: NOW)
    execute = AsyncMock(return_value=result)
    session = cast(AsyncSession, SimpleNamespace(execute=execute))

    changed = await repo.upsert_user_credential(
        session,
        issuer="issuer",
        subject="subject",
        email="user@example.org",
        encrypted_token=b"ciphertext",
        expires_at=NOW,
    )

    assert changed is True
    call = execute.await_args
    assert call is not None
    statement = call.args[0]
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": False}
        )
    )
    assert "ON CONFLICT (issuer, subject) DO UPDATE" in sql
    assert "WHERE excluded.expires_at > user_credentials.expires_at" in sql
    assert execute.await_count == 1

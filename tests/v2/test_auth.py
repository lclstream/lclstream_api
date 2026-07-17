from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest
from fastapi import HTTPException
from fastapi_jwks.models.types import JWKSAuthCredentials
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from lclstream_api.v2 import auth

EXP = 1_893_499_200


def _credentials(*, subject: str = "user-123", email: str = "user@example.org"):
    return JWKSAuthCredentials(
        scheme="Bearer",
        credentials="raw-secret-token",
        payload=auth.TokenPayload(
            iss="https://idp.example.org",
            aud="lclstream",
            exp=EXP,
            email=email,
            email_verified=True,
            sub=subject,
        ),
    )


@pytest.mark.asyncio
async def test_require_user_returns_immutable_principal_and_captures_token(
    monkeypatch: pytest.MonkeyPatch, fake_session: AsyncSession
) -> None:
    monkeypatch.setattr(
        auth,
        "get_oidc",
        lambda: SimpleNamespace(expected_users=["user@example.org"]),
    )
    capture_token = AsyncMock(return_value=True)
    monkeypatch.setattr(auth, "capture_token", capture_token)

    user = await auth.require_user(_credentials(), fake_session)

    assert user.issuer == "https://idp.example.org"
    assert user.subject == "user-123"
    assert user.email == "user@example.org"
    assert user.token.get_secret_value() == "raw-secret-token"
    assert user.expires_at == datetime.fromtimestamp(EXP, UTC)
    assert user.principal == ("https://idp.example.org", "user-123")
    assert "raw-secret-token" not in repr(user)
    capture_token.assert_awaited_once_with(ANY, user)

    with pytest.raises(ValidationError):
        user.email = "changed@example.org"  # type: ignore[misc]


def test_token_payload_requires_subject() -> None:
    with pytest.raises(ValidationError):
        auth.TokenPayload.model_validate(
            {
                "iss": "https://idp.example.org",
                "aud": "lclstream",
                "exp": EXP,
                "email": "user@example.org",
                "email_verified": True,
            }
        )


@pytest.mark.asyncio
async def test_require_user_preserves_allowlist_policy(
    monkeypatch: pytest.MonkeyPatch, fake_session: AsyncSession
) -> None:
    monkeypatch.setattr(
        auth,
        "get_oidc",
        lambda: SimpleNamespace(expected_users=["someone-else@example.org"]),
    )

    with pytest.raises(HTTPException) as exc_info:
        await auth.require_user(_credentials(), fake_session)

    assert exc_info.value.status_code == 403

import re
from datetime import UTC, datetime
from typing import Annotated

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Depends, HTTPException, status
from fastapi_jwks.dependencies.jwk_auth import JWKSAuth
from fastapi_jwks.models.types import (
    JWKSAuthCredentials,
    JWKSConfig,
    JWTDecodeConfig,
)
from fastapi_jwks.validators import JWKSValidator
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from . import config, repo
from .config import get_oidc
from .db import get_session

# Claims every accepted token must carry.
_REQUIRED_JWT_FIELDS = ["exp", "iss", "aud", "sub"]

# Becomes a path segment, so keep "/" and ".." out.
# POSIX name rule: "name" is a display name by spec.
_UNIX_USERNAME = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")


class TokenPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    iss: str = Field(min_length=1)
    aud: str | list[str]
    exp: int
    email: str
    email_verified: bool = False
    sub: str = Field(min_length=1)
    # Dex/S3DF puts the unix username here.
    name: str | None = None


_validator = JWKSValidator[TokenPayload](
    decode_config=JWTDecodeConfig(
        audience=get_oidc().audiences,
        issuer=get_oidc().issuer_url,
        options={"require": _REQUIRED_JWT_FIELDS},
    ),
    jwks_config=JWKSConfig(url=get_oidc().jwks_uri),
)
_jwks_auth = JWKSAuth[TokenPayload](_validator)


class AuthenticatedUser(BaseModel):
    """Verified caller identity plus its request-scoped credential.

    Never pass ``token`` into a durable workflow.
    """

    model_config = ConfigDict(frozen=True)

    issuer: str
    subject: str
    email: str
    username: str
    token: SecretStr
    expires_at: datetime

    @property
    def principal(self) -> tuple[str, str]:
        return self.issuer, self.subject


def _cipher() -> Fernet:
    return Fernet(config.get_credentials().fernet_key.get_secret_value())


def validate_configuration() -> None:
    """Fail startup when the Fernet key is invalid."""

    _cipher()


async def capture_token(session: AsyncSession, user: AuthenticatedUser) -> bool:
    """Encrypt and store ``user``'s credential; returns whether the row changed.

    Older or equal expiries are a no-op, keeping fresher tokens.
    """

    encrypted_token = _cipher().encrypt(user.token.get_secret_value().encode())
    return await repo.upsert_user_credential(
        session,
        issuer=user.issuer,
        subject=user.subject,
        email=user.email,
        encrypted_token=encrypted_token,
        expires_at=user.expires_at,
    )


async def get_valid_token(
    session: AsyncSession,
    issuer: str,
    subject: str,
    *,
    now: datetime,
) -> str | None:
    """Return a principal's usable token, or ``None`` without exposing failures."""

    row = await repo.get_user_credential(session, issuer, subject)
    if row is None or row.expires_at <= now:
        return None
    try:
        return _cipher().decrypt(row.encrypted_token).decode()
    except InvalidToken, UnicodeDecodeError:
        return None


def _unix_username(payload: TokenPayload) -> str:
    """Read the SDF unix username from the token's ``name`` claim.

    Every transfer writes under this name, so reject tokens without one.
    """
    if payload.name is None or not _UNIX_USERNAME.fullmatch(payload.name):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token claim 'name' is not a usable unix username",
        )
    return payload.name


async def require_user(
    auth_credentials: Annotated[JWKSAuthCredentials, Depends(_jwks_auth)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AuthenticatedUser:
    payload = auth_credentials.payload
    if not payload.email_verified or payload.email not in get_oidc().expected_users:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is not authorized to access this resource",
        )
    user = AuthenticatedUser(
        issuer=payload.iss,
        subject=payload.sub,
        email=payload.email,
        username=_unix_username(payload),
        token=auth_credentials.credentials,
        expires_at=datetime.fromtimestamp(payload.exp, UTC),
    )
    # Refresh the token background reconcile work uses.
    async with session.begin():
        await capture_token(session, user)
    return user


CurrentUser = Annotated[AuthenticatedUser, Depends(require_user)]

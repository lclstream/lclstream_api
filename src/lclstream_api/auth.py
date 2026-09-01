from typing import Annotated
from datetime import datetime, timezone, timedelta

from fastapi import Depends, HTTPException, status
from fastapi_jwks.dependencies.jwk_auth import JWKSAuth  # type: ignore
from fastapi_jwks.models.types import (  # type: ignore
    JWKSAuthCredentials,
    JWKSConfig,
    JWTDecodeConfig,
)
from fastapi_jwks.validators import JWKSValidator  # type: ignore
from cryptography.fernet import Fernet
from pydantic import BaseModel, ConfigDict

from .cfg import oidc, credentials
from .models import Principal

# Claims every accepted token must carry (validated by pyjwt's ``require``).
_REQUIRED_JWT_FIELDS = ["exp", "iss", "aud"]


class TokenPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    iss: str
    aud: str | list[str]
    exp: int
    email: str
    email_verified: bool = False
    sub: str | None = None
    name: str | None = None


_validator = JWKSValidator[TokenPayload](
    decode_config=JWTDecodeConfig(
        audience=oidc.audiences,
        issuer=oidc.issuer_url,
        options={"require": _REQUIRED_JWT_FIELDS},
    ),
    jwks_config=JWKSConfig(url=oidc.jwks_uri),
)
_jwks_auth = JWKSAuth[TokenPayload](_validator)
jwks_auth = _jwks_auth


async def require_user(
    credentials: Annotated[JWKSAuthCredentials, Depends(_jwks_auth)],
) -> Principal:
    payload = credentials.payload
    if not payload.email_verified or payload.email not in oidc.expected_users:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is not authorized to access this resource",
        )
    return Principal(issuer=payload.iss, subject=payload.sub or "", email=payload.email)


CurrentUser = Annotated[Principal, Depends(require_user)]


def capture_token(token: str, payload: TokenPayload) -> tuple[bytes, datetime]:
    """Encrypt the token and return the encrypted bytes and expiry datetime."""
    f = Fernet(credentials.fernet_key.get_secret_value().encode())
    encrypted = f.encrypt(token.encode())

    # payload.exp is a unix timestamp
    expiry = datetime.fromtimestamp(payload.exp, tz=timezone.utc)

    return encrypted, expiry


def get_valid_token(encrypted_token: bytes) -> str:
    """Decrypt the token and return the original token string."""
    f = Fernet(credentials.fernet_key.get_secret_value().encode())
    decrypted = f.decrypt(encrypted_token)
    return decrypted.decode()


def validate_token_lifetime(expiry: datetime, requested_duration_s: int) -> bool:
    """Check if the token is valid for the requested duration plus grace period."""
    now = datetime.now(timezone.utc)
    grace = timedelta(seconds=credentials.lifecycle_grace_seconds)
    return expiry > now + timedelta(seconds=requested_duration_s) + grace

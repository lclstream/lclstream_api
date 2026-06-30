from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi_jwks.dependencies.jwk_auth import JWKSAuth
from fastapi_jwks.models.types import (
    JWKSAuthCredentials,
    JWKSConfig,
    JWTDecodeConfig,
)
from fastapi_jwks.validators import JWKSValidator
from pydantic import BaseModel, ConfigDict

from .config import load_config

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

_oidc = load_config().oidc

_validator = JWKSValidator[TokenPayload](
    decode_config=JWTDecodeConfig(
        audience=_oidc.audiences,
        issuer=_oidc.issuer_url,
        options={"require": _REQUIRED_JWT_FIELDS},
    ),
    jwks_config=JWKSConfig(url=_oidc.jwks_uri),
)
_jwks_auth = JWKSAuth[TokenPayload](_validator)


async def require_user(
    credentials: Annotated[JWKSAuthCredentials, Depends(_jwks_auth)],
) -> str:
    payload = credentials.payload
    if not payload.email_verified or payload.email not in _oidc.expected_users:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is not authorized to access this resource",
        )
    return payload.email


CurrentUser = Annotated[str, Depends(require_user)]

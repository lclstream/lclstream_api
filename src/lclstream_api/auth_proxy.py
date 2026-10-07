import base64
import json
from typing import Annotated, Callable
from fastapi import Depends, Header, HTTPException, status, Request
from .config import Config, load_config

import certified.fast


def _email_from_bearer(authorization: str | None) -> str | None:
    """Extract email/preferred_username from a Bearer JWT payload (no sig verification).

    oauth2-proxy has already validated the JWT; we just decode the claims.
    """
    if not authorization or not authorization.startswith("Bearer "):
        return None
    parts = authorization[7:].split(".")
    if len(parts) != 3:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        return payload.get("email") or payload.get("preferred_username") or None
    except Exception:
        return None


def get_user_identity(trust_header: bool = False):
    """Dependency factory returning a user identity string.

    trust_header=True: identity comes from the trusted proxy (header or JWT).
    trust_header=False: identity MUST come from mTLS (certified).
    """
    async def _get_user(
        request: Request,
        x_auth_request_user: Annotated[str | None, Header()] = None,
        authorization: Annotated[str | None, Header()] = None,
    ) -> str:
        cfg = load_config()

        if trust_header and cfg.trusted_proxy:
            # Prefer the header oauth2-proxy injects for session-based auth.
            if x_auth_request_user:
                return x_auth_request_user
            # For --skip-jwt-bearer-tokens the proxy forwards the JWT but does
            # NOT inject X-Auth-Request-User; decode the claim directly.
            email = _email_from_bearer(authorization)
            if email:
                return email
            # Fall through to mTLS for direct-to-:8000 access with a client cert.

        # mTLS identity (direct access or fallback).
        # Reject the "addr:X" fallback — a bare IP is not a verified identity.
        try:
            user = certified.fast.get_clientname(request)
            if user and not user.startswith("addr:"):
                return user
        except Exception:
            pass

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User identity could not be verified via proxy headers or mTLS.",
        )

    return _get_user


CurrentUser = Annotated[str, Depends(get_user_identity(trust_header=True))]
CallbackUser = Annotated[str, Depends(get_user_identity(trust_header=False))]

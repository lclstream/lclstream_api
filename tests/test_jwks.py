import os
from typing import Optional

import pytest
from unittest.mock import MagicMock

import jwt
import requests
from jwt import PyJWKClient
from fastapi_jwks.models.types import JWTHeader

# FIXME: config. loading for jwks currently uses $VIRTUAL_ENV
# due to module-level definitions in auth.py
#
#from test_config import config, setup_lclstream_api  # noqa: F401

from fastapi import HTTPException, Request
from lclstream_api.auth import require_user, _jwks_auth, TokenPayload
from lclstream_api.config import load_config

from starlette.requests import Request
from starlette.types import Scope

def make_request_with_auth(token: str) -> Request:
    scope: Scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [
            (b"authorization", f"Bearer {token}".encode("latin-1")),
        ],
        "query_string": b"",
        "client": ("testclient", 123),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)

@pytest.fixture
def my_token() -> Optional[str]:
    try:
        with open(os.path.join(os.getenv("HOME"), ".s3df-access-token"), "r") as f:
            return f.read().strip()
    except Exception as e:
        return None

#@pytest.mark.asyncio
#async def test_validate_token_direct(my_token):
#    payload = await _validator(my_token)  # or: await _validator.validate(token) depending on version
#    assert payload.iss == _oidc.issuer_url

def test_config_aud():
    """ Parsing the audiences field is apparently difficult for the lclstream-api config.
    """

    aud = load_config().oidc.audiences
    assert len(aud) == 1
    assert aud[0] == "s3df"

def test_token(my_token):
    if my_token is None:
        pytest.skip("No token - skipping")

    unverified_header = jwt.get_unverified_header(my_token)
    unverified_claims = jwt.decode(my_token, options={"verify_signature": False})

    #kid = unverified_header.get("kid")
    #alg = unverified_header.get("alg")
    #print("kid:", kid, "alg:", alg)
    #print("iss:", unverified_claims.get("iss"))
    #print("aud:", unverified_claims.get("aud"))

    header = JWTHeader.model_validate(unverified_header)
    print(header)

    jwks_data = _jwks_auth.jwks_validator.jwks_data()
    provided_algorithms = jwks_data.algorithms
    if provided_algorithms and header.alg not in provided_algorithms:
        pytest.fail( f"Could not find '{header.alg}' in provided algorithms: {provided_algorithms}" )
    for key in jwks_data.keys:
        if key.kid == header.kid:
            public_key = jwt.algorithms.get_default_algorithms()[
                header.alg
            ].from_jwk(key.model_dump(exclude_none=True))
            break
    if public_key is None:
        pytest.fail( f"No public key for provided algorithm '{header.alg}' found in JWKS data" )
    decoded = jwt.decode(
            my_token,
            key=public_key,
            **_jwks_auth.jwks_validator.decode_config.model_dump(),
            algorithms=[header.alg],
        )
    print(TokenPayload.model_validate(decoded))


@pytest.mark.asyncio
async def test_jwks_auth_validation(my_token):
    # 1. Create a mock request with the Authorization header
    #mock_request = MagicMock(spec=Request)
    #mock_request.headers = {
    #    "authorization": "Bearer invalid.jwt.auth"
    #}
    mock_request = make_request_with_auth("invalid.jwt.auth")

    with pytest.raises(HTTPException):
        user_payload = await _jwks_auth(mock_request)

    if my_token is None:
        pytest.skip("'~/.s3df-access-token' is missing. Skipping test.")

    #mock_request.headers = {
    #    "Authorization": f"Bearer {my_token}"
    #}
    mock_request = make_request_with_auth(my_token)

    # 2. Call the dependency instance directly
    # Note: _jwks_auth is likely an instance of JWKSAuth
    try:
        user_payload = await _jwks_auth(mock_request)
        assert user_payload is not None
    except Exception as e:
        pytest.fail(f"Token validation failed: {e}")

    email = await require_user(user_payload)
    print(f"Extracted email = {email}")

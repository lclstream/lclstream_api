from typing import Literal, Self

from pydantic import model_validator
from starlette.requests import HTTPConnection, Request

from .base import AuthBackend, Principal

try:
    from certified.fast import get_clientname
except ImportError:
    get_clientname = None  # type: ignore[assignment]


class MTLS(AuthBackend):
    """Read identity from the TLS client certificate, via certified.

    The server must request client certs, e.g. `certified serve`.
    Users come back as "uid:<userID>" or "cn:<commonName>".
    """

    type: Literal["mtls"] = "mtls"

    @model_validator(mode="after")
    def _needs_certified(self) -> Self:
        if get_clientname is None:
            raise ValueError("auth type 'mtls' needs the certified package installed.")
        return self

    async def authenticate(self, conn: HTTPConnection) -> Principal | None:
        if get_clientname is None:
            return None

        try:
            user = get_clientname(Request(conn.scope))
        except Exception:
            return None

        # Without a cert certified falls back to "addr:<ip>", which proves nothing.
        if not user or user.startswith("addr:"):
            return None
        return Principal(user)

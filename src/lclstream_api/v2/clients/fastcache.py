import ssl
from pathlib import Path
from uuid import UUID

import httpx
from pydantic import AnyUrl, BaseModel

from ..config import FastcacheClientSettings
from ..core import transfer as tcore


class CacheConfig(BaseModel):
    """Cache configuration document returned by fastcache_api (TODO: this is mirrored)."""

    hostname: str
    pull_uri: AnyUrl
    push_uri: AnyUrl
    type: int = 4
    helper_threads: int = 0
    io_threads: int = 16
    hwm: int = 10
    timeout: int = 120_000
    verbose: bool = False


class CacheCreate(BaseModel):
    """POST body for creating a cache (TODO: this is mirrored)."""

    transfer_id: UUID
    requested_by: str


class CachePublic(BaseModel):
    """A cache row as returned by fastcache_api (TODO: this is mirrored)."""

    id: UUID
    transfer_id: str
    user: str
    state: tcore.CacheState
    log_path: Path
    config: CacheConfig


def _ssl_context(settings: FastcacheClientSettings) -> ssl.SSLContext:
    if settings.verify is False:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    elif settings.verify is True:
        context = ssl.create_default_context()
    else:
        context = ssl.create_default_context(cafile=settings.verify)

    context.load_cert_chain(str(settings.client_cert), str(settings.client_key))
    return context


class FastcacheClient:
    def __init__(self, settings: FastcacheClientSettings) -> None:
        self._settings = settings
        self._http = httpx.AsyncClient(
            base_url=str(settings.base_url),
            verify=_ssl_context(settings),
            timeout=settings.timeout_s,
        )

    def _auth_headers(self) -> httpx.Headers:
        # Re-read the shared token file on every call so a rotated mint is
        # picked up without restarting.
        return httpx.Headers({"Authorization": f"Bearer {self._settings.token}"})

    async def aclose(self) -> None:
        await self._http.aclose()

    async def create_cache(self, transfer_id: UUID, requested_by: str) -> CachePublic:
        body = CacheCreate(
            transfer_id=transfer_id,
            requested_by=requested_by,
        )
        response = await self._http.post(
            "/caches/",
            json=body.model_dump(exclude_none=True),
            headers=self._auth_headers(),
        )
        response.raise_for_status()
        return CachePublic.model_validate(response.json())

    async def get_cache(self, cache_id: UUID) -> CachePublic | None:
        response = await self._http.get(
            f"/caches/{cache_id}", headers=self._auth_headers()
        )
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        response.raise_for_status()
        return CachePublic.model_validate(response.json())

    async def delete_cache(self, cache_id: UUID) -> None:
        response = await self._http.delete(
            f"/caches/{cache_id}", headers=self._auth_headers()
        )
        if response.status_code == httpx.codes.NOT_FOUND:
            return
        response.raise_for_status()

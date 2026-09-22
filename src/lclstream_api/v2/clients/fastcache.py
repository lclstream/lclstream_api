import logging
import ssl
from pathlib import Path
from uuid import UUID

import httpx
from pydantic import AnyUrl, BaseModel

from .. import config
from ..config import FastcacheClientSettings
from ..core import transfer as tcore

logger = logging.getLogger(__name__)


class CacheConfig(BaseModel):
    """Cache config from fastcache_api (TODO: hand-mirrored, drifts silently)."""

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
    """POST body for creating a cache (TODO: hand-mirrored, drifts silently)."""

    key: str
    requested_by: str
    idle_timeout_ms: int | None = None
    output: str = "push"


class CachePublic(BaseModel):
    """Cache row from fastcache_api (TODO: hand-mirrored, drifts silently)."""

    id: UUID
    key: str | None
    user: str
    state: tcore.CacheState
    log_path: Path
    config: CacheConfig


def _raise_for_status(response: httpx.Response) -> None:
    """httpx's raise_for_status drops the body, which is the only place
    fastcache_api puts the reason ('no free cache ports', the EACCES, ...).
    Keeping it turns an opaque 503 into an actionable transfer transition."""
    if not response.is_error:
        return
    detail = response.text.strip()
    logger.error(
        "fastcache %s %s -> %s %s",
        response.request.method,
        response.request.url,
        response.status_code,
        detail,
    )
    raise httpx.HTTPStatusError(
        f"fastcache_api {response.status_code} for "
        f"{response.request.method} {response.request.url}: {detail}",
        request=response.request,
        response=response,
    )


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

    async def aclose(self) -> None:
        await self._http.aclose()

    async def create_cache(
        self,
        *,
        key: str,
        requested_by: str,
        idle_timeout_ms: int | None = None,
        output: str = "push",
    ) -> CachePublic:
        body = CacheCreate(
            key=key,
            requested_by=requested_by,
            idle_timeout_ms=idle_timeout_ms,
            output=output,
        )
        response = await self._http.post(
            "/caches/",
            json=body.model_dump(mode="json", exclude_none=True),
        )
        _raise_for_status(response)
        return CachePublic.model_validate(response.json())

    async def get_cache(self, cache_id: UUID) -> CachePublic | None:
        response = await self._http.get(f"/caches/{cache_id}")
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        _raise_for_status(response)
        return CachePublic.model_validate(response.json())

    async def delete_cache(self, cache_id: UUID) -> None:
        response = await self._http.delete(f"/caches/{cache_id}")
        if response.status_code == httpx.codes.NOT_FOUND:
            return
        _raise_for_status(response)


_client: FastcacheClient | None = None


def startup() -> None:
    global _client
    _client = FastcacheClient(config.get_fastcache())


async def shutdown() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
    _client = None


def client() -> FastcacheClient:
    if _client is None:
        raise RuntimeError("fastcache client not initialized; call clients.startup()")
    return _client

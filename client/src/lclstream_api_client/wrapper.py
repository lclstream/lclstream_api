from collections.abc import Callable
from uuid import UUID

from ._generated.api.caches_api import CachesApi
from ._generated.api.transfers_api import TransfersApi
from ._generated.api_client import ApiClient
from ._generated.configuration import Configuration
from ._generated.models.cache_mode import CacheMode
from ._generated.models.caches_public import CachesPublic
from ._generated.models.job_spec import JobSpec
from ._generated.models.log_read_mode import LogReadMode
from ._generated.models.log_stream import LogStream
from ._generated.models.message import Message
from ._generated.models.transfer_create import TransferCreate
from ._generated.models.transfer_detail import TransferDetail
from ._generated.models.transfer_log_index import TransferLogIndex
from ._generated.models.transfer_public import TransferPublic
from ._generated.models.transfer_state import TransferState
from ._generated.models.transfers_public import TransfersPublic
from .params import Parameters

Token = str | Callable[[], str]


def _resolve_token(token: Token) -> str:
    return token() if callable(token) else token


class LclstreamApiClient:
    def __init__(self, base_url: str, token: Token) -> None:
        self._token = token
        self._configuration = Configuration(host=base_url)
        api_client = ApiClient(self._configuration)
        self._api = TransfersApi(api_client)
        self._caches_api = CachesApi(api_client)

    def _authed(self) -> TransfersApi:
        self._configuration.access_token = _resolve_token(self._token)
        return self._api

    def _authed_caches(self) -> CachesApi:
        self._configuration.access_token = _resolve_token(self._token)
        return self._caches_api

    def list_transfers(
        self,
        *,
        skip: int | None = None,
        limit: int | None = None,
        state: TransferState | None = None,
    ) -> TransfersPublic:
        return self._authed().get_transfers_transfers_get_sync(
            skip=skip, limit=limit, state=state
        )

    def get_transfer(self, transfer_id: UUID) -> TransferDetail:
        return self._authed().get_transfer_transfers_transfer_id_get_sync(transfer_id)

    def create_transfer(
        self,
        parameters: Parameters,
        *,
        cache_mode: CacheMode | None = None,
        job_spec_override: JobSpec | None = None,
    ) -> TransferPublic:
        return self._authed().new_transfer_transfers_post_sync(
            TransferCreate(
                parameters=parameters.to_generated(),
                cache_mode=cache_mode,
                job_spec_override=job_spec_override,
            )
        )

    def cancel_transfer(self, transfer_id: UUID) -> Message:
        return self._authed().cancel_transfer_transfers_transfer_id_delete_sync(
            transfer_id
        )

    def list_transfer_logs(self, transfer_id: UUID) -> TransferLogIndex:
        return self._authed().get_transfer_logs_transfers_transfer_id_logs_get_sync(
            transfer_id
        )

    def get_transfer_log(
        self,
        transfer_id: UUID,
        stream: LogStream,
        *,
        mode: LogReadMode | None = None,
        lines: int | None = None,
        bytes_: int | None = None,
    ) -> str:
        return (
            self._authed().get_transfer_log_transfers_transfer_id_logs_stream_get_sync(
                transfer_id, stream, mode=mode, lines=lines, bytes=bytes_
            )
        )

    def list_caches(self, experiment: str) -> CachesPublic:
        """The active shared (long-lived) cache for an experiment, if any."""
        return self._authed_caches().get_caches_caches_get_sync(experiment)

    def shutdown_cache(self, cache_id: UUID, *, force: bool | None = None) -> Message:
        return self._authed_caches().shutdown_cache_caches_cache_id_delete_sync(
            cache_id, force=force
        )


class AsyncLclstreamApiClient:
    def __init__(self, base_url: str, token: Token) -> None:
        self._token = token
        self._configuration = Configuration(host=base_url)
        api_client = ApiClient(self._configuration)
        self._api = TransfersApi(api_client)
        self._caches_api = CachesApi(api_client)

    def _authed(self) -> TransfersApi:
        self._configuration.access_token = _resolve_token(self._token)
        return self._api

    def _authed_caches(self) -> CachesApi:
        self._configuration.access_token = _resolve_token(self._token)
        return self._caches_api

    async def aclose(self) -> None:
        await self._api.api_client.close()

    async def list_transfers(
        self,
        *,
        skip: int | None = None,
        limit: int | None = None,
        state: TransferState | None = None,
    ) -> TransfersPublic:
        return await self._authed().get_transfers_transfers_get(
            skip=skip, limit=limit, state=state
        )

    async def get_transfer(self, transfer_id: UUID) -> TransferDetail:
        return await self._authed().get_transfer_transfers_transfer_id_get(transfer_id)

    async def create_transfer(
        self,
        parameters: Parameters,
        *,
        cache_mode: CacheMode | None = None,
        job_spec_override: JobSpec | None = None,
    ) -> TransferPublic:
        return await self._authed().new_transfer_transfers_post(
            TransferCreate(
                parameters=parameters.to_generated(),
                cache_mode=cache_mode,
                job_spec_override=job_spec_override,
            )
        )

    async def cancel_transfer(self, transfer_id: UUID) -> Message:
        return await self._authed().cancel_transfer_transfers_transfer_id_delete(
            transfer_id
        )

    async def list_transfer_logs(self, transfer_id: UUID) -> TransferLogIndex:
        return await self._authed().get_transfer_logs_transfers_transfer_id_logs_get(
            transfer_id
        )

    async def get_transfer_log(
        self,
        transfer_id: UUID,
        stream: LogStream,
        *,
        mode: LogReadMode | None = None,
        lines: int | None = None,
        bytes_: int | None = None,
    ) -> str:
        return (
            await self._authed().get_transfer_log_transfers_transfer_id_logs_stream_get(
                transfer_id, stream, mode=mode, lines=lines, bytes=bytes_
            )
        )

    async def list_caches(self, experiment: str) -> CachesPublic:
        """The active shared (long-lived) cache for an experiment, if any."""
        return await self._authed_caches().get_caches_caches_get(experiment)

    async def shutdown_cache(
        self, cache_id: UUID, *, force: bool | None = None
    ) -> Message:
        return await self._authed_caches().shutdown_cache_caches_cache_id_delete(
            cache_id, force=force
        )

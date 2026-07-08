from collections.abc import Callable
from uuid import UUID

from ._generated.api.transfers_api import TransfersApi
from ._generated.api_client import ApiClient
from ._generated.configuration import Configuration
from ._generated.models.log_read_mode import LogReadMode
from ._generated.models.log_stream import LogStream
from ._generated.models.message import Message
from ._generated.models.parameters import Parameters
from ._generated.models.transfer_create import TransferCreate
from ._generated.models.transfer_detail import TransferDetail
from ._generated.models.transfer_log_index import TransferLogIndex
from ._generated.models.transfer_public import TransferPublic
from ._generated.models.transfer_state import TransferState
from ._generated.models.transfers_public import TransfersPublic

Token = str | Callable[[], str]


def _resolve_token(token: Token) -> str:
    return token() if callable(token) else token


class LclstreamApiClient:
    def __init__(self, base_url: str, token: Token) -> None:
        self._token = token
        self._configuration = Configuration(host=base_url)
        self._api = TransfersApi(ApiClient(self._configuration))

    def _authed(self) -> TransfersApi:
        self._configuration.access_token = _resolve_token(self._token)
        return self._api

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

    def create_transfer(self, parameters: Parameters) -> TransferPublic:
        return self._authed().new_transfer_transfers_post_sync(
            TransferCreate(parameters=parameters)
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


class AsyncLclstreamApiClient:
    def __init__(self, base_url: str, token: Token) -> None:
        self._token = token
        self._configuration = Configuration(host=base_url)
        self._api = TransfersApi(ApiClient(self._configuration))

    def _authed(self) -> TransfersApi:
        self._configuration.access_token = _resolve_token(self._token)
        return self._api

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

    async def create_transfer(self, parameters: Parameters) -> TransferPublic:
        return await self._authed().new_transfer_transfers_post(
            TransferCreate(parameters=parameters)
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

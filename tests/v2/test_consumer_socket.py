"""The consumer's socket is published contract: fastcache types 4/5/6 bind
PUSH/ROUTER/REP, so a consumer that dials PULL against a REP cache hangs.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest

from lclstream_api.v2 import service
from lclstream_api.v2.core.enums import CacheMode, ConsumerSocket, TransferState
from lclstream_api.v2.models import TransferPublic

TRANSFER_ID = UUID("12345678-1234-5678-1234-567812345678")
NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("socket", "output"),
    [
        (ConsumerSocket.pull, "push"),
        (ConsumerSocket.dealer, "router"),
        (ConsumerSocket.req, "rep"),
    ],
)
def test_cache_output_mapping(socket: ConsumerSocket, output: str) -> None:
    assert socket.cache_output == output


def _transfer(state: TransferState, socket: ConsumerSocket) -> SimpleNamespace:
    return SimpleNamespace(
        id=TRANSFER_ID,
        created_at=NOW,
        owner_email="owner@example.org",
        state=state,
        experiment="mfxl1001",
        run="42",
        cache_mode=CacheMode.per_transfer,
        consumer_socket=socket,
        cache_hostname="dtn-01",
        push_port=5555,
    )


@pytest.mark.parametrize("socket", list(ConsumerSocket))
def test_ready_transfer_publishes_socket(socket: ConsumerSocket) -> None:
    public = TransferPublic.from_transfer(_transfer(TransferState.ready, socket))

    assert public.connection_info is not None
    assert public.connection_info.socket is socket
    assert public.connection_info.uri == "tcp://dtn-01:5555"


def test_unready_transfer_has_no_connection_info() -> None:
    """Ports mean nothing until both cache and producer are up."""
    public = TransferPublic.from_transfer(
        _transfer(TransferState.provisioning, ConsumerSocket.req)
    )

    assert public.connection_info is None


def test_consumer_host_is_remapped_for_offsite_clients() -> None:
    """Off-site clients cannot resolve DTN names, so hand them an address."""
    public = TransferPublic.from_transfer(
        _transfer(TransferState.ready, ConsumerSocket.req),
        {"dtn-01": "198.51.100.7"},
    )

    assert public.connection_info is not None
    assert public.connection_info.uri == "tcp://198.51.100.7:5555"
    assert public.connection_info.host == "198.51.100.7"


def test_unmapped_cache_host_is_left_alone() -> None:
    public = TransferPublic.from_transfer(
        _transfer(TransferState.ready, ConsumerSocket.req),
        {"dtn-99": "198.51.100.7"},
    )

    assert public.connection_info is not None
    assert public.connection_info.uri == "tcp://dtn-01:5555"


@pytest.mark.asyncio
async def test_resolution_is_skipped_when_disabled(monkeypatch) -> None:
    """The toggle is the escape hatch if a DTN resolves differently inside."""
    monkeypatch.setattr(
        service.config,
        "get_fastcache",
        lambda: SimpleNamespace(resolve_consumer_host=False),
    )
    assert await service._consumer_hosts("dtn-01") == {}


@pytest.mark.asyncio
async def test_resolution_skips_names_that_do_not_resolve(monkeypatch) -> None:
    """A dead name must fall back to itself, not blow up the listing."""
    monkeypatch.setattr(
        service.config,
        "get_fastcache",
        lambda: SimpleNamespace(resolve_consumer_host=True),
    )
    hosts = await service._consumer_hosts("no-such-host.invalid", None)
    assert hosts == {}

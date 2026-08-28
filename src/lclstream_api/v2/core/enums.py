from enum import StrEnum


class TransferState(StrEnum):
    provisioning = "provisioning"  # spinning up cache + producer
    ready = "ready"  # both active, connection_info available
    canceling = "canceling"  # cancel requested, teardown in progress
    canceled = "canceled"
    completed = "completed"
    failed = "failed"

    def is_final(self) -> bool:
        return self in (
            TransferState.canceled,
            TransferState.completed,
            TransferState.failed,
        )


class TransitionSource(StrEnum):
    producer = "producer"
    cache = "cache"
    user = "user"
    orchestrator = "orchestrator"  # from lclstream_api


class CacheMode(StrEnum):
    # One cache per transfer (default).
    per_transfer = "per_transfer"
    # One cache shared per experiment.
    shared = "shared"


class ConsumerSocket(StrEnum):
    """ZMQ socket the consumer connects with.

    ``dealer`` and ``req`` are request-driven: the consumer asks for each
    message. fastcache only reports metrics under ``pull``.
    """

    pull = "pull"  # fastcache pushes; the default
    dealer = "dealer"  # fastcache binds ROUTER
    req = "req"  # fastcache binds REP

    @property
    def cache_output(self) -> str:
        """fastcache_api's matching CacheOutput value."""
        return _CACHE_OUTPUT[self]


_CACHE_OUTPUT = {
    ConsumerSocket.pull: "push",
    ConsumerSocket.dealer: "router",
    ConsumerSocket.req: "rep",
}


class CacheState(StrEnum):
    """Observed fastcache cache lifecycle."""

    new = "new"
    queued = "queued"
    active = "active"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"

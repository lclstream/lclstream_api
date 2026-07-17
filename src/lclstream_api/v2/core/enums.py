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


class CacheState(StrEnum):
    """Observed fastcache cache lifecycle."""

    new = "new"
    queued = "queued"
    active = "active"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"

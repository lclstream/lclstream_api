from psik.models import JobState
from pydantic import BaseModel


class CacheRequest(BaseModel):
    # supervisor need to know transfer_id for idempotency
    transfer_id: str


class CacheInfo(BaseModel):
    """Poll response from supervisor"""

    transfer_id: str
    host: str

    # fastcache inurl (ZMQ_PULL bind); the producer PUSHes here.
    pull_port: int | None = None
    pull_uri: str | None = None

    # fastcache outurl (ZMQ_PUSH bind); the consumer PULLs here.
    push_port: int | None = None
    push_uri: str | None = None

    state: JobState

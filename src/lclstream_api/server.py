import logging
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Any

from fastapi import FastAPI

from .config import load_config
from .routers.callback import callback
from .routers.transfer import transfers

_logger = logging.getLogger(__name__)
version_tag = version(__package__)

description = """
Access your psana(2) data remotely.
Configure detectors, and request a download of packed,
assembled events computed at S3DF.
"""

tags_metadata: list[dict[str, Any]] = [
    {"name": "transfers", "description": "LCLStreamer data transfers"},
]

api = FastAPI(
    title="LCLStream API",
    # lifespan = lifespan,
    openapi_url="/openapi.json",
    root_path="/v1",
    docs_url="/",
    description=description,
    summary="An API for psana(2) data.",
    version=version_tag,
    # terms_of_service="You're on your own here.",
    # contact={
    #    "name": "",
    #    "url": "",
    #    "email": "help@lclstream.local",
    # },
    openapi_tags=tags_metadata,
    responses={404: {"description": "Not found"}},
)

api.include_router(
    callback,
    prefix="/callback",
    tags=["callback"],
)
api.include_router(
    transfers,
    prefix="/transfers",
    tags=["transfers"],
)


@asynccontextmanager
async def lifespan(app):
    _logger.info("Loading config.")
    config = load_config()
    # Setup activities
    # setup_security(config.authz)
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/v1", api)

try:
    from certified.formatter import log_request  # type: ignore[import-not-found]

    app.middleware("http")(log_request)
except ImportError:
    pass

"""
import signal

def cleanup():
    # Cleanup function
    pass

def handle_exit(sig, frame):
    # Additional signal handling for manual interruption
    cleanup()

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

@app.on_event("shutdown")
async def shutdown_event():
    # Register cleanup with FastAPI shutdown event
    cleanup()
"""

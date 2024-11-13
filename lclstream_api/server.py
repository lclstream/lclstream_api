#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# run as:
#
#     uvicorn --host localhost --port 5001 lclstream.server:app
#
# or (preferred):
#
#     certified serve lclstream.server:app https://0.0.0.0:5001
#

from typing import Dict, List, Any, Optional
import logging
from importlib.metadata import version
_logger = logging.getLogger(__name__)
version_tag = version(__package__)

from fastapi import FastAPI, HTTPException

from .config import load_config
from .routers.transfer import transfers

description = """
Access your psana data remotely.
Configure detectors, and request a download of packed,
assembled events computed at S3DF.
"""

tags_metadata : List[Dict[str, Any]] = [
    { "name": "transfers",
      "description": "psana2h5 data transfers"
    },
]

api = FastAPI(
        title = "LCLStream API",
        lifespan = lifespan,
        openapi_url   = "/openapi.json",
        root_path     = "/v1",
        docs_url      = "/",
        description   = description,
        summary      = "An API for psana data.",
        version       = version_tag,
        #terms_of_service="You're on your own here.",
        #contact={
        #    "name": "",
        #    "url": "",
        #    "email": "help@lclstream.local",
        #},
        openapi_tags  = tags_metadata,
        responses     = {404: {"description": "Not found"}},
    )

api.include_router(
    transfers,
    prefix="/transfers",
    tags = ["transfers"],
)

app = FastAPI()
app.mount("/v1", api)

try:
    from certified.formatter import log_request # type: ignore[import-not-found]
    app.middleware("http")(log_request)
except ImportError:
    pass

@app.on_event("startup")
async def setup_config_event():
    _logger.info("Loading config.")
    config = load_config()
    # Setup activities
    #setup_security(config.authz)

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

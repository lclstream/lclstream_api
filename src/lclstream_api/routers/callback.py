import logging
from typing import Annotated

import psik
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Header,
    HTTPException,
    Request,
)

from ..models import ClientName
from ..xfer_db import Database

_logger = logging.getLogger(__name__)

callback = APIRouter(responses={401: {"description": "Unauthorized"}})

async def handle_callback(
    client: ClientName,
    cb: psik.Callback,
    db: Database,
    request: Request,
    bg_tasks: BackgroundTasks,
    x_hub_signature_256: Annotated[str | None, Header()] = None,
) -> bool:
    try:
        xfer, job = db.lookup_job(client, cb.jobid)
    except KeyError:
        raise HTTPException(404, "Job not found.")
    if job is None:
        # xfer found, but job is terminated already
        raise HTTPException(404, "Job not found.")

    if job.spec.cb_secret:
        if x_hub_signature_256 is None:
            raise HTTPException(
                status_code=403, detail="x-hub-signature-256 header is missing!"
            )
        try:
            body = (await request.body()).decode("utf-8")
        except AttributeError:  # aiohttp uses read()
            body = (await request.read()).decode("utf-8")  # type: ignore[attr-defined]
            # body = cb.model_dump_json()
        psik.web.verify_signature(
            body, job.spec.cb_secret.get_secret_value(), x_hub_signature_256
        )

    bg_tasks.add_task(
        xfer.transition,
        client,
        cb.state,
        cb.jobndx,
        cb.info,
    )

    return True

@callback.post("/producer")
async def producer_callback(
    cb: psik.Callback,
    db: Database,
    request: Request,
    bg_tasks: BackgroundTasks,
    x_hub_signature_256: Annotated[str | None, Header()] = None,
) -> bool:
    return await handle_callback(ClientName.producer, cb, db, request, bg_tasks, x_hub_signature_256)

@callback.post("/forwarder")
async def forwarder_callback(
    cb: psik.Callback,
    db: Database,
    request: Request,
    bg_tasks: BackgroundTasks,
    x_hub_signature_256: Annotated[str | None, Header()] = None,
) -> bool:
    return await handle_callback(ClientName.cache, cb, db, request, bg_tasks, x_hub_signature_256)

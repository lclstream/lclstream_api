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
from ..ports import Database

_logger = logging.getLogger(__name__)

callback = APIRouter(responses={401: {"description": "Unauthorized"}})


@callback.post("")
async def post_callback(
    cb: psik.Callback,
    db: Database,
    request: Request,
    bg_tasks: BackgroundTasks,
    x_hub_signature_256: Annotated[str | None, Header()] = None,
) -> bool:
    try:
        entry = db[cb.jobid]
    except KeyError:
        raise HTTPException(404, "Job not found.")

    job = entry.job
    if job is None:
        return False

    if job.spec.client_secret:
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
            body, job.spec.client_secret.get_secret_value(), x_hub_signature_256
        )

    bg_tasks.add_task(
        entry.transition,
        ClientName.producer,
        cb.state,
        cb.jobndx,
        str(cb.info),
        job,
    )

    return True

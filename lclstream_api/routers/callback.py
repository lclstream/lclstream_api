from typing import Optional, List
from typing_extensions import Annotated
from pathlib import Path
import logging
_logger = logging.getLogger(__name__)

from fastapi import (
    APIRouter,
    HTTPException,
    BackgroundTasks,
    Depends,
    Request,
    Header,
)
import psik

from ..ports import Database

callback = APIRouter(responses={
        401: {"description": "Unauthorized"}})

@callback.post("")
async def post_callback(cb: psik.Callback,
                        db: Database,
                        request: Request,
                        x_hub_signature_256: Annotated[Optional[str], Header()]
                                            = None) -> bool:
    try:
        entry = db[cb.jobid]
    except KeyError:
        raise HTTPException(404, "Job not found.")

    job = entry.job
    if job is None:
        return False

    if job.spec.client_secret:
        if x_hub_signature_256 is None:
            raise HTTPException(status_code=403, detail="x-hub-signature-256 header is missing!")
        try:
            body = (await request.body()).decode("utf-8")
        except AttributeError: # aiohttp uses read()
            body = (await request.read()).decode("utf-8") # type: ignore[attr-defined]
            #body = cb.model_dump_json()
        psik.web.verify_signature(
                   body,
                   job.spec.client_secret.get_secret_value(),
                   x_hub_signature_256)

    ok = await job.reached(cb.jobndx, cb.state, cb.info)
    if not ok:
        return False
    return True

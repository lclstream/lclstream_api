import psik
import pytest
import svcs
from psik.models import Callback, JobState
from psik.web import sign_message
from pydantic import SecretStr

from lclstream_api.core.models import ClientName
from lclstream_api.transfer_mgr import Transfer
from lclstream_api.xfer_db import XferDatabase

SECRET = "secret token"
JSON = {"content-type": "application/json"}


async def add_transfer(
    client, name: ClientName, cb_secret: str | None = None
) -> tuple[Transfer, psik.Job]:
    """Track one transfer whose job for name exists but has not run."""
    with svcs.Container(svcs.fastapi.get_registry(client)) as services:
        db, mgr = services.get(XferDatabase, psik.JobManager)

    secret = SecretStr(cb_secret) if cb_secret else None
    job = await mgr.create(psik.JobSpec(script="true", cb_secret=secret))

    xfer = Transfer(eid=1)
    xfer.transition(name, JobState.new, job=job)
    db.add(xfer.eid, xfer)
    return xfer, job


def queued(job: psik.Job) -> str:
    cb = Callback(jobid=job.stamp, jobndx=0, state=JobState.queued, info="ok")
    return cb.model_dump_json()


def test_invalid_body(client):
    response = client.post(
        "/callback/producer", content='{"name": "hello"}', headers=JSON
    )
    assert response.status_code == 422


def test_unknown_job(client):
    cb = Callback(jobid="123.456", jobndx=0, state=JobState.queued, info="ok")
    response = client.post(
        "/callback/producer", content=cb.model_dump_json(), headers=JSON
    )
    assert response.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "route"),
    [
        (ClientName.producer, "/callback/producer"),
        (ClientName.cache, "/callback/forwarder"),
    ],
)
async def test_transition(client, name, route):
    xfer, job = await add_transfer(client, name)

    response = client.post(route, content=queued(job), headers=JSON)
    assert response.status_code == 200
    assert xfer.states[name] == JobState.queued


@pytest.mark.asyncio
async def test_missing_signature(client):
    xfer, job = await add_transfer(client, ClientName.producer, cb_secret=SECRET)

    response = client.post("/callback/producer", content=queued(job), headers=JSON)
    assert response.status_code == 403
    assert xfer.states[ClientName.producer] == JobState.new


@pytest.mark.asyncio
async def test_signed_transition(client):
    xfer, job = await add_transfer(client, ClientName.producer, cb_secret=SECRET)
    body = queued(job)

    response = client.post(
        "/callback/producer",
        content=body,
        headers={**JSON, "x-hub-signature-256": sign_message(body, SECRET)},
    )
    assert response.status_code == 200
    assert xfer.states[ClientName.producer] == JobState.queued


@pytest.mark.asyncio
async def test_bad_signature(client):
    xfer, job = await add_transfer(client, ClientName.producer, cb_secret=SECRET)
    body = queued(job)

    response = client.post(
        "/callback/producer",
        content=body,
        headers={**JSON, "x-hub-signature-256": sign_message(body, "wrong")},
    )
    assert response.status_code == 403
    assert xfer.states[ClientName.producer] == JobState.new

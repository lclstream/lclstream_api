import json

import psik
import pytest
import pytest_asyncio
from aiohttp import web
from fastapi import HTTPException
from psik.models import Callback, JobState
from psik.web import post_json

from lclstream_api.xfer_db import get_database
from lclstream_api.routers.callback import (
    producer_callback,
    forwarder_callback,
)
from lclstream_api.models import ClientName

### test fixture for accepting a callback ###
cb_value = web.AppKey("value", None)  # type: ignore[var-annotated]

from test_config import config


class MockBackgroundTasks(list):
    def add_task(self, task, *args):
        self.append((task, args))

    async def run_tasks(self):
        for task, args in self:
            await task(*args)


async def post_cb(name: ClientName, request, config):
    if request.method != "POST":
        raise KeyError(request.method)

    body = await request.read()

    cb = psik.Callback.model_validate_json(body)
    request.app[cb_value] = cb
    x_hub_signature_256 = request.headers.get("x_hub_signature_256", None)
    db = get_database()

    bg_tasks = MockBackgroundTasks()

    try:
        if name == ClientName.producer:
            ans = await producer_callback( cb, db, request, bg_tasks, x_hub_signature_256) # type: ignore[arg-type]
        else:
            ans = await forwarder_callback( cb, db, request, bg_tasks, x_hub_signature_256) # type: ignore[arg-type]
    except HTTPException:
        return web.Response(text='"false"', status=200)
    assert len(bg_tasks) == 1
    await bg_tasks.run_tasks()

    # return ans
    # body = await request.post()
    # print(f"test cb received: {body}")
    return web.Response(
        text=json.dumps(ans), content_type="application/json", status=200
    )


@pytest_asyncio.fixture
async def cb_client(aiohttp_client, config):
    app = web.Application()

    async def run_prod_cb(request):
        return await post_cb(ClientName.producer, request, config)
    async def run_fwd_cb(request):
        return await post_cb(ClientName.cache, request, config)

    app.router.add_post("/callback/producer", run_prod_cb)
    app.router.add_post("/callback/forwarder", run_fwd_cb)
    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_local_cb(cb_client):
    cb_server = cb_client.server
    cb_server.app[cb_value] = None

    ans = await post_json(
        str(cb_server.make_url("/callback/producer")),
        '{"name": "hello", "script": "echo hello; pwd"}',
        "secret token",
    )
    assert ans is None  # should fail to parse
    assert cb_server.app[cb_value] is None

    cb = Callback(jobid="123.456", jobndx=0, state=JobState.queued, info="ok")
    ans = await post_json(str(cb_server.make_url("/callback/producer")), cb.model_dump_json())
    print(ans)
    assert ans is not None

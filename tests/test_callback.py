import json

import pytest
import pytest_asyncio

from fastapi import HTTPException

from aiohttp import web
from psik.web import post_json
from psik.models import Callback, JobState
import psik

from lclstream_api.routers import callback
from lclstream_api.ports import get_database

from test_config import config

### test fixture for accepting a callback ###
cb_value = web.AppKey("value", None) # type: ignore[var-annotated]

class MockBackgroundTasks(list):
    def add_task(self, task, *args):
        self.append((task, args))
    async def run_tasks(self):
        for task, args in self:
            await task(*args)

async def post_cb(request, config):
    request.app[cb_value] = None
    if request.method != "POST":
        raise KeyError(request.method)

    body = await request.read()

    cb = psik.Callback.model_validate_json(body)
    request.app[cb_value] = cb
    x_hub_signature_256 = request.headers.get("x_hub_signature_256", None)
    db = get_database(config)

    bg_tasks = MockBackgroundTasks()

    try:
        ans = await callback.post_callback(cb, db, request,
                                           bg_tasks,
                                           x_hub_signature_256)
    except HTTPException as e:
        return web.Response(text='"false"', status=200)
    assert len(bg_tasks) == 1
    await bg_tasks.run_tasks()

    #return ans
    #body = await request.post()
    #print(f"test cb received: {body}")
    return web.Response(text=json.dumps(ans),
                        content_type="application/json", status=200)

@pytest_asyncio.fixture
async def cb_client(aiohttp_client, config):
    app = web.Application()
    async def run_post_cb(request):
        return await post_cb(request, config)
    app.router.add_post("/callback", run_post_cb)
    return await aiohttp_client(app)

@pytest.mark.asyncio
async def test_local_cb(cb_client):
    cb_server = cb_client.server
    ans = await post_json(str(cb_server.make_url("/callback")),
                '{"name": "hello", "script": "echo hello; pwd"}',
                "secret token")
    assert ans is None # failed parse
    assert cb_server.app[cb_value] is None

    cb = Callback(jobid="123.456", jobndx=0,
                  state = JobState.queued,
                  info = 0)
    ans = await post_json(str(cb_server.make_url("/callback")),
                          cb.model_dump_json())
    print(ans)
    assert ans is not None


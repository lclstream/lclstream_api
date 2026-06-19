import asyncio
import json

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from lclstream.zmqsock import puller
from test_jobs import param2

from lclstream_api.models import TransferInfo, TransferStatus
from lclstream_api.server import api

from test_config import setup_lclstream_api, config

ADDR = "tcp://127.0.0.1:28451"

client = TestClient(api)


@pytest_asyncio.fixture
async def pull_server():
    async def run_pull(addr):
        # Sleep to allow the server boot-up.
        await asyncio.sleep(0.1)
        pull = puller(addr, 1)
        nmsg = 0
        for data in pull:
            nmsg += 1
        print(f"pull_server: received {nmsg} messages")

    # event_loop = asyncio.get_running_loop()
    # task = asyncio.ensure_future(run_pull(ADDR), loop=event_loop)
    task = asyncio.create_task(run_pull(ADDR))

    try:
        yield
    finally:
        task.cancel()
        try:
            ans = await task
        except asyncio.CancelledError:
            pass


def test_get_list(setup_lclstream_api):
    for path in ["/transfers", "/transfers/"]:
        response = client.get(path)
        assert response.status_code == 200
        resp = response.json()
        assert isinstance(resp, list)


# Doesn't work because of zmq threading / async incompat.
@pytest.mark.skip
@pytest.mark.asyncio
async def test_mk_transfer(pull_server, setup_lclstream_api):
    response = client.post("/transfers", json={"abc": 2})
    assert response.status_code == 422

    trs = json.loads(param2)
    response = client.post("/transfers", json=trs)
    # response = client.post("/transfers", body=param2)
    assert response.status_code == 200
    stat = TransferStatus.model_validate_json(response.text)
    assert stat.state == "new"
    tid = stat.id

    response = client.get(f"/transfers/{tid}")
    assert response.status_code == 200
    # state = response.json()
    info = TransferInfo.model_validate_json(response.text)
    # assert isinstance(state, list)
    # for i, s in enumerate(state):
    #    state[i] = TransferStatus.model_validate(s)
    print(f"Transfer info = {info}")

    response = client.delete(f"/transfers/{tid}")
    assert response.status_code == 200
    ok = response.json()
    assert ok is None


def test_read_transfer(setup_lclstream_api):
    response = client.get("/transfers/12")
    assert response.status_code == 404

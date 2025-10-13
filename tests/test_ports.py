import asyncio
import pytest

from lclstream_api.ports import get_database

from test_config import config

@pytest.mark.asyncio
async def test_db(config):
    DB = get_database(config)

    with pytest.raises(KeyError):
        await DB.delete("123")
    
    nopen = len(DB.open_ports)
    ent = await DB.create("job1", "tester")
    assert len(DB.open_ports) == nopen-1

    assert ent.user == "tester"
    assert ent.port > 1024
    assert ent.internal_url.startswith("tcp")
    assert ent.external_url.startswith("tcp")

    with pytest.raises(KeyError):
        job = DB["111"]

    with pytest.raises(KeyError):
        ent2 = await DB.create("job1", "not_tester")
    ent2 = await DB.create("job1", "tester")
    assert ent == ent2

    await asyncio.sleep(0.2)
    print(DB["job1"])

    ans = await DB.delete("job1")
    assert len(DB.open_ports) == nopen
    print(ans.log)

    assert ans.states["cache"].is_final()

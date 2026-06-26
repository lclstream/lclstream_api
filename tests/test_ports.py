import pytest

from lclstream_api.ports import get_portusage
from test_config import config  # noqa: F401


def test_db(config):
    DB = get_portusage(config)

    with pytest.raises(KeyError):
        DB.delete(123)

    nopen = len(DB.open_ports)
    ent = DB.create("user1")
    assert len(DB.open_ports) == nopen - 1

    assert ent.user == "user1"
    assert ent.port > 1024
    assert ent.internal_url.startswith("tcp")
    assert ent.external_url.startswith("tcp")

    with pytest.raises(KeyError):
        DB[111]

    ent1 = DB[ent.eid]
    assert ent1 == ent

    ent2 = DB.create("user2")
    assert len(DB.open_ports) == nopen - 2
    assert ent2.eid != ent1.eid
    print(DB[ent2.eid])

    DB.delete(ent.eid)
    assert len(DB.open_ports) == nopen - 1

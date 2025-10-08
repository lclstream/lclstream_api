import pytest

from lclstream_api.ports import get_database

def test_db():
    DB = get_database()

    with pytest.raises(KeyError):
        DB.delete("123")
    
    nopen = len(DB.open_ports)
    ent = DB.create("job1", "tester")
    assert len(DB.open_ports) == nopen-1

    assert ent.user == "tester"
    assert ent.port > 1024
    assert ent.internal_url.startswith("tcp")
    assert ent.external_url.startswith("tcp")

    with pytest.raises(KeyError):
        job = DB["111"]

    with pytest.raises(KeyError):
        ent2 = DB.create("job1", "not_tester")
    ent2 = DB.create("job1", "tester")
    assert ent == ent2

    DB.delete("job1")
    assert len(DB.open_ports) == nopen

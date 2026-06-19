import asyncio
from pathlib import Path

from lclstream_api.cache import cache_process, watch_cmd
from lclstream_api.config import to_mgr
from lclstream_api.jobs import create_job
from lclstream_api.lclstreamer_param import Parameters
from lclstream_api.models import (
    CacheMetrics,
    ClientName,
    JobState,
    PortEntry,
)


@pytest.mark.asyncio()
async def test_watch_cmd():
    entry = PortEntry(
        user="abc", port=11203, internal_url="internal", external_url="external"
    )
    metric = CacheMetrics(time=123, producers=1, recvd=2, sent=1, buffered=1)
    await watch_cmd("echo", metric.model_dump_json(), port=entry)
    assert entry.states[ClientName.cache] == JobState.completed
    assert len(entry.log) == 3
    assert entry.cache_metrics == metric

    # tested interactively to check parse is working as received
    if False:
        metric2 = CacheMetrics(time=124, producers=0, recvd=2, sent=2, buffered=0)
        entry = PortEntry(
            user="abc", port=11203, internal_url="internal", external_url="external"
        )
        await watch_cmd(
            "bash",
            "-c",
            "echo '%s'; sleep 1; echo '%s'"
            % (metric.model_dump_json(), metric2.model_dump_json()),
            port=entry,
        )


@pytest.mark.asyncio()
async def test_cache_job_complete(unused_tcp_port_factory, config):
    port1 = unused_tcp_port_factory()
    port2 = unused_tcp_port_factory()

    entry = PortEntry(
        user="abc",
        port=port1,
        internal_url=f"tcp://127.0.0.1:{port1}",
        external_url=f"tcp://127.0.0.1:{port2}",
    )
    metric = CacheMetrics(time=123, producers=1, recvd=2, sent=1, buffered=1)

    task = await cache_process(config.run_cache, port=entry)

    req = Parameters.model_validate_json(param2)
    mgr = to_mgr(config)
    spec = create_job(req, entry.internal_url, config)
    job = await mgr.create(spec)

    (Path(job.spec.directory) / "lclstreamer.json").write_text(
        req.model_dump_json(indent=2)
    )

    await entry.transition(
        ClientName.producer, JobState.new, jobndx=0, info="", job=job
    )
    await job.submit()

    async def run_pull(addr):
        await asyncio.sleep(0.1)
        ctx = zmq.Context()
        sock = ctx.socket(zmq.PULL)
        sock.setsockopt(zmq.RCVTIMEO, 5000)
        sock.connect(addr)
        nmsg = 0
        try:
            while True:
                sock.recv()
                nmsg += 1
        except zmq.Again:
            pass
        finally:
            sock.close()
            ctx.term()
        print(f"pull_server: received {nmsg} messages")

    await run_pull(entry.external_url)

    await task
    print("task complete. Log:")
    print(entry.log)
    assert entry.states[ClientName.cache] == JobState.completed
    assert entry.states[ClientName.producer].is_final()

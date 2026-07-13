#!/usr/bin/env python3
"""Submit a live lclstreamer transfer via lclstream_api_client and pull the
resulting ZMQ stream directly in this process.

    uv run main.py
"""

import asyncio
import contextlib
import json
import time
from pathlib import Path
from typing import Any
from uuid import UUID

import zmq
import zmq.asyncio
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from lclstream_api_client import AsyncLclstreamApiClient, CacheMode, exceptions, params

FINAL_STATES = {"canceled", "completed", "failed"}
POLL_TIMEOUT_S = 180.0
POLL_INTERVAL_S = 5.0


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LCLSTREAM_EXAMPLE_", env_file=".env")

    api_url: AnyHttpUrl = AnyHttpUrl(
        "https://lcls-data-portal.slac.stanford.edu/lclstream-dev"
    )
    token_file: Path
    source_identifier: str = (
        "exp=mfx100848724,run=51,dir=/sdf/data/lcls/ds/prj/public01/xtc"
    )

    @property
    def token(self) -> str:
        """Read the bearer token from file (fresh each call)."""
        try:
            token = self.token_file.read_text().strip()
        except FileNotFoundError:
            raise SystemExit(
                f"Token file not found: {self.token_file}\n"
                "Run ./scripts/dev-token.py to mint one (see .env.example)."
            ) from None
        if token.lower().startswith("bearer "):
            token = token.split(None, 1)[1].strip()
        if not token:
            raise SystemExit(
                f"Token file is empty: {self.token_file}\n"
                "Run ./scripts/dev-token.py to mint one (see .env.example)."
            )
        return token


def default_parameters(source_identifier: str) -> params.Parameters:
    """The public psana2 jungfrau/Simplon payload used for manual smoke tests."""
    return params.Parameters(
        source_identifier=source_identifier,
        skip_incomplete_events=True,
        event_source=params.Psana2EventSourceParameters(type="Psana2EventSource"),
        data_sources={
            "timestamp": params.Psana2TimestampParameters(type="Psana2Timestamp"),
            "detector_data": params.Psana2DetectorInterfaceParameters(
                type="Psana2DetectorInterface",
                psana_name="jungfrau",
                psana_fields="raw.calib",
                dtype="float64",
            ),
            "photon_wavelength": params.Psana2DetectorInterfaceParameters(
                type="Psana2DetectorInterface",
                psana_name="SIOC:SYS0:ML00:AO192",
            ),
            "detector_geometry": params.Psana2DetectorInterfaceParameters(
                type="Psana2DetectorInterface",
                psana_name="jungfrau",
                psana_fields=["_detid", "raw._det_geotxt_default"],
                dtype="str",
            ),
            "beam_data": params.Psana2DetectorInterfaceParameters(
                type="Psana2DetectorInterface",
                psana_name="ebeamh",
                psana_fields=[
                    "raw.ebeamUndAngX",
                    "raw.ebeamUndAngY",
                    "raw.ebeamUndPosX",
                    "raw.ebeamUndPosY",
                    "raw.ebeamL3Energy",
                ],
            ),
            "run_info": params.Psana2RunInfoParameters(type="Psana2RunInfo"),
        },
        processing_pipeline=params.BatchProcessingPipelineParameters(
            type="BatchProcessingPipeline", batch_size=1
        ),
        data_serializer=params.SimplonBinarySerializerParameters(
            type="SimplonBinarySerializer",
            data_source_to_serialize="jungfrau.raw.calib",
            polarization_fraction=0.0,
            polarization_axis=[1.0, 0.0, 0.0],
            data_collection_rate="120Hz",
            detector_name="jungfrau",
            detector_type="AreaDetector",
        ),
        data_handlers=[
            params.BinaryDataStreamingDataHandlerParameters(
                type="BinaryDataStreamingDataHandler",
                # lclstream-api overwrites this with the fastcache socket.
                urls=["tcp://127.0.0.1:1"],
                distribute=False,
                buffer=0,
                role="client",
            )
        ],
    )


def print_model(label: str, model: Any) -> None:
    print(f"{label}:")
    print(
        json.dumps(model.model_dump(mode="json"), indent=2, sort_keys=True, default=str)
    )


async def pull_stream(uri: str) -> None:
    """Connect a ZMQ PULL socket to the cache's push socket and consume it.

    Mirrors lclstream's `zmqsock.puller`: the cache binds a PUSH socket, so a
    consumer connects (dials out) with a PULL socket -- no inbound
    connectivity required on this side.
    """
    print(f"[zmq] connecting PULL socket to {uri}")
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.PULL)
    socket.connect(uri)

    count = 0
    total_bytes = 0
    start = time.monotonic()
    try:
        while True:
            frame = await socket.recv()
            count += 1
            total_bytes += len(frame)
            if count == 1 or count % 10 == 0:
                elapsed = max(time.monotonic() - start, 1e-9)
                print(
                    f"[zmq] received {count} messages, "
                    f"{total_bytes / 1024**2:.2f} MiB total, "
                    f"{total_bytes / elapsed / 1024**2:.2f} MiB/s"
                )
    except asyncio.CancelledError:
        raise
    finally:
        elapsed = max(time.monotonic() - start, 1e-9)
        print(
            f"[zmq] stopped after {count} messages, "
            f"{total_bytes / 1024**2:.2f} MiB total, "
            f"{total_bytes / elapsed / 1024**2:.2f} MiB/s"
        )
        socket.close(linger=0)
        ctx.term()


async def run(settings: Settings) -> int:
    client = AsyncLclstreamApiClient(
        base_url=str(settings.api_url), token=lambda: settings.token
    )

    transfer_id: UUID | None = None
    pull_task: asyncio.Task[None] | None = None
    last_state: str | None = None
    try:
        parameters = default_parameters(settings.source_identifier)
        try:
            created = await client.create_transfer(
                parameters, cache_mode=CacheMode.SHARED
            )
        except exceptions.ApiException as exc:
            raise SystemExit(
                f"create_transfer failed: HTTP {exc.status}\n{exc.body}"
            ) from exc
        print_model("created_transfer", created)
        transfer_id = created.id

        deadline = time.monotonic() + POLL_TIMEOUT_S
        while True:
            transfer = await client.get_transfer(transfer_id)
            if transfer.state.value != last_state:
                print(f"state: {transfer.state.value}")
                last_state = transfer.state.value

            if transfer.connection_info is not None and pull_task is None:
                pull_task = asyncio.create_task(
                    pull_stream(transfer.connection_info.uri)
                )

            if transfer.state.value in FINAL_STATES:
                print_model("final_transfer", transfer)
                break
            if time.monotonic() > deadline:
                print(f"poll timeout reached after {POLL_TIMEOUT_S:g}s")
                break
            await asyncio.sleep(POLL_INTERVAL_S)

        if pull_task is not None and not pull_task.done():
            print("stopping zmq pull task...")
            pull_task.cancel()
        if pull_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await pull_task
        return 0
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("interrupted, shutting down...")
        if pull_task is not None and not pull_task.done():
            pull_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pull_task
        if transfer_id is not None and last_state not in FINAL_STATES:
            print(f"canceling transfer {transfer_id}...")
            with contextlib.suppress(exceptions.ApiException):
                await client.cancel_transfer(transfer_id)
        return 130
    finally:
        await client.aclose()


def main() -> int:
    return asyncio.run(run(Settings()))


if __name__ == "__main__":
    raise SystemExit(main())

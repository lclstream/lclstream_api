#!/usr/bin/env python3
"""Start an lclstreamer->fastcache transfer at S3DF, then run CrystFEL here.

Run from a NERSC login node:

    uv run crystfel_nersc.py
"""

import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from lclstream_api_client import (
    CacheMode,
    ConsumerSocket,
    JobAttributes,
    JobSpec,
    LclstreamApiClient,
    exceptions,
    params,
)

FINAL_STATES = {"canceled", "completed", "failed"}
CONNECT_TIMEOUT_S = 300.0
POLL_INTERVAL_S = 5.0


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LCLSTREAM_CRYSTFEL_", env_file=".env")

    api_url: AnyHttpUrl = AnyHttpUrl(
        "https://lcls-data-portal.slac.stanford.edu/lclstream-dev"
    )
    token_file: Path = Path(".secrets/s3df_token")

    # Valerio's CrystFEL example targets this run; jf16mgeom.geom matches it.
    source_identifier: str = "exp=mfx100852324,run=355"
    # Charged to the group we belong to, not the data's experiment.
    job_spec_account: str = "lcls:mfx100852324@milano"

    # CrystFEL side, all local to this node.
    geometry_file: Path = Path("jf16mgeom.geom")
    output_file: Path = Path("crystfel.stream")
    crystfel_image: str = (
        "docker://gitlab.desy.de:5555/thomas.white/crystfel/crystfel:latest"
    )
    crystfel_workers: int = 2
    # Skip indexing; just prove the stream arrives.
    indexing: str = "none"

    @property
    def token(self) -> str:
        try:
            token = self.token_file.read_text().strip()
        except FileNotFoundError:
            raise SystemExit(
                f"Token file not found: {self.token_file}\n"
                "Run ./scripts/dev-token.py to mint one."
            ) from None
        if token.lower().startswith("bearer "):
            token = token.split(None, 1)[1].strip()
        if not token:
            raise SystemExit(f"Token file is empty: {self.token_file}")
        return token


def crystfel_parameters(source_identifier: str) -> params.Parameters:
    """Mirrors lclstreamer's examples/lclstreamer-psana2-mfx-crystfel.yaml."""
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
            ),
            "photon_wavelength": params.Psana2DetectorInterfaceParameters(
                type="Psana2DetectorInterface",
                psana_name="SIOC:SYS0:ML00:AO192",
            ),
            "detector_distance": params.Psana2DetectorInterfaceParameters(
                type="Psana2DetectorInterface",
                psana_name="MFX:DET:MMS:04.RBV",
            ),
            "run_info": params.Psana2RunInfoParameters(type="Psana2RunInfo"),
        },
        processing_pipeline=params.CrystfelPreprocessingPipelineParameters(
            type="CrystfelPreprocessingPipeline"
        ),
        data_serializer=params.MsgpackBinarySerializerParameters(
            type="MsgpackBinarySerializer"
        ),
        data_handlers=[
            params.BinaryDataStreamingDataHandlerParameters(
                type="BinaryDataStreamingDataHandler",
                # lclstream_api overwrites this with the fastcache inurl.
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


def wait_for_connection(client: LclstreamApiClient, transfer_id: UUID) -> str:
    """Poll until the cache reports its consumer socket, or the transfer dies."""
    deadline = time.monotonic() + CONNECT_TIMEOUT_S
    last_state: str | None = None
    while True:
        transfer = client.get_transfer(transfer_id)
        if transfer.state.value != last_state:
            last_state = transfer.state.value
            print(f"state: {last_state}")

        if transfer.connection_info is not None:
            info = transfer.connection_info
            print(f"cache ready: {info.uri} (socket={info.socket.value})")
            if info.socket != ConsumerSocket.REQ:
                raise SystemExit(
                    f"cache is serving {info.socket.value}, but CrystFEL dials req"
                )
            return info.uri

        if transfer.state.value in FINAL_STATES:
            print_model("transfer", transfer)
            raise SystemExit(f"transfer reached {transfer.state.value} with no cache")
        if time.monotonic() > deadline:
            raise SystemExit(f"no cache after {CONNECT_TIMEOUT_S:g}s")
        time.sleep(POLL_INTERVAL_S)


def run_crystfel(cfg: Settings, uri: str) -> int:
    """Run indexamajig in the CrystFEL container against the cache."""
    geom = cfg.geometry_file.resolve()
    if not geom.is_file():
        raise SystemExit(f"Geometry file not found: {geom}")
    workdir = cfg.output_file.resolve().parent

    indexamajig = [
        "indexamajig",
        f"--zmq-input={uri}",
        "--zmq-request=next",
        "-g",
        f"/geom/{geom.name}",
        "-j",
        str(cfg.crystfel_workers),
        "--peaks=msgpack",
        "--copy-header=timestamp",
        "--copy-header=event_id",
        "--copy-header=source",
        "--data-format=msgpack",
        f"--indexing={cfg.indexing}",
        "-o",
        f"/out/{cfg.output_file.name}",
    ]
    cmd = [
        "podman-hpc",
        "run",
        "--rm",
        # The cache lives at S3DF; the container needs the host's network.
        "--network=host",
        "-v",
        f"{geom.parent}:/geom:ro",
        "-v",
        f"{workdir}:/out",
        cfg.crystfel_image.removeprefix("docker://"),
        *indexamajig,
    ]
    print(f"[crystfel] {' '.join(cmd)}")
    return subprocess.run(cmd).returncode


def main() -> int:
    cfg = Settings()
    if shutil.which("podman-hpc") is None:
        raise SystemExit("podman-hpc not on PATH; run this on a NERSC node")

    client = LclstreamApiClient(base_url=str(cfg.api_url), token=lambda: cfg.token)
    transfer_id: UUID | None = None
    try:
        created = client.create_transfer(
            crystfel_parameters(cfg.source_identifier),
            cache_mode=CacheMode.PER_TRANSFER,
            consumer_socket=ConsumerSocket.REQ,
            job_spec_override=JobSpec(
                attributes=JobAttributes(account=cfg.job_spec_account),
                environment={"LCLSTREAMER_DEBUG": "1"},
            ),
        )
    except exceptions.ApiException as exc:
        raise SystemExit(
            f"create_transfer failed: HTTP {exc.status}\n{exc.body}"
        ) from exc

    print_model("created_transfer", created)
    transfer_id = created.id
    try:
        uri = wait_for_connection(client, transfer_id)
        return run_crystfel(cfg, uri)
    except KeyboardInterrupt:
        print("interrupted")
        return 130
    finally:
        print(f"canceling transfer {transfer_id}...")
        try:
            client.cancel_transfer(transfer_id)
        except exceptions.ApiException as exc:
            print(f"cancel failed (HTTP {exc.status}); check the dashboard")


if __name__ == "__main__":
    raise SystemExit(main())

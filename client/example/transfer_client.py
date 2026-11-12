#!/usr/bin/env python3
"""Open and close an lclstreamer->fastcache transfer at S3DF.

Runs inside the client image built by this directory's Dockerfile. The job
body calls it twice: once to get a ZMQ URI, once to hand the transfer back.

    transfer_client.py create          # prints TRANSFER_ID and URI
    transfer_client.py cancel <id>
"""

import argparse
import os
import re
import time
from pathlib import Path

import yaml
from pydantic import AnyHttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from lclstream_api_client import (
    CacheMode,
    ConsumerSocket,
    JobAttributes,
    JobSpec,
    LclstreamApiClient,
    TransferState,
    exceptions,
    params,
)

FINAL_STATES = {"canceled", "completed", "failed"}
CONNECT_TIMEOUT_S = 300.0
POLL_INTERVAL_S = 5.0

# Matches run_lclsstream_nersc_template.py's placeholder style, so one
# escaping convention covers every templated file in this directory.
PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_CRYSTFEL_", env_file=".env", extra="ignore"
    )

    api_url: AnyHttpUrl = AnyHttpUrl(
        "https://lclstream-dev.slac.stanford.edu"
    )
    # The job body mounts the staged token here.
    token_file: Path = Path("/run/s3df_token")

    source_identifier: str = "exp=mfx100852324,run=355"
    # Charged to a project we belong to, not the data's experiment.
    job_spec_account: str = "LCLS:mfx101592326"

    # lclstreamer config, in the same YAML shape as lclstreamer's own
    # examples/*.yaml. Mounted at /cfg alongside the geometry file.
    # {{ source_identifier }} is filled in at runtime, since the per-job
    # exp/run is not known until submission.
    parameters_file: Path = Path("/cfg/lclstreamer-crystfel.yaml")
    ejfat_uri: SecretStr | None = None

    @property
    def token(self) -> str:
        try:
            token = self.token_file.read_text().strip()
        except FileNotFoundError:
            raise SystemExit(
                f"Token file not found: {self.token_file}\n"
                "submit_crystfel_nersc.py stages and mounts it."
            ) from None
        if token.lower().startswith("bearer "):
            token = token.split(None, 1)[1].strip()
        if not token:
            raise SystemExit(f"Token file is empty: {self.token_file}")
        return token


def load_parameters(parameters_file: Path, source_identifier: str) -> params.Parameters:
    """Load lclstreamer parameters from a YAML file, in the format used by
    lclstreamer's own examples/*.yaml.

    The ``urls`` entry of any ``BinaryDataStreamingDataHandler`` is
    overwritten by lclstream_api with the fastcache inurl, so its value in
    the file is a placeholder.
    """
    try:
        text = parameters_file.read_text()
    except FileNotFoundError:
        raise SystemExit(
            f"Parameters file not found: {parameters_file}\n"
            "Mount it at /cfg alongside the geometry file."
        ) from None

    def substitute(match: re.Match) -> str:
        name = match.group(1)
        if name == "source_identifier":
            return source_identifier
        raise SystemExit(f"Unknown placeholder {{{{ {name} }}}} in {parameters_file}")

    rendered = PLACEHOLDER_RE.sub(substitute, text)
    raw = yaml.safe_load(rendered)
    if not isinstance(raw, dict):
        raise SystemExit(f"{parameters_file} must contain a mapping of parameters")
    return params.Parameters.model_validate(raw)


def open_client(cfg: Settings) -> LclstreamApiClient:
    return LclstreamApiClient(base_url=str(cfg.api_url), token=lambda: cfg.token)


def wait_for_uri(client: LclstreamApiClient, transfer_id, ejfat: bool = False) -> str | None:
    """Poll until the cache reports its consumer socket, or the transfer dies."""
    deadline = time.monotonic() + CONNECT_TIMEOUT_S
    last_state = None
    while True:
        try:
            transfer = client.get_transfer(transfer_id)
        except exceptions.ApiException as exc:
            # A blip must not cost the whole queue wait.
            print(f"poll failed (HTTP {exc.status}); retrying")
            time.sleep(POLL_INTERVAL_S)
            continue
        if transfer.state.value != last_state:
            last_state = transfer.state.value
            print(f"state: {last_state}")
        if ejfat and transfer.state == TransferState.READY:
            return None
        if not ejfat and transfer.connection_info is not None:
            info = transfer.connection_info
            if info.socket != ConsumerSocket.REQ:
                raise SystemExit(
                    f"cache is serving {info.socket.value}, but CrystFEL dials req"
                )
            return info.uri
        if transfer.state.value in FINAL_STATES:
            raise SystemExit(f"transfer reached {transfer.state.value} with no cache")
        if time.monotonic() > deadline:
            raise SystemExit(f"no cache after {CONNECT_TIMEOUT_S:g}s")
        time.sleep(POLL_INTERVAL_S)


def create(cfg: Settings) -> int:
    """Start the transfer and print what the job body needs, as shell vars."""
    client = open_client(cfg)
    ejfat_uri = os.getenv("EJFAT_URI") or cfg.ejfat_uri
    cons_socket = ConsumerSocket.REQ
    if ejfat_uri:
        cons_socket = ConsumerSocket.EJFAT
    try:
        created = client.create_transfer(
            load_parameters(cfg.parameters_file, cfg.source_identifier),
            cache_mode=CacheMode.PER_TRANSFER,
            consumer_socket=cons_socket,
            ejfat_uri=ejfat_uri,
            job_spec_override=JobSpec(
                attributes=JobAttributes(account=cfg.job_spec_account),
                environment={"LCLSTREAMER_DEBUG": "1"},
            ),
        )
    except exceptions.ApiException as exc:
        raise SystemExit(f"create_transfer failed: HTTP {exc.status}\n{exc.body}")
    print(f"EJFAT_URI={ejfat_uri}")
    print(f"transfer: {created.id}")
    uri = wait_for_uri(client, created.id, ejfat=cfg.ejfat_uri is not None)
    # Parsed by crystfel_nersc.sh.
    print(f"TRANSFER_ID={created.id}")
    print(f"URI={uri}")
    return 0


def cancel(cfg: Settings, transfer_id: str) -> int:
    from uuid import UUID

    try:
        open_client(cfg).cancel_transfer(UUID(transfer_id))
    except exceptions.ApiException as exc:
        print(f"cancel failed (HTTP {exc.status}); check the dashboard")
        return 0
    print(f"canceled {transfer_id}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("create")
    stop = sub.add_parser("cancel")
    stop.add_argument("transfer_id")
    args = parser.parse_args()

    cfg = Settings()
    if args.command == "create":
        return create(cfg)
    return cancel(cfg, args.transfer_id)


if __name__ == "__main__":
    raise SystemExit(main())

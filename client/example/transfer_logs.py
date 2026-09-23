#!/usr/bin/env python3
"""Why did a transfer fail? Pulls every log the SLAC side will give you.

    uv run transfer_logs.py <transfer_id>
    uv run transfer_logs.py <transfer_id> --watch   # before compensation deletes it

lclstream_api removes the work dir when a transfer fails, so the cache log
exists for only a few seconds; --watch races that.
"""

import argparse
import time
from pathlib import Path

import httpx
from amscrot.facility import FacilityClient
from amscrot.facility.models import Resource
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

STREAMS = ("cache", "producer_stdout", "producer_stderr")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_LOGS_", env_file=".env", extra="ignore"
    )

    api_url: AnyHttpUrl = AnyHttpUrl(
        "https://lcls-data-portal.slac.stanford.edu/lclstream-dev"
    )
    token_file: Path = Path("~/.s3df-access-token").expanduser()

    # S3DF IRI, for files the API will not serve.
    iri_url: AnyHttpUrl = AnyHttpUrl("https://iri.slac.stanford.edu")
    fs_resource: str = "sdfdata"
    # fastcache_api's own log; unset skips that section.
    supervisor_log: Path | None = None

    @property
    def token(self) -> str:
        try:
            return self.token_file.read_text().strip()
        except FileNotFoundError:
            raise SystemExit(f"No token at {self.token_file}; run ./scripts/s3df-login") from None


def api(cfg: Settings) -> httpx.Client:
    return httpx.Client(
        base_url=str(cfg.api_url).rstrip("/"),
        headers={"Authorization": f"Bearer {cfg.token}"},
        timeout=20,
    )


def show_state(client: httpx.Client, tid: str) -> None:
    r = client.get(f"/transfers/{tid}")
    if r.status_code != 200:
        raise SystemExit(f"transfer {tid}: HTTP {r.status_code} {r.text[:200]}")
    d = r.json()
    print(f"state: {d['state']}   cache_mode={d.get('cache_mode')}")
    if d.get("connection_info"):
        print(f"consumer: {d['connection_info']['uri']} ({d['connection_info']['socket']})")
    for t in d.get("transitions", []):
        print(f"  {t['created_at'][11:19]} {t['state']:<13} {t['source']:<13} {t.get('info') or ''}")


def show_streams(client: httpx.Client, tid: str, lines: int) -> None:
    index = client.get(f"/transfers/{tid}/logs")
    for s in index.json().get("streams", []) if index.status_code == 200 else []:
        print(f"\n=== {s['stream']}  {s['path']}  available={s['available']}")
        if not s["available"]:
            continue
        r = client.get(f"/transfers/{tid}/logs/{s['stream']}", params={"mode": "tail", "lines": lines})
        print(r.text if r.status_code == 200 else f"(HTTP {r.status_code})")


def watch_cache(client: httpx.Client, tid: str, seconds: float, lines: int) -> None:
    """Keep the last copy seen; the work dir is deleted on failure."""
    url = f"/transfers/{tid}/logs/cache"
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        r = client.get(url, params={"mode": "tail", "lines": lines})
        if r.status_code == 200 and r.text.strip() and r.text != last:
            last = r.text
            print(f"\n=== cache log at t+{seconds - (deadline - time.monotonic()):.1f}s")
            print(last)
        time.sleep(0.4)
    if last is None:
        print("\n=== cache log never appeared")


def show_supervisor(cfg: Settings, lines: int) -> None:
    if cfg.supervisor_log is None:
        print("\n(set LCLSTREAM_LOGS_SUPERVISOR_LOG for the fastcache_api log)")
        return
    fs = Resource(
        data={"id": cfg.fs_resource},
        facility_client=FacilityClient(endpoint=str(cfg.iri_url), token=cfg.token, name="s3df"),
    ).fs
    print(f"\n=== fastcache_api {cfg.supervisor_log}")
    # S3DF returns the content directly, unlike NERSC's stringified dict.
    out = str(fs.tail(str(cfg.supervisor_log), lines=lines).result)
    print("\n".join(l for l in out.splitlines() if "GET /api/v1/caches/" not in l))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("transfer_id")
    parser.add_argument("--watch", type=float, metavar="SECONDS", nargs="?", const=45.0)
    parser.add_argument("--lines", type=int, default=40)
    args = parser.parse_args()

    cfg = Settings()
    with api(cfg) as client:
        show_state(client, args.transfer_id)
        if args.watch:
            watch_cache(client, args.transfer_id, args.watch, args.lines)
        else:
            show_streams(client, args.transfer_id, args.lines)
    show_supervisor(cfg, args.lines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

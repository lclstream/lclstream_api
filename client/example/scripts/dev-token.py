#!/usr/bin/env python3
"""Mints an S3DF/Dex bearer token (device-code flow via `s3df login`) and
stages it at .secrets/s3df_token.
"""

import os
import subprocess
import tempfile
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_EXAMPLE_", frozen=True, validate_default=True
    )

    TOKEN_HOST: str = "sdfssh001"
    REMOTE_TOKEN: Path = Path(".s3df-access-token")
    TOKEN_FILE: Path = Path(".secrets/s3df_token")


cfg = Settings()


def main() -> int:
    cfg.TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)

    print(f"Logging in on {cfg.TOKEN_HOST} -- follow the browser prompt it prints")
    try:
        subprocess.run(
            ["ssh", "-t", cfg.TOKEN_HOST, "/sdf/sw/s3df-cli/bin/s3df login"], check=True
        )

        print(f"Copying the token back from {cfg.TOKEN_HOST}")
        fd, tmp_name = tempfile.mkstemp(prefix="s3df_token.", dir=cfg.TOKEN_FILE.parent)
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            subprocess.run(
                ["scp", f"{cfg.TOKEN_HOST}:{cfg.REMOTE_TOKEN}", str(tmp_path)],
                check=True,
            )
            tmp_path.chmod(0o400)
            tmp_path.replace(cfg.TOKEN_FILE)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
    except subprocess.CalledProcessError as exc:
        print(f"{exc.cmd[0]} failed with exit code {exc.returncode}")
        return exc.returncode

    print(f"Token staged at {cfg.TOKEN_FILE}")
    print(f"Set LCLSTREAM_EXAMPLE_TOKEN_FILE={cfg.TOKEN_FILE} in .env to use it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

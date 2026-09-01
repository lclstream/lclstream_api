from pathlib import Path
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BeforeValidator, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_comma_list(v: Any) -> Any:
    if isinstance(v, str) and not v.startswith("["):
        return [item.strip() for item in v.split(",") if item.strip()]
    return v


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_DB_", frozen=True, validate_default=True
    )

    url: PostgresDsn = PostgresDsn(
        url="postgresql+psycopg://postgres:postgres@localhost:5432/lclstream_api"
    )


class DbosSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_DBOS_", frozen=True, validate_default=True
    )

    name: str = "lclstream-api-dbos"
    system_schema: str = "dbos"


class FastcacheClientSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_FASTCACHE_", frozen=True, validate_default=True
    )

    base_url: AnyHttpUrl = AnyHttpUrl("https://sdfdtn003.sdf.slac.stanford.edu:8000")
    # mTLS: CA bundle that signs the fastcache server cert, plus our client cert.
    verify: bool | str = True
    client_cert: Path = Field(default=Path("/tmp/client.crt"))
    client_key: Path = Field(default=Path("/tmp/client.key"))
    timeout_s: float = 30.0

    def token(self) -> str:
        raise NotImplementedError("Bearer tokens are deprecated for FastCache; use mTLS.")


class LCLStreamerProducerSettings(BaseSettings):
    """Static, deployment-level knobs for the producer (lclstreamer) IRI job."""

    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_PRODUCER_", frozen=True, validate_default=True
    )

    # Root of the per-experiment data tree on S3DF. The per-transfer job
    # directory is built underneath this (see ``core.producer.producer_job_path``).
    data_base_dir: str = "/sdf/data/lcls/ds"
    # Environment variables keyed by psana env name ("psana1" / "psana2").
    # Complex value: override via a JSON-encoded ``LCLSTREAM_PRODUCER_ENVIRONMENTS``.
    environments: dict[str, dict[str, str]] = Field(default_factory=dict)


class CredentialsSettings(BaseSettings):
    """Encrypted delegated-token storage and admission policy."""

    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_CREDENTIALS_", frozen=True, validate_default=True
    )

    fernet_key: SecretStr = Field(
        default=SecretStr("SLBZwIDk2LnEnjkomcOWXuywapbSC9Wa6bHFWqo8sEI="),
        description="Base64 Fernet key encrypting stored user bearer tokens",
    )
    lifecycle_grace_seconds: int = Field(
        default=900,
        ge=0,
        description="Token lifetime reserved beyond the requested producer limit",
    )


CommaSeparatedList = Annotated[list[str], BeforeValidator(_parse_comma_list)]


class OidcSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_OIDC_", frozen=True, validate_default=True
    )

    issuer_url: str = "https://dex.example/dex"
    jwks_uri: str = "https://dex.example/dex/keys"
    audiences: CommaSeparatedList = Field(default_factory=list)
    # Verified emails allowed to use the service (the access allowlist).
    expected_users: CommaSeparatedList = Field(default_factory=list)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_APP_", frozen=True, validate_default=True
    )
    root_path: str = "/api/v2"


database = DatabaseSettings()
dbos = DbosSettings()
fastcache = FastcacheClientSettings()  # type: ignore
producer = LCLStreamerProducerSettings()
oidc = OidcSettings()
credentials = CredentialsSettings()  # type: ignore
app = AppSettings()

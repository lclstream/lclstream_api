from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BeforeValidator, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _parse_comma_list(v: Any) -> Any:
    if isinstance(v, str) and not v.startswith("["):
        return [item.strip() for item in v.split(",") if item.strip()]
    return v


def _ensure_psycopg_driver(v: Any) -> Any:
    """Coerce a driverless ``postgresql://`` DSN to ``postgresql+psycopg``."""
    if isinstance(v, str) and v.startswith("postgresql://"):
        return "postgresql+psycopg://" + v.removeprefix("postgresql://")
    return v


PsycopgDsn = Annotated[PostgresDsn, BeforeValidator(_ensure_psycopg_driver)]


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_DB_", frozen=True, validate_default=True
    )

    url: PsycopgDsn = PostgresDsn(
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
    # mTLS: server CA, plus our client cert.
    verify: bool | str = True
    client_cert: Path
    client_key: Path
    timeout_s: float = 30.0
    # Hand consumers an address, not a DTN name they cannot resolve.
    # ZMQ retries a bad name forever instead of failing.
    resolve_consumer_host: bool = True


class IriClientSettings(BaseSettings):
    """Connection and placement settings for IRI submission at S3DF."""

    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_IRI_", frozen=True, validate_default=True
    )

    # Dev is https://iri-dev.slac.stanford.edu.
    base_url: AnyHttpUrl = AnyHttpUrl("https://iri.slac.stanford.edu")
    facility: str = "s3df"
    resource: str = "milano"
    fs_resource: str = "sdfdata"


class LCLStreamerProducerSettings(BaseSettings):
    """Static, deployment-level knobs for the producer (lclstreamer) IRI job."""

    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_PRODUCER_", frozen=True, validate_default=True
    )

    # S3DF data tree; shared cache only.
    data_base_dir: str = "/sdf/data/lcls/ds"
    # S3DF home root: {home_base_dir}/{username[0]}/{username}.
    home_base_dir: str = "/sdf/home"
    # Keyed by psana env name: psana1/psana2.
    environments: dict[str, dict[str, str]] = Field(default_factory=dict)
    # Override with a pre-pulled .sif.
    container_image: str = "docker://ghcr.io/lclstream/lclstreamer-psana2extmpi:latest"


# NoDecode: env values are plain, not JSON.
CommaSeparatedList = Annotated[list[str], NoDecode, BeforeValidator(_parse_comma_list)]


class OidcSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_OIDC_", frozen=True, validate_default=True
    )

    issuer_url: str = "https://dex.example/dex"
    jwks_uri: str = "https://dex.example/dex/keys"
    audiences: CommaSeparatedList = Field(default_factory=list)
    # Verified emails allowed to use the service.
    expected_users: CommaSeparatedList = Field(default_factory=list)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_APP_", frozen=True, validate_default=True
    )
    root_path: str = "/api/v2"
    cors_origins: CommaSeparatedList = Field(default_factory=list)


class CredentialsSettings(BaseSettings):
    """Encrypted delegated-token storage and admission policy."""

    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_CREDENTIALS_", frozen=True, validate_default=True
    )

    fernet_key: SecretStr = Field(
        description="Base64 Fernet key encrypting stored user bearer tokens"
    )
    lifecycle_grace_seconds: int = Field(
        default=900,
        ge=0,
        description="Token lifetime reserved beyond the requested producer limit",
    )


@lru_cache
def get_database() -> DatabaseSettings:
    return DatabaseSettings()


@lru_cache
def get_dbos() -> DbosSettings:
    return DbosSettings()


@lru_cache
def get_fastcache() -> FastcacheClientSettings:
    return FastcacheClientSettings()


@lru_cache
def get_iri() -> IriClientSettings:
    return IriClientSettings()


@lru_cache
def get_producer() -> LCLStreamerProducerSettings:
    return LCLStreamerProducerSettings()


@lru_cache
def get_oidc() -> OidcSettings:
    return OidcSettings()


@lru_cache
def get_app() -> AppSettings:
    return AppSettings()


@lru_cache
def get_credentials() -> CredentialsSettings:
    return CredentialsSettings()

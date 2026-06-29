from pathlib import Path
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BeforeValidator, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _parse_comma_list(v: Any) -> Any:
    if isinstance(v, str) and not v.startswith("["):
        return [item.strip() for item in v.split(",") if item.strip()]
    return v


def _ensure_psycopg_driver(v: Any) -> Any:
    """Coerce driverless ``postgresql://`` DSN to the ``postgresql+psycopg``
    driver the async engine expects."""
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
    # Path to the shared 12h SLAC bearer token (re-read on each request so the
    # mint can rotate it without a restart). Same file IRI uses.
    token_file: Path = Field(
        description="Path to a file containing the shared SLAC bearer token",
    )
    # mTLS: CA bundle that signs the fastcache server cert, plus our client cert.
    verify: bool | str = True
    client_cert: Path
    client_key: Path
    timeout_s: float = 30.0

    @property
    def token(self) -> str:
        """Read the bearer token from file (fresh each call)."""
        return self.token_file.read_text().strip()


class IriClientSettings(BaseSettings):
    """Connection + placement settings for IRI submission at S3DF.

    Reads ``LCLSTREAM_IRI_*`` environment variables.
    Requires ``s3df_token_file`` pointing to a file containing the S3DF Dex bearer token.
    """

    model_config = SettingsConfigDict(
        env_prefix="LCLSTREAM_IRI_", frozen=True, validate_default=True
    )

    # Prod IRI API; override to https://iri-dev.slac.stanford.edu for dev.
    base_url: AnyHttpUrl = AnyHttpUrl("https://iri.slac.stanford.edu")
    s3df_token_file: Path = Field(
        description="Path to a file containing the S3DF Dex bearer token",
    )
    facility: str = "s3df"
    # IRI resource id (URL path key), not the display name
    resource: str = "s3df-slurm"
    # Filesystem resource hosting the producer config upload. A distinct IRI
    # role from `resource` (compute), though they share an id at S3DF today.
    # TODO: ask whether the compute and filesystem resources can ever differ.
    fs_resource: str = "s3df-slurm"

    @property
    def s3df_token(self) -> SecretStr:
        """Read the S3DF bearer token from file."""
        return SecretStr(self.s3df_token_file.read_text().strip())


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
    # Override this with a pre-pulled .sif file
    container_image: str = "docker://ghcr.io/lclstream/lclstreamer-psana2extmpi:latest"


# NoDecode stops pydantic-settings from JSON-decoding the env value in the
# source (which a bare string like "s3df" fails)
CommaSeparatedList = Annotated[list[str], NoDecode, BeforeValidator(_parse_comma_list)]


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
fastcache = FastcacheClientSettings()
iri = IriClientSettings()
producer = LCLStreamerProducerSettings()
oidc = OidcSettings()
app = AppSettings()

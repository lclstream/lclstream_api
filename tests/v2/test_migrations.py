from collections.abc import Generator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine
from testcontainers.postgres import PostgresContainer

import lclstream_api.v2.tables as _tables  # noqa: F401 — registers all tables in Base.metadata

POSTGRES_IMAGE = "postgres:18"
POSTGRES_DRIVER = "psycopg"
REPO_ROOT = Path(__file__).parent.parent.parent


def _alembic_config(engine: Engine) -> Config:
    cfg = Config(toml_file=str(REPO_ROOT / "pyproject.toml"))
    cfg.set_main_option(
        "sqlalchemy.url", engine.url.render_as_string(hide_password=False)
    )
    return cfg


@pytest.fixture(scope="module")
def postgres_engine() -> Generator[Engine]:
    """Needs Docker: runs every migration against a real PostgreSQL container."""
    with PostgresContainer(POSTGRES_IMAGE) as pg:
        engine = create_engine(pg.get_connection_url(driver=POSTGRES_DRIVER))
        command.upgrade(_alembic_config(engine), "head")
        yield engine
        engine.dispose()


class TestMigrations:
    def test_upgrade_succeeds(self, postgres_engine: Engine) -> None:
        """alembic upgrade head must complete without error."""

    def test_no_model_drift(self, postgres_engine: Engine) -> None:
        """No model change is missing from the migration history."""
        cfg = _alembic_config(postgres_engine)
        # command.check raises when it detects drift.
        command.check(cfg)

    def test_downgrade_and_upgrade(self, postgres_engine: Engine) -> None:
        cfg = _alembic_config(postgres_engine)
        command.downgrade(cfg, "base")
        command.upgrade(cfg, "head")

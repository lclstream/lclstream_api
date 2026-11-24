"""Generate an Alembic migration from SQLAlchemy ORM metadata."""

import sys
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from testcontainers.postgres import PostgresContainer

import lclstream_api.v2.tables as _tables  # noqa: F401 — registers tables on Base.metadata

POSTGRES_IMAGE = "postgres:18"
POSTGRES_DRIVER = "psycopg"
REPO_ROOT = Path(__file__).parent.parent
VERSIONS_DIR = REPO_ROOT / "src/lclstream_api/v2/alembic/versions"


def main() -> None:
    message = (
        " ".join(sys.argv[1:])
        if len(sys.argv) > 1
        else input("Migration name: ").strip()
    )
    if not message:
        print("Aborted: migration name cannot be empty.")
        sys.exit(1)

    before = {f for f in VERSIONS_DIR.glob("*.py") if f.name != "__init__.py"}

    print(f"Starting {POSTGRES_IMAGE}...")
    with PostgresContainer(POSTGRES_IMAGE) as pg:
        engine = create_engine(pg.get_connection_url(driver=POSTGRES_DRIVER))
        cfg = Config(toml_file=str(REPO_ROOT / "pyproject.toml"))
        cfg.set_main_option(
            "sqlalchemy.url", engine.url.render_as_string(hide_password=False)
        )
        command.upgrade(cfg, "head")
        command.revision(cfg, autogenerate=True, message=message)
        engine.dispose()

    new_files = {
        f for f in VERSIONS_DIR.glob("*.py") if f.name != "__init__.py"
    } - before
    for f in sorted(new_files):
        print(f"Generated: {f.name}")


if __name__ == "__main__":
    main()

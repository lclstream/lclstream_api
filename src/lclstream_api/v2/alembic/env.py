import logging

from alembic import context
from sqlalchemy import create_engine, pool

import lclstream_api.v2.tables as _tables  # noqa: F401 - registers tables
from lclstream_api.v2.config import get_database
from lclstream_api.v2.tables import Base

config = context.config
logging.basicConfig(level=logging.INFO)
target_metadata = Base.metadata


def get_url() -> str:
    # set programmatically by scripts/gen_migration.py for testcontainers
    url = config.get_alembic_option("sqlalchemy.url")
    if url:
        return url
    # for running migrations against real db
    return str(get_database().url)


def run_migrations() -> None:
    connectable = create_engine(get_url(), poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


run_migrations()

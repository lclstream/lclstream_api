"""add cache_log_path to transfers

Revision ID: b7e2c4f10a93
Revises: 5cd4b2a36fbe
Create Date: 2026-09-22 10:05:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7e2c4f10a93"
down_revision: str | list[str] | None = "5cd4b2a36fbe"
branch_labels: str | list[str] | None = None
depends_on: str | list[str] | None = None


def upgrade():
    # Nullable with no backfill on purpose: the old path was under the
    # requester's home, where fastcache_api could never write, so those
    # rows have no cache log to point at.
    op.add_column("transfers", sa.Column("cache_log_path", sa.String(), nullable=True))


def downgrade():
    op.drop_column("transfers", "cache_log_path")

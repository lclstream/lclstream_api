"""add owner_username to transfers

Revision ID: ffc90f0b98a8
Revises: 5ad7d226665a
Create Date: 2026-08-28 15:15:49.110103

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ffc90f0b98a8"
down_revision: str | list[str] | None = "5ad7d226665a"
branch_labels: str | list[str] | None = None
depends_on: str | list[str] | None = None


def upgrade():
    # Older rows derived the username from the email local part, so backfill
    # with it: that is the path those transfers wrote to, so their logs stay
    # readable. New rows carry the token claim instead.
    op.add_column("transfers", sa.Column("owner_username", sa.String(), nullable=True))
    op.execute("UPDATE transfers SET owner_username = split_part(owner_email, '@', 1)")
    op.alter_column("transfers", "owner_username", nullable=False)


def downgrade():
    op.drop_column("transfers", "owner_username")

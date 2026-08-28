"""add consumer_socket to transfers

Revision ID: 5cd4b2a36fbe
Revises: ffc90f0b98a8
Create Date: 2026-08-28 15:16:35.453479

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5cd4b2a36fbe"
down_revision: str | list[str] | None = "ffc90f0b98a8"
branch_labels: str | list[str] | None = None
depends_on: str | list[str] | None = None


def upgrade():
    # server_default backfills existing rows; pull was the only behaviour.
    op.add_column(
        "transfers",
        sa.Column(
            "consumer_socket", sa.String(), server_default="pull", nullable=False
        ),
    )


def downgrade():
    op.drop_column("transfers", "consumer_socket")

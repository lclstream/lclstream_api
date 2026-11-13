"""add cache registry

Revision ID: 5ad7d226665a
Revises: d4a14dbb7b21
Create Date: 2026-07-17 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5ad7d226665a"
down_revision: str | list[str] | None = "d4a14dbb7b21"
branch_labels: str | list[str] | None = None
depends_on: str | list[str] | None = None


def upgrade():
    op.create_table(
        "caches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("experiment", sa.String(), nullable=False),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_caches_experiment"),
        "caches",
        ["experiment"],
        unique=False,
    )


def downgrade():
    op.drop_index(op.f("ix_caches_experiment"), table_name="caches")
    op.drop_table("caches")

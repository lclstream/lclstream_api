"""add OIDC ownership and delegated user credentials

Revision ID: d4a14dbb7b21
Revises: f3a1c9b8e2d4
Create Date: 2026-07-17 11:53:23.643548

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4a14dbb7b21"
down_revision: str | list[str] | None = "f3a1c9b8e2d4"
branch_labels: str | list[str] | None = None
depends_on: str | list[str] | None = None


def upgrade():
    # Email-only rows cannot be mapped safely to an OIDC (issuer, subject).
    # Finished rows have no live external resources or DBOS workflows, so
    # they're safe to purge automatically. Only in-flight rows require an
    # operator to settle/cancel them first.
    bind = op.get_bind()
    bind.execute(
        sa.text(
            "DELETE FROM transfers WHERE state IN ('canceled', 'completed', 'failed')"
        )
    )
    has_active_transfers = bind.execute(
        sa.text("SELECT EXISTS (SELECT 1 FROM transfers)")
    ).scalar()
    if has_active_transfers:
        raise RuntimeError(
            "cannot migrate OIDC ownership while active transfers exist; "
            "settle or cancel all in-flight V2 transfers first"
        )
    op.alter_column(
        "transfers",
        "user",
        new_column_name="owner_email",
        existing_type=sa.String(),
        existing_nullable=False,
    )
    op.add_column("transfers", sa.Column("owner_issuer", sa.String(), nullable=False))
    op.add_column("transfers", sa.Column("owner_subject", sa.String(), nullable=False))

    op.create_table(
        "user_credentials",
        sa.Column("issuer", sa.String(), nullable=False),
        sa.Column("subject", sa.String(), nullable=False),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("encrypted_token", sa.LargeBinary(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("issuer", "subject"),
    )


def downgrade():
    has_active_transfers = (
        op.get_bind()
        .execute(sa.text("SELECT EXISTS (SELECT 1 FROM transfers)"))
        .scalar()
    )
    if has_active_transfers:
        raise RuntimeError(
            "cannot downgrade OIDC ownership while active transfers exist; "
            "settle or cancel all in-flight V2 transfers first"
        )
    op.drop_table("user_credentials")
    op.drop_column("transfers", "owner_subject")
    op.drop_column("transfers", "owner_issuer")
    op.alter_column(
        "transfers",
        "owner_email",
        new_column_name="user",
        existing_type=sa.String(),
        existing_nullable=False,
    )

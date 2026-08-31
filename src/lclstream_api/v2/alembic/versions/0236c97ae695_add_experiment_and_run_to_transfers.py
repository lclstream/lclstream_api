"""add experiment and run to transfers

Revision ID: 0236c97ae695
Revises: aa298396d6a0
Create Date: 2026-07-10 16:42:02.711541

"""

import sqlalchemy as sa
from alembic import op

from lclstream_api.v2.core.producer import parse_exp_run

# revision identifiers, used by Alembic.
revision: str = "0236c97ae695"
down_revision: str | list[str] | None = "aa298396d6a0"
branch_labels: str | list[str] | None = None
depends_on: str | list[str] | None = None


def upgrade():
    # Nullable first so the backfill below can populate existing rows;
    # tightened to NOT NULL once every row has a value.
    op.add_column("transfers", sa.Column("experiment", sa.String(), nullable=True))
    op.add_column("transfers", sa.Column("run", sa.String(), nullable=True))

    transfers = sa.table(
        "transfers",
        sa.column("id", sa.Uuid()),
        sa.column("parameters", sa.JSON()),
        sa.column("experiment", sa.String()),
        sa.column("run", sa.String()),
    )
    connection = op.get_bind()
    rows = connection.execute(
        sa.select(transfers.c.id, transfers.c.parameters)
    ).fetchall()
    for row_id, parameters in rows:
        source_identifier = (parameters or {}).get("source_identifier", "")
        exp, run = parse_exp_run(source_identifier)
        if exp is None or run is None:
            raise RuntimeError(
                f"transfer {row_id} has an unresolvable source_identifier "
                f"{source_identifier!r}; cannot backfill experiment/run"
            )
        connection.execute(
            transfers.update()
            .where(transfers.c.id == row_id)
            .values(experiment=exp, run=run)
        )

    op.alter_column("transfers", "experiment", nullable=False)
    op.alter_column("transfers", "run", nullable=False)


def downgrade():
    op.drop_column("transfers", "run")
    op.drop_column("transfers", "experiment")

"""home url run contract and run-conference-year mapping

Revision ID: 20260301_000002
Revises: 20260301_000001
Create Date: 2026-03-01 21:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260301_000002"
down_revision = "20260301_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("scrape_runs", sa.Column("home_url", sa.Text(), nullable=True, server_default=""))

    op.alter_column("scrape_runs", "conference_id", existing_type=sa.Integer(), nullable=True)
    op.alter_column("scrape_runs", "start_year", existing_type=sa.Integer(), nullable=True)
    op.alter_column("scrape_runs", "end_year", existing_type=sa.Integer(), nullable=True)

    op.execute("UPDATE scrape_runs SET home_url = '' WHERE home_url IS NULL")
    op.alter_column("scrape_runs", "home_url", existing_type=sa.Text(), nullable=False)
    op.alter_column("scrape_runs", "home_url", server_default=None)

    op.create_table(
        "run_conference_years",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "run_id",
            postgresql.UUID(as_uuid=False),
            sa.ForeignKey("scrape_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "conference_year_id",
            sa.Integer(),
            sa.ForeignKey("conference_years.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.UniqueConstraint("run_id", "conference_year_id", name="uq_run_conference_year"),
    )


def downgrade() -> None:
    op.drop_table("run_conference_years")

    op.alter_column("scrape_runs", "home_url", existing_type=sa.Text(), nullable=True)
    op.drop_column("scrape_runs", "home_url")

    op.alter_column("scrape_runs", "conference_id", existing_type=sa.Integer(), nullable=False)
    op.alter_column("scrape_runs", "start_year", existing_type=sa.Integer(), nullable=False)
    op.alter_column("scrape_runs", "end_year", existing_type=sa.Integer(), nullable=False)

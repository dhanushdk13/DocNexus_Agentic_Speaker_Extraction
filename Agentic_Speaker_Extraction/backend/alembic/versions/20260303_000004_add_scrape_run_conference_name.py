"""add conference_name to scrape_runs

Revision ID: 20260303_000004
Revises: 20260302_000003
Create Date: 2026-03-03 11:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260303_000004"
down_revision = "20260302_000003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("scrape_runs", sa.Column("conference_name", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("scrape_runs", "conference_name")


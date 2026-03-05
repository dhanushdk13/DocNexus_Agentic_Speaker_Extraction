"""add conference identity fields

Revision ID: 20260302_000003
Revises: 20260301_000002
Create Date: 2026-03-02 18:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260302_000003"
down_revision = "20260301_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("conferences", sa.Column("organizer_name", sa.String(length=255), nullable=True))
    op.add_column("conferences", sa.Column("event_series_name", sa.String(length=255), nullable=True))
    op.add_column("conferences", sa.Column("name_confidence", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("conferences", "name_confidence")
    op.drop_column("conferences", "event_series_name")
    op.drop_column("conferences", "organizer_name")

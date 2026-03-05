"""add physician profile enrichment fields

Revision ID: 20260303_000005
Revises: 20260303_000004
Create Date: 2026-03-03 12:15:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260303_000005"
down_revision = "20260303_000004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("physicians", sa.Column("primary_specialty", sa.String(length=255), nullable=True))
    op.add_column("physicians", sa.Column("primary_education", sa.String(length=255), nullable=True))
    op.add_column("physicians", sa.Column("primary_profile_url", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("physicians", "primary_profile_url")
    op.drop_column("physicians", "primary_education")
    op.drop_column("physicians", "primary_specialty")


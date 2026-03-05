"""add physician photo/bio enrichment fields

Revision ID: 20260303_000006
Revises: 20260303_000005
Create Date: 2026-03-03 17:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260303_000006"
down_revision = "20260303_000005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("physicians", sa.Column("photo_url", sa.Text(), nullable=True))
    op.add_column("physicians", sa.Column("photo_source_url", sa.Text(), nullable=True))
    op.add_column("physicians", sa.Column("bio_short", sa.Text(), nullable=True))
    op.add_column("physicians", sa.Column("bio_source_url", sa.Text(), nullable=True))
    op.add_column("physicians", sa.Column("enrichment_confidence", sa.Float(), nullable=True))
    op.add_column("physicians", sa.Column("enrichment_updated_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("physicians", "enrichment_updated_at")
    op.drop_column("physicians", "enrichment_confidence")
    op.drop_column("physicians", "bio_source_url")
    op.drop_column("physicians", "bio_short")
    op.drop_column("physicians", "photo_source_url")
    op.drop_column("physicians", "photo_url")

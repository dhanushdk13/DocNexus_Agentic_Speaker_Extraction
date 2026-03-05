"""scrub physician photo fields

Revision ID: 20260303_000008
Revises: 20260303_000007
Create Date: 2026-03-03 23:55:00
"""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "20260303_000008"
down_revision = "20260303_000007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE physicians SET photo_url = NULL, photo_source_url = NULL")


def downgrade() -> None:
    # Data scrub migration; no automatic restoration possible.
    pass

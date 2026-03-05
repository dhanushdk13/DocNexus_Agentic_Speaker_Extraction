"""add navigation template memory table

Revision ID: 20260303_000007
Revises: 20260303_000006
Create Date: 2026-03-03 18:25:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260303_000007"
down_revision = "20260303_000006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "navigation_template_memory",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("domain", sa.String(length=255), nullable=False),
        sa.Column("template_key", sa.String(length=500), nullable=False),
        sa.Column("intent", sa.String(length=64), nullable=True),
        sa.Column("visits", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("speaker_hits", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("appearance_hits", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("zero_yield_streak", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("domain", "template_key", name="uq_navigation_template_memory_domain_template"),
    )


def downgrade() -> None:
    op.drop_table("navigation_template_memory")

"""initial schema

Revision ID: 20260301_000001
Revises:
Create Date: 2026-03-01 00:00:01
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260301_000001"
down_revision = None
branch_labels = None
depends_on = None


conference_year_status = postgresql.ENUM(
    "pending", "running", "complete", "blocked", "partial", "error", name="conference_year_status", create_type=False
)
source_category = postgresql.ENUM(
    "official_speakers",
    "official_program",
    "pdf_program",
    "platform",
    "recap",
    "unknown",
    name="source_category",
    create_type=False,
)
source_method = postgresql.ENUM(
    "http_static", "playwright_dom", "playwright_network", "pdf_text", name="source_method", create_type=False
)
fetch_status = postgresql.ENUM("new", "fetched", "blocked", "error", "skipped", name="fetch_status", create_type=False)
extraction_artifact_type = postgresql.ENUM(
    "clean_text",
    "candidate_blocks",
    "llm_output",
    "pdf_text",
    "network_json_sample",
    name="extraction_artifact_type",
    create_type=False,
)
run_status = postgresql.ENUM("pending", "running", "complete", "partial", "error", name="run_status", create_type=False)


def upgrade() -> None:
    bind = op.get_bind()
    conference_year_status.create(bind, checkfirst=True)
    source_category.create(bind, checkfirst=True)
    source_method.create(bind, checkfirst=True)
    fetch_status.create(bind, checkfirst=True)
    extraction_artifact_type.create(bind, checkfirst=True)
    run_status.create(bind, checkfirst=True)

    op.create_table(
        "conferences",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("canonical_name", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "conference_years",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("conference_id", sa.Integer(), sa.ForeignKey("conferences.id", ondelete="CASCADE"), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("status", conference_year_status, nullable=False, server_default="pending"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("conference_id", "year", name="uq_conference_year"),
    )

    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("conference_year_id", sa.Integer(), sa.ForeignKey("conference_years.id", ondelete="CASCADE"), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("category", source_category, nullable=False, server_default="unknown"),
        sa.Column("method", source_method, nullable=False, server_default="http_static"),
        sa.Column("fetch_status", fetch_status, nullable=False, server_default="new"),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("content_type", sa.String(length=32), nullable=True),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "physicians",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("full_name", sa.String(length=255), nullable=False),
        sa.Column("name_key", sa.String(length=255), nullable=False, unique=True),
        sa.Column("primary_designation", sa.String(length=255), nullable=True),
        sa.Column("primary_affiliation", sa.String(length=255), nullable=True),
        sa.Column("primary_location", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "physician_aliases",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("physician_id", sa.Integer(), sa.ForeignKey("physicians.id", ondelete="CASCADE"), nullable=False),
        sa.Column("alias", sa.String(length=255), nullable=False),
        sa.Column("alias_key", sa.String(length=255), nullable=False, unique=True),
    )

    op.create_table(
        "extractions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id", ondelete="CASCADE"), nullable=False),
        sa.Column("artifact_type", extraction_artifact_type, nullable=False),
        sa.Column("data", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "appearances",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("physician_id", sa.Integer(), sa.ForeignKey("physicians.id", ondelete="CASCADE"), nullable=False),
        sa.Column("conference_year_id", sa.Integer(), sa.ForeignKey("conference_years.id", ondelete="CASCADE"), nullable=False),
        sa.Column("role", sa.String(length=255), nullable=True),
        sa.Column("session_title", sa.String(length=500), nullable=True),
        sa.Column("talk_brief_extracted", sa.Text(), nullable=True),
        sa.Column("talk_brief_generated", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.UniqueConstraint(
            "physician_id",
            "conference_year_id",
            "session_title",
            name="uq_physician_conference_session",
        ),
    )

    op.create_table(
        "scrape_runs",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("conference_id", sa.Integer(), sa.ForeignKey("conferences.id", ondelete="CASCADE"), nullable=False),
        sa.Column("start_year", sa.Integer(), nullable=False),
        sa.Column("end_year", sa.Integer(), nullable=False),
        sa.Column("status", run_status, nullable=False, server_default="pending"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "run_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", postgresql.UUID(as_uuid=False), sa.ForeignKey("scrape_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("conference_year_id", sa.Integer(), sa.ForeignKey("conference_years.id", ondelete="CASCADE"), nullable=True),
        sa.Column("stage", sa.String(length=64), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("data_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("run_events")
    op.drop_table("scrape_runs")
    op.drop_table("appearances")
    op.drop_table("extractions")
    op.drop_table("physician_aliases")
    op.drop_table("physicians")
    op.drop_table("sources")
    op.drop_table("conference_years")
    op.drop_table("conferences")

    bind = op.get_bind()
    run_status.drop(bind, checkfirst=True)
    extraction_artifact_type.drop(bind, checkfirst=True)
    fetch_status.drop(bind, checkfirst=True)
    source_method.drop(bind, checkfirst=True)
    source_category.drop(bind, checkfirst=True)
    conference_year_status.drop(bind, checkfirst=True)

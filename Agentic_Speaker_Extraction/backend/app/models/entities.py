from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import DateTime, Enum as SAEnum, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base
from app.models.enums import (
    ConferenceYearStatus,
    ExtractionArtifactType,
    FetchStatus,
    RunStatus,
    SourceCategory,
    SourceMethod,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Conference(Base):
    __tablename__ = "conferences"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    canonical_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    organizer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_series_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    name_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    years: Mapped[list[ConferenceYear]] = relationship(back_populates="conference", cascade="all, delete-orphan")


class ConferenceYear(Base):
    __tablename__ = "conference_years"
    __table_args__ = (UniqueConstraint("conference_id", "year", name="uq_conference_year"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    conference_id: Mapped[int] = mapped_column(ForeignKey("conferences.id", ondelete="CASCADE"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ConferenceYearStatus] = mapped_column(
        SAEnum(ConferenceYearStatus, name="conference_year_status"),
        default=ConferenceYearStatus.pending,
        nullable=False,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    conference: Mapped[Conference] = relationship(back_populates="years")
    sources: Mapped[list[Source]] = relationship(back_populates="conference_year", cascade="all, delete-orphan")
    appearances: Mapped[list[Appearance]] = relationship(back_populates="conference_year", cascade="all, delete-orphan")
    run_links: Mapped[list[RunConferenceYear]] = relationship(
        back_populates="conference_year",
        cascade="all, delete-orphan",
    )


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    conference_year_id: Mapped[int] = mapped_column(ForeignKey("conference_years.id", ondelete="CASCADE"), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[SourceCategory] = mapped_column(
        SAEnum(SourceCategory, name="source_category"),
        default=SourceCategory.unknown,
        nullable=False,
    )
    method: Mapped[SourceMethod] = mapped_column(
        SAEnum(SourceMethod, name="source_method"),
        default=SourceMethod.http_static,
        nullable=False,
    )
    fetch_status: Mapped[FetchStatus] = mapped_column(
        SAEnum(FetchStatus, name="fetch_status"),
        default=FetchStatus.new,
        nullable=False,
    )
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    conference_year: Mapped[ConferenceYear] = relationship(back_populates="sources")
    extractions: Mapped[list[Extraction]] = relationship(back_populates="source", cascade="all, delete-orphan")


class Physician(Base):
    __tablename__ = "physicians"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    name_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    primary_designation: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_affiliation: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_specialty: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_education: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_profile_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    photo_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    photo_source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    bio_short: Mapped[str | None] = mapped_column(Text, nullable=True)
    bio_source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    enrichment_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    enrichment_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    aliases: Mapped[list[PhysicianAlias]] = relationship(back_populates="physician", cascade="all, delete-orphan")
    appearances: Mapped[list[Appearance]] = relationship(back_populates="physician", cascade="all, delete-orphan")


class PhysicianAlias(Base):
    __tablename__ = "physician_aliases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    physician_id: Mapped[int] = mapped_column(ForeignKey("physicians.id", ondelete="CASCADE"), nullable=False)
    alias: Mapped[str] = mapped_column(String(255), nullable=False)
    alias_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    physician: Mapped[Physician] = relationship(back_populates="aliases")


class Extraction(Base):
    __tablename__ = "extractions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"), nullable=False)
    artifact_type: Mapped[ExtractionArtifactType] = mapped_column(
        SAEnum(ExtractionArtifactType, name="extraction_artifact_type"),
        nullable=False,
    )
    data: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    source: Mapped[Source] = relationship(back_populates="extractions")


class Appearance(Base):
    __tablename__ = "appearances"
    __table_args__ = (
        UniqueConstraint(
            "physician_id",
            "conference_year_id",
            "session_title",
            name="uq_physician_conference_session",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    physician_id: Mapped[int] = mapped_column(ForeignKey("physicians.id", ondelete="CASCADE"), nullable=False)
    conference_year_id: Mapped[int] = mapped_column(ForeignKey("conference_years.id", ondelete="CASCADE"), nullable=False)
    role: Mapped[str | None] = mapped_column(String(255), nullable=True)
    session_title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    talk_brief_extracted: Mapped[str | None] = mapped_column(Text, nullable=True)
    talk_brief_generated: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    physician: Mapped[Physician] = relationship(back_populates="appearances")
    conference_year: Mapped[ConferenceYear] = relationship(back_populates="appearances")


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4()))
    home_url: Mapped[str] = mapped_column(Text, nullable=False)
    conference_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    conference_id: Mapped[int | None] = mapped_column(ForeignKey("conferences.id", ondelete="CASCADE"), nullable=True)
    start_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[RunStatus] = mapped_column(SAEnum(RunStatus, name="run_status"), default=RunStatus.pending, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    discovered_year_links: Mapped[list[RunConferenceYear]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
    )


class RunConferenceYear(Base):
    __tablename__ = "run_conference_years"
    __table_args__ = (
        UniqueConstraint("run_id", "conference_year_id", name="uq_run_conference_year"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("scrape_runs.id", ondelete="CASCADE"), nullable=False)
    conference_year_id: Mapped[int] = mapped_column(ForeignKey("conference_years.id", ondelete="CASCADE"), nullable=False)

    run: Mapped[ScrapeRun] = relationship(back_populates="discovered_year_links")
    conference_year: Mapped[ConferenceYear] = relationship(back_populates="run_links")


class RunEvent(Base):
    __tablename__ = "run_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("scrape_runs.id", ondelete="CASCADE"), nullable=False)
    conference_year_id: Mapped[int | None] = mapped_column(ForeignKey("conference_years.id", ondelete="CASCADE"), nullable=True)
    stage: Mapped[str] = mapped_column(String(64), nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    message: Mapped[str] = mapped_column(Text, nullable=False)
    data_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class NavigationTemplateMemory(Base):
    __tablename__ = "navigation_template_memory"
    __table_args__ = (
        UniqueConstraint("domain", "template_key", name="uq_navigation_template_memory_domain_template"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain: Mapped[str] = mapped_column(String(255), nullable=False)
    template_key: Mapped[str] = mapped_column(String(500), nullable=False)
    intent: Mapped[str | None] = mapped_column(String(64), nullable=True)
    visits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    speaker_hits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    appearance_hits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    zero_yield_streak: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

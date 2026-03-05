from datetime import datetime

from pydantic import BaseModel


class AppearanceOut(BaseModel):
    id: int
    conference_year_id: int
    conference_id: int
    conference_name: str
    year: int
    role: str | None
    session_title: str | None
    talk_brief_extracted: str | None
    talk_brief_generated: str | None
    confidence: float | None
    source_url: str | None


class PhysicianCardOut(BaseModel):
    id: int
    full_name: str
    primary_designation: str | None
    primary_affiliation: str | None
    primary_location: str | None
    primary_specialty: str | None = None
    primary_education: str | None = None
    primary_profile_url: str | None = None
    specialty: str | None = None
    profile_url: str | None = None
    photo_url: str | None = None
    bio_short: str | None = None
    created_at: datetime
    appearances: list[AppearanceOut]


class PhysicianDetailOut(PhysicianCardOut):
    aliases: list[str]
    highlight_conference_id: int | None = None
    highlight_year: int | None = None


class PhysicianCardLiteOut(BaseModel):
    id: int
    full_name: str
    primary_designation: str | None
    primary_specialty: str | None = None
    primary_profile_url: str | None = None
    specialty: str | None = None
    profile_url: str | None = None
    photo_url: str | None = None
    bio_short: str | None = None
    conference_count: int
    appearance_count: int

from datetime import datetime

from pydantic import BaseModel

from app.models.enums import ConferenceYearStatus


class ConferenceYearOut(BaseModel):
    id: int
    year: int
    status: ConferenceYearStatus
    notes: str | None
    created_at: datetime


class ConferenceOut(BaseModel):
    id: int
    name: str
    canonical_name: str | None
    organizer_name: str | None = None
    event_series_name: str | None = None
    name_confidence: float | None = None
    created_at: datetime
    years: list[ConferenceYearOut]


class ConferencePhysicianCardOut(BaseModel):
    physician_id: int
    full_name: str
    primary_designation: str | None
    appearance_count: int
    session_count: int


class ConferenceYearPhysicianGroupOut(BaseModel):
    year: int
    status: ConferenceYearStatus
    notes: str | None
    physicians: list[ConferencePhysicianCardOut]


class ConferenceDetailOut(BaseModel):
    id: int
    name: str
    canonical_name: str | None
    organizer_name: str | None = None
    event_series_name: str | None = None
    name_confidence: float | None = None
    created_at: datetime
    years: list[ConferenceYearOut]
    total_physicians: int
    total_appearances: int

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Appearance, Conference, ConferenceYear, Physician
from app.schemas.conferences import (
    ConferenceDetailOut,
    ConferenceOut,
    ConferencePhysicianCardOut,
    ConferenceYearOut,
    ConferenceYearPhysicianGroupOut,
)

router = APIRouter(tags=["conferences"])


@router.get("/conferences", response_model=list[ConferenceOut])
def list_conferences(db: Session = Depends(get_db)) -> list[ConferenceOut]:
    conferences = db.execute(select(Conference).order_by(Conference.created_at.desc())).scalars().all()

    output: list[ConferenceOut] = []
    for conference in conferences:
        years = db.execute(
            select(ConferenceYear)
            .where(ConferenceYear.conference_id == conference.id)
            .order_by(ConferenceYear.year.desc())
        ).scalars().all()

        output.append(
            ConferenceOut(
                id=conference.id,
                name=conference.name,
                canonical_name=conference.canonical_name,
                organizer_name=conference.organizer_name,
                event_series_name=conference.event_series_name,
                name_confidence=conference.name_confidence,
                created_at=conference.created_at,
                years=[
                    ConferenceYearOut(
                        id=y.id,
                        year=y.year,
                        status=y.status,
                        notes=y.notes,
                        created_at=y.created_at,
                    )
                    for y in years
                ],
            )
        )

    return output


@router.get("/conferences/{conference_id}", response_model=ConferenceDetailOut)
def get_conference(conference_id: int, db: Session = Depends(get_db)) -> ConferenceDetailOut:
    conference = db.execute(select(Conference).where(Conference.id == conference_id)).scalar_one_or_none()
    if not conference:
        raise HTTPException(status_code=404, detail="Conference not found")

    years = db.execute(
        select(ConferenceYear)
        .where(ConferenceYear.conference_id == conference.id)
        .order_by(ConferenceYear.year.desc())
    ).scalars().all()

    total_physicians = (
        db.execute(
            select(func.count(func.distinct(Appearance.physician_id)))
            .join(ConferenceYear, ConferenceYear.id == Appearance.conference_year_id)
            .where(ConferenceYear.conference_id == conference.id)
        ).scalar_one()
        or 0
    )
    total_appearances = (
        db.execute(
            select(func.count(Appearance.id))
            .join(ConferenceYear, ConferenceYear.id == Appearance.conference_year_id)
            .where(ConferenceYear.conference_id == conference.id)
        ).scalar_one()
        or 0
    )

    return ConferenceDetailOut(
        id=conference.id,
        name=conference.name,
        canonical_name=conference.canonical_name,
        organizer_name=conference.organizer_name,
        event_series_name=conference.event_series_name,
        name_confidence=conference.name_confidence,
        created_at=conference.created_at,
        years=[
            ConferenceYearOut(
                id=y.id,
                year=y.year,
                status=y.status,
                notes=y.notes,
                created_at=y.created_at,
            )
            for y in years
        ],
        total_physicians=int(total_physicians),
        total_appearances=int(total_appearances),
    )


@router.get("/conferences/{conference_id}/physicians", response_model=list[ConferenceYearPhysicianGroupOut])
def list_conference_physicians(
    conference_id: int,
    year: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[ConferenceYearPhysicianGroupOut]:
    conference = db.execute(select(Conference).where(Conference.id == conference_id)).scalar_one_or_none()
    if not conference:
        raise HTTPException(status_code=404, detail="Conference not found")

    year_stmt = select(ConferenceYear).where(ConferenceYear.conference_id == conference.id)
    if year is not None:
        year_stmt = year_stmt.where(ConferenceYear.year == year)
    years = db.execute(year_stmt.order_by(ConferenceYear.year.desc())).scalars().all()

    groups: list[ConferenceYearPhysicianGroupOut] = []
    for conference_year in years:
        rows = db.execute(
            select(
                Physician.id,
                Physician.full_name,
                Physician.primary_designation,
                func.count(Appearance.id).label("appearance_count"),
                func.count(func.distinct(Appearance.session_title)).label("session_count"),
            )
            .join(Appearance, Appearance.physician_id == Physician.id)
            .where(Appearance.conference_year_id == conference_year.id)
            .group_by(Physician.id, Physician.full_name, Physician.primary_designation)
            .order_by(Physician.full_name.asc())
        ).all()

        cards = [
            ConferencePhysicianCardOut(
                physician_id=int(pid),
                full_name=name,
                primary_designation=designation,
                appearance_count=int(appearance_count or 0),
                session_count=int(session_count or 0),
            )
            for pid, name, designation, appearance_count, session_count in rows
        ]
        groups.append(
            ConferenceYearPhysicianGroupOut(
                year=conference_year.year,
                status=conference_year.status,
                notes=conference_year.notes,
                physicians=cards,
            )
        )

    return groups

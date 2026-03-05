from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Appearance, Conference, ConferenceYear, Physician, PhysicianAlias
from app.schemas.physicians import AppearanceOut, PhysicianCardLiteOut, PhysicianCardOut, PhysicianDetailOut

router = APIRouter(tags=["physicians"])


def _build_appearances(db: Session, physician_id: int) -> list[AppearanceOut]:
    rows = db.execute(
        select(Appearance, ConferenceYear, Conference)
        .join(ConferenceYear, ConferenceYear.id == Appearance.conference_year_id)
        .join(Conference, Conference.id == ConferenceYear.conference_id)
        .where(Appearance.physician_id == physician_id)
        .order_by(ConferenceYear.year.desc())
    ).all()

    out: list[AppearanceOut] = []
    for appearance, conference_year, conference in rows:
        out.append(
            AppearanceOut(
                id=appearance.id,
                conference_year_id=conference_year.id,
                conference_id=conference.id,
                conference_name=conference.name,
                year=conference_year.year,
                role=appearance.role,
                session_title=appearance.session_title,
                talk_brief_extracted=appearance.talk_brief_extracted,
                talk_brief_generated=appearance.talk_brief_generated,
                confidence=appearance.confidence,
                source_url=appearance.source_url,
            )
        )
    return out


@router.get("/physicians", response_model=list[PhysicianCardOut])
def list_physicians(
    query: str | None = Query(default=None),
    conference_id: int | None = Query(default=None),
    year: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[PhysicianCardOut]:
    stmt = select(Physician).order_by(Physician.full_name.asc())

    if query:
        like = f"%{query.lower()}%"
        stmt = stmt.where(func.lower(Physician.full_name).like(like))

    physicians = db.execute(stmt).scalars().all()

    cards: list[PhysicianCardOut] = []
    for physician in physicians:
        appearances = _build_appearances(db, physician.id)
        if conference_id is not None:
            filtered: list[AppearanceOut] = []
            for app in appearances:
                cy = db.execute(select(ConferenceYear).where(ConferenceYear.id == app.conference_year_id)).scalar_one_or_none()
                if cy and cy.conference_id == conference_id:
                    filtered.append(app)
            appearances = filtered

        if year is not None:
            appearances = [a for a in appearances if a.year == year]

        if conference_id is not None and not appearances:
            continue
        if year is not None and not appearances:
            continue

        cards.append(
            PhysicianCardOut(
                id=physician.id,
                full_name=physician.full_name,
                primary_designation=physician.primary_designation,
                primary_affiliation=physician.primary_affiliation,
                primary_location=physician.primary_location,
                primary_specialty=physician.primary_specialty,
                primary_education=physician.primary_education,
                primary_profile_url=None,
                specialty=physician.primary_specialty,
                profile_url=None,
                photo_url=None,
                bio_short=physician.bio_short,
                created_at=physician.created_at,
                appearances=appearances,
            )
        )

    return cards


@router.get("/physicians/cards", response_model=list[PhysicianCardLiteOut])
def list_physician_cards(
    query: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[PhysicianCardLiteOut]:
    stmt = select(Physician).order_by(Physician.full_name.asc())
    if query:
        like = f"%{query.lower()}%"
        stmt = stmt.where(func.lower(Physician.full_name).like(like))

    physicians = db.execute(stmt).scalars().all()
    output: list[PhysicianCardLiteOut] = []
    for physician in physicians:
        conference_count = (
            db.execute(
                select(func.count(func.distinct(ConferenceYear.conference_id)))
                .select_from(Appearance)
                .join(ConferenceYear, ConferenceYear.id == Appearance.conference_year_id)
                .where(Appearance.physician_id == physician.id)
            ).scalar_one()
            or 0
        )
        appearance_count = (
            db.execute(select(func.count(Appearance.id)).where(Appearance.physician_id == physician.id)).scalar_one() or 0
        )
        output.append(
            PhysicianCardLiteOut(
                id=physician.id,
                full_name=physician.full_name,
                primary_designation=physician.primary_designation,
                primary_specialty=physician.primary_specialty,
                primary_profile_url=None,
                specialty=physician.primary_specialty,
                profile_url=None,
                photo_url=None,
                bio_short=physician.bio_short,
                conference_count=int(conference_count),
                appearance_count=int(appearance_count),
            )
        )
    return output


@router.get("/physicians/{physician_id}", response_model=PhysicianDetailOut)
def get_physician(
    physician_id: int,
    fromConferenceId: int | None = Query(default=None),
    fromYear: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> PhysicianDetailOut:
    physician = db.execute(select(Physician).where(Physician.id == physician_id)).scalar_one_or_none()
    if not physician:
        raise HTTPException(status_code=404, detail="Physician not found")

    alias_rows = db.execute(select(PhysicianAlias).where(PhysicianAlias.physician_id == physician.id)).scalars().all()
    appearances = _build_appearances(db, physician.id)

    return PhysicianDetailOut(
        id=physician.id,
        full_name=physician.full_name,
        primary_designation=physician.primary_designation,
        primary_affiliation=physician.primary_affiliation,
        primary_location=physician.primary_location,
        primary_specialty=physician.primary_specialty,
        primary_education=physician.primary_education,
        primary_profile_url=None,
        specialty=physician.primary_specialty,
        profile_url=None,
        photo_url=None,
        bio_short=physician.bio_short,
        created_at=physician.created_at,
        appearances=appearances,
        aliases=[row.alias for row in alias_rows],
        highlight_conference_id=fromConferenceId,
        highlight_year=fromYear,
    )

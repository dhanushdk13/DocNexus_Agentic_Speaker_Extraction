from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Appearance, Conference, ConferenceYear, ConferenceYearStatus, Physician
from app.services.dedupe import get_or_create_physician, merge_close_physicians, name_key
from app.services.name_cleaner import canonicalize_person_name


def test_canonicalize_person_name_examples() -> None:
    examples = [
        ("FIDSA Asa Radix", "Asa Radix", "FIDSA"),
        ("MSPH Eric Farmer", "Eric Farmer", "MSPH"),
        ("PA-C Melanie Thompson", "Melanie Thompson", "PA-C"),
        ("HIV Treatment Melanie Thompson", "Melanie Thompson", None),
    ]
    for raw, expected_name, designation_hint in examples:
        result = canonicalize_person_name(full_name=raw, designation=None, role=None, evidence=None)
        assert result.is_valid is True
        assert result.full_name == expected_name
        if designation_hint:
            assert designation_hint in (result.designation or "")


def test_merge_close_physicians_merges_typo_last_name() -> None:
    db = SessionLocal()

    conference = Conference(name="ACTHIV", canonical_name="acthiv")
    db.add(conference)
    db.flush()
    conference_year = ConferenceYear(conference_id=conference.id, year=2026, status=ConferenceYearStatus.complete)
    db.add(conference_year)
    db.flush()

    first = Physician(
        full_name="Bruce Aldred",
        name_key=name_key("Bruce Aldred"),
        primary_designation="MD",
        primary_specialty="Neurology",
        primary_affiliation="Neuro Clinic",
        primary_location="Boston, MA",
        primary_profile_url="https://example.org/faculty/bruce-aldred",
    )
    second = Physician(
        full_name="Bruce Alred",
        name_key=name_key("Bruce Alred"),
        primary_designation="MD",
        primary_specialty="Neurology",
        primary_affiliation="Neuro Clinic",
        primary_location="Boston, MA",
        primary_profile_url="https://example.org/faculty/bruce-alred",
    )
    db.add_all([first, second])
    db.flush()

    db.add_all(
        [
            Appearance(physician_id=first.id, conference_year_id=conference_year.id, session_title="Session A", confidence=0.8),
            Appearance(physician_id=second.id, conference_year_id=conference_year.id, session_title="Session B", confidence=0.9),
        ]
    )
    db.commit()

    stats = merge_close_physicians(db, physician_ids={first.id, second.id})
    db.commit()

    physicians = db.execute(select(Physician).order_by(Physician.id.asc())).scalars().all()
    appearances = db.execute(select(Appearance)).scalars().all()
    db.close()

    assert stats.merged_physicians == 1
    assert len(physicians) == 1
    assert physicians[0].full_name in {"Bruce Aldred", "Bruce Alred"}
    assert len(appearances) == 2
    assert len({row.physician_id for row in appearances}) == 1


def test_same_name_conflicting_profiles_create_distinct_physicians() -> None:
    db = SessionLocal()

    first = get_or_create_physician(
        db=db,
        full_name="Rupa Patel",
        designation="MD",
        affiliation="HIV Prevention Institute",
        location="New York, NY",
        aliases=[],
    )
    second = get_or_create_physician(
        db=db,
        full_name="Rupa Patel",
        designation="MD",
        affiliation="Neurology and Stroke Center",
        location="Boston, MA",
        aliases=[],
    )
    db.commit()

    physicians = db.execute(select(Physician).where(Physician.full_name == "Rupa Patel")).scalars().all()
    db.close()

    assert first.id != second.id
    assert len(physicians) == 2
    assert all(item.name_key.startswith("rupa patel") for item in physicians)


def test_merge_close_physicians_skips_conflicting_profile_matches() -> None:
    db = SessionLocal()

    conference = Conference(name="Mixed Summit", canonical_name="mixed summit")
    db.add(conference)
    db.flush()
    conference_year = ConferenceYear(conference_id=conference.id, year=2026, status=ConferenceYearStatus.complete)
    db.add(conference_year)
    db.flush()

    first = Physician(
        full_name="Rupa Patel",
        name_key=name_key("Rupa Patel"),
        primary_designation="MD",
        primary_affiliation="HIV Prevention Institute",
        primary_location="New York, NY",
    )
    second = Physician(
        full_name="Rupa Patel",
        name_key=f"{name_key('Rupa Patel')}::neurology",
        primary_designation="MD",
        primary_affiliation="Neurology and Stroke Center",
        primary_location="Boston, MA",
    )
    db.add_all([first, second])
    db.flush()
    db.add_all(
        [
            Appearance(physician_id=first.id, conference_year_id=conference_year.id, session_title="HIV Session", confidence=0.8),
            Appearance(
                physician_id=second.id,
                conference_year_id=conference_year.id,
                session_title="Neuro Session",
                confidence=0.85,
            ),
        ]
    )
    db.commit()

    stats = merge_close_physicians(db, physician_ids={first.id, second.id})
    db.commit()

    physicians = db.execute(select(Physician).where(Physician.full_name == "Rupa Patel")).scalars().all()
    db.close()

    assert stats.merged_physicians == 0
    assert len(physicians) == 2

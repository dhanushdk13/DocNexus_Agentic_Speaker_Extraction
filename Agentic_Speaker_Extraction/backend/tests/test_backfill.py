from __future__ import annotations

import json
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.db import SessionLocal
from app.models import (
    Appearance,
    Conference,
    ConferenceYear,
    Extraction,
    ExtractionArtifactType,
    RunConferenceYear,
    RunStatus,
    ScrapeRun,
    Source,
    SourceCategory,
    SourceMethod,
)
from app.services import backfill
from app.services.extract_llm import AttributionTargetHint, ExtractedSpeaker, NormalizeResult


@pytest.mark.asyncio
async def test_backfill_creates_missing_appearance_from_candidate_artifacts(monkeypatch) -> None:
    db = SessionLocal()
    conference = Conference(name="Continuum", canonical_name="continuum")
    db.add(conference)
    db.flush()
    conference_year = ConferenceYear(conference_id=conference.id, year=2025)
    db.add(conference_year)
    db.flush()

    run = ScrapeRun(id=str(uuid4()), home_url="https://example.org/continuum-2025/", status=RunStatus.complete)
    db.add(run)
    db.flush()
    db.add(RunConferenceYear(run_id=run.id, conference_year_id=conference_year.id))
    db.flush()

    source = Source(
        conference_year_id=conference_year.id,
        url="https://example.org/continuum-2025/",
        category=SourceCategory.official_program,
        method=SourceMethod.http_static,
    )
    db.add(source)
    db.flush()
    db.add(
        Extraction(
            source_id=source.id,
            artifact_type=ExtractionArtifactType.candidate_blocks,
            data=json.dumps(
                [
                    {
                        "candidate_type": "session_speaker_pair",
                        "source_url": "https://example.org/continuum-2025/",
                        "session_title": "Person-Centered HIV Care: Implementing Patient Choice in ART",
                        "speaker_name_raw": "Rupa Patel",
                        "context_snippet": "Session agenda line",
                        "text": "Session: Person-Centered HIV Care: Implementing Patient Choice in ART. Speaker: Rupa Patel.",
                    }
                ]
            ),
        )
    )
    db.commit()
    db.close()

    async def fake_normalize(settings, candidates, conference_year_hints, batch_size: int = 40):  # noqa: ANN001
        return NormalizeResult(
            records=[
                ExtractedSpeaker(
                    full_name="Rupa Patel",
                    designation="MD",
                    affiliation=None,
                    location=None,
                    role="Speaker",
                    session_title="Person-Centered HIV Care: Implementing Patient Choice in ART",
                    talk_brief_extracted=None,
                    aliases=[],
                    is_physician_candidate=True,
                    confidence=0.88,
                    evidence_span="Session pair candidate",
                    attribution_targets=[
                        AttributionTargetHint(
                            conference_name="Continuum",
                            year=2025,
                            confidence=0.9,
                            reason="artifact",
                        )
                    ],
                )
            ]
        )

    monkeypatch.setattr(backfill, "normalize_candidates", fake_normalize)
    stats = await backfill.run_backfill(run_id=run.id, all_complete_runs=False)

    db = SessionLocal()
    appearances = db.execute(select(Appearance)).scalars().all()
    db.close()

    assert stats.appearances_created == 1
    assert len(appearances) == 1
    assert appearances[0].session_title == "Person-Centered HIV Care: Implementing Patient Choice in ART"

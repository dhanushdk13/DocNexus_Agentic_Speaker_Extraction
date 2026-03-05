from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass

from sqlalchemy import and_, select

from app.config import get_settings
from app.db import SessionLocal
from app.models import Appearance, Conference, ConferenceYear, Extraction, ExtractionArtifactType, RunConferenceYear, ScrapeRun, Source
from app.models.enums import RunStatus
from app.services.dedupe import get_or_create_physician, is_physician_like, merge_close_physicians
from app.services.extract_llm import normalize_candidates
from app.services.name_cleaner import canonicalize_person_name


@dataclass(slots=True)
class BackfillStats:
    runs_processed: int = 0
    sources_processed: int = 0
    appearances_created: int = 0
    duplicate_skips: int = 0
    merged_physicians: int = 0


async def _backfill_run(run_id: str, stats: BackfillStats) -> None:
    settings = get_settings()
    db = SessionLocal()
    try:
        run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
        if not run:
            return

        rows = db.execute(
            select(Source, ConferenceYear, Conference, Extraction)
            .join(ConferenceYear, ConferenceYear.id == Source.conference_year_id)
            .join(Conference, Conference.id == ConferenceYear.conference_id)
            .join(RunConferenceYear, RunConferenceYear.conference_year_id == ConferenceYear.id)
            .join(
                Extraction,
                and_(
                    Extraction.source_id == Source.id,
                    Extraction.artifact_type == ExtractionArtifactType.candidate_blocks,
                ),
            )
            .where(RunConferenceYear.run_id == run_id)
            .order_by(Source.id.asc(), Extraction.id.desc())
        ).all()

        seen_sources: set[int] = set()
        linked_physician_ids: set[int] = set()
        for source, conference_year, conference, extraction in rows:
            if source.id in seen_sources:
                continue
            seen_sources.add(source.id)
            stats.sources_processed += 1

            try:
                candidates = json.loads(extraction.data)
            except json.JSONDecodeError:
                continue
            if not isinstance(candidates, list) or not candidates:
                continue

            hints = [{"conference_name": conference.name, "year": conference_year.year}]
            normalized = await normalize_candidates(
                settings,
                candidates,
                conference_year_hints=hints,
                batch_size=max(1, int(settings.llm_normalize_batch_size)),
            )

            for record in normalized.records:
                canonical = canonicalize_person_name(
                    full_name=record.full_name,
                    designation=record.designation,
                    role=record.role,
                    evidence=record.evidence_span,
                )
                if not canonical.is_valid:
                    continue
                if not (
                    record.is_physician_candidate
                    or is_physician_like(
                        canonical.full_name,
                        canonical.designation,
                        record.affiliation,
                        record.role,
                        session_title=record.session_title,
                        evidence_span=record.evidence_span,
                    )
                ):
                    continue

                physician = get_or_create_physician(
                    db=db,
                    full_name=canonical.full_name,
                    designation=canonical.designation or record.designation,
                    affiliation=record.affiliation,
                    location=record.location,
                    aliases=list(record.aliases) + canonical.aliases,
                )
                linked_physician_ids.add(physician.id)

                existing_appearance = db.execute(
                    select(Appearance).where(
                        and_(
                            Appearance.physician_id == physician.id,
                            Appearance.conference_year_id == conference_year.id,
                            Appearance.session_title == record.session_title,
                        )
                    )
                ).scalar_one_or_none()
                if existing_appearance:
                    stats.duplicate_skips += 1
                    continue

                db.add(
                    Appearance(
                        physician_id=physician.id,
                        conference_year_id=conference_year.id,
                        role=record.role,
                        session_title=record.session_title,
                        talk_brief_extracted=record.talk_brief_extracted,
                        talk_brief_generated=None,
                        confidence=record.confidence,
                        source_url=source.url,
                    )
                )
                stats.appearances_created += 1

            db.commit()

        if linked_physician_ids:
            merge_stats = merge_close_physicians(db, physician_ids=linked_physician_ids)
            db.commit()
            stats.merged_physicians += merge_stats.merged_physicians

        stats.runs_processed += 1
    finally:
        db.close()


async def run_backfill(*, run_id: str | None, all_complete_runs: bool) -> BackfillStats:
    db = SessionLocal()
    try:
        if run_id:
            run_ids = [run_id]
        elif all_complete_runs:
            rows = db.execute(
                select(ScrapeRun.id).where(ScrapeRun.status.in_([RunStatus.complete, RunStatus.partial]))
            ).all()
            run_ids = [row[0] for row in rows]
        else:
            run_ids = []
    finally:
        db.close()

    stats = BackfillStats()
    for item in run_ids:
        await _backfill_run(item, stats)
    return stats


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing physician appearances from stored artifacts")
    parser.add_argument("--run-id", dest="run_id", default=None)
    parser.add_argument("--all-complete-runs", dest="all_complete_runs", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if not args.run_id and not args.all_complete_runs:
        raise SystemExit("Provide --run-id <id> or --all-complete-runs")

    stats = asyncio.run(run_backfill(run_id=args.run_id, all_complete_runs=bool(args.all_complete_runs)))
    print(
        json.dumps(
            {
                "runs_processed": stats.runs_processed,
                "sources_processed": stats.sources_processed,
                "appearances_created": stats.appearances_created,
                "duplicate_skips": stats.duplicate_skips,
                "merged_physicians": stats.merged_physicians,
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()

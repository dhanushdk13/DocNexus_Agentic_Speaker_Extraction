from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Conference, ConferenceYear, RunConferenceYear, RunEvent, ScrapeRun
from app.schemas.scrape_runs import (
    RunDashboardConferenceYearOut,
    RunDashboardResponse,
    RunDashboardSummaryOut,
    RunMetricsOut,
)


def _run_log_path(run_id: str) -> Path:
    return Path(__file__).resolve().parents[2] / "run_logs" / f"{run_id}.json"


def _load_years_from_run_log(run_id: str) -> list[RunDashboardConferenceYearOut] | None:
    path = _run_log_path(run_id)
    if not path.exists():
        return None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None

    raw_years = payload.get("years") if isinstance(payload, dict) else None
    if not isinstance(raw_years, list):
        return None

    years_by_id: dict[int, RunDashboardConferenceYearOut] = {}
    for row in raw_years:
        if not isinstance(row, dict):
            continue

        conference_year_id = row.get("conference_year_id")
        conference_name = row.get("conference_name")
        year = row.get("year")
        if not isinstance(conference_year_id, int) or not isinstance(conference_name, str) or not isinstance(year, int):
            continue

        years_by_id[conference_year_id] = RunDashboardConferenceYearOut(
            conference_year_id=conference_year_id,
            conference_name=conference_name.strip() or "Unknown",
            year=year,
            status=str(row.get("status") or "complete"),
            linked_appearances=int(row.get("linked") or 0),
            duplicate_links=int(row.get("duplicates") or 0),
            notes=row.get("notes") if isinstance(row.get("notes"), str) else None,
        )

    if not years_by_id:
        return None

    return sorted(
        years_by_id.values(),
        key=lambda item: (item.conference_name.lower(), -item.year, item.conference_year_id),
    )


def _linked_appearance_counts_from_events(db: Session, run_id: str) -> dict[int, int]:
    rows = db.execute(
        select(RunEvent.conference_year_id, func.count(RunEvent.id))
        .where(RunEvent.run_id == run_id)
        .where(RunEvent.conference_year_id.is_not(None))
        .where(RunEvent.stage.in_(["link_created", "link_completion_created"]))
        .group_by(RunEvent.conference_year_id)
    ).all()

    out: dict[int, int] = {}
    for conference_year_id, count in rows:
        if conference_year_id is None:
            continue
        out[int(conference_year_id)] = int(count or 0)
    return out


def _fallback_years_from_db(db: Session, run_id: str) -> list[RunDashboardConferenceYearOut]:
    linked_counts = _linked_appearance_counts_from_events(db, run_id)

    rows = db.execute(
        select(RunConferenceYear, ConferenceYear, Conference)
        .join(ConferenceYear, ConferenceYear.id == RunConferenceYear.conference_year_id)
        .join(Conference, Conference.id == ConferenceYear.conference_id)
        .where(RunConferenceYear.run_id == run_id)
    ).all()

    out: list[RunDashboardConferenceYearOut] = []
    for _, conference_year, conference in rows:
        status_value = conference_year.status.value if hasattr(conference_year.status, "value") else str(conference_year.status)
        out.append(
            RunDashboardConferenceYearOut(
                conference_year_id=int(conference_year.id),
                conference_name=conference.name,
                year=int(conference_year.year),
                status=status_value,
                linked_appearances=int(linked_counts.get(int(conference_year.id), 0)),
                duplicate_links=0,
                notes=conference_year.notes,
            )
        )

    return sorted(out, key=lambda item: (item.conference_name.lower(), -item.year, item.conference_year_id))


def _event_stage_counts(db: Session, run_id: str) -> dict[str, int]:
    rows = db.execute(
        select(RunEvent.stage, func.count(RunEvent.id))
        .where(RunEvent.run_id == run_id)
        .group_by(RunEvent.stage)
    ).all()
    return {str(stage): int(count or 0) for stage, count in rows}


def build_run_dashboard(
    db: Session,
    *,
    run: ScrapeRun,
    metrics: RunMetricsOut,
) -> RunDashboardResponse:
    stage_counts = _event_stage_counts(db, run.id)

    conference_years = _load_years_from_run_log(run.id)
    if conference_years is None:
        conference_years = _fallback_years_from_db(db, run.id)

    conferences_scraped = len({item.conference_name.strip().lower() for item in conference_years if item.conference_name.strip()})
    unique_years_scraped = len({int(item.year) for item in conference_years})

    attribution_unresolved = max(
        int(metrics.unresolved_attributions),
        int(stage_counts.get("attribution_unresolved", 0)),
    )

    summary = RunDashboardSummaryOut(
        conferences_scraped=conferences_scraped,
        conference_year_entries=len(conference_years),
        unique_years_scraped=unique_years_scraped,
        speakers_discovered=int(metrics.speaker_candidates_found),
        normalized_speakers=int(metrics.normalized_speakers),
        profiles_enrichment_started=int(stage_counts.get("physician_enrichment_start", 0)),
        profiles_enriched=int(stage_counts.get("physician_enriched", 0)),
        profiles_enrichment_skipped=int(stage_counts.get("physician_enrichment_skipped", 0)),
        physicians_linked=int(metrics.physicians_linked),
        appearances_linked=int(metrics.appearances_linked),
        attribution_unresolved=attribution_unresolved,
        llm_calls=int(metrics.llm_calls),
        llm_failures=int(metrics.llm_failures),
        llm_calls_saved=int(metrics.llm_calls_saved),
        nav_reask_attempts=int(metrics.nav_reask_attempts),
        nav_reask_successes=int(metrics.nav_reask_successes),
    )

    return RunDashboardResponse(
        run_id=run.id,
        conference_name=run.conference_name,
        summary=summary,
        conference_years=conference_years,
    )

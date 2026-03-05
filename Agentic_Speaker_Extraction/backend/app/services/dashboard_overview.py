from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Appearance, Conference, ConferenceYear, RunConferenceYear, RunEvent, RunStatus, ScrapeRun
from app.schemas.scrape_runs import (
    DashboardOverviewConferenceOut,
    DashboardOverviewResponse,
    DashboardOverviewTotalsOut,
    DashboardOverviewYearOut,
)


def _run_log_path(run_id: str) -> Path:
    return Path(__file__).resolve().parents[2] / "run_logs" / f"{run_id}.json"


def _safe_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _speaker_count_from_data_json(data_json: str | None) -> int:
    if not data_json:
        return 0
    try:
        payload = json.loads(data_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return 0
    if not isinstance(payload, dict):
        return 0
    return max(0, _safe_int(payload.get("speaker_count")))


def _read_run_log_stats(run_id: str) -> tuple[int, set[str], int] | None:
    path = _run_log_path(run_id)
    if not path.exists():
        return None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None

    if not isinstance(payload, dict):
        return None

    urls = payload.get("urls")
    if not isinstance(urls, list):
        return None

    pages_visited = 0
    good_pages = 0
    unique_links: set[str] = set()

    for row in urls:
        if not isinstance(row, dict):
            continue
        pages_visited += 1

        normalized_records = _safe_int(row.get("normalized_records"))
        if normalized_records > 0:
            good_pages += 1

        next_urls = row.get("nav_next_urls")
        if isinstance(next_urls, list):
            for raw_url in next_urls:
                if isinstance(raw_url, str) and raw_url.strip():
                    unique_links.add(raw_url.strip())

    return pages_visited, unique_links, good_pages


def _fallback_pages_visited(db: Session, run_id: str) -> int:
    count = db.execute(
        select(func.count(RunEvent.id)).where(RunEvent.run_id == run_id).where(RunEvent.stage == "fetch_route")
    ).scalar_one()
    return int(count or 0)


def _fallback_good_pages(db: Session, run_id: str) -> int:
    rows = db.execute(
        select(RunEvent.data_json).where(RunEvent.run_id == run_id).where(RunEvent.stage == "markdown_extract_end")
    ).all()
    good = 0
    for (data_json,) in rows:
        if _speaker_count_from_data_json(data_json) > 0:
            good += 1
    return good


def build_dashboard_overview(db: Session) -> DashboardOverviewResponse:
    terminal_runs = db.execute(
        select(ScrapeRun.id, ScrapeRun.conference_name)
        .where(ScrapeRun.status.in_([RunStatus.complete, RunStatus.partial, RunStatus.error]))
        .order_by(ScrapeRun.created_at.desc())
    ).all()

    run_ids = [str(row.id) for row in terminal_runs]
    conference_for_run = {
        str(row.id): (row.conference_name.strip() if isinstance(row.conference_name, str) and row.conference_name.strip() else "Unknown")
        for row in terminal_runs
    }

    if not run_ids:
        return DashboardOverviewResponse(
            generated_at=datetime.now(timezone.utc),
            totals=DashboardOverviewTotalsOut(),
            conferences=[],
        )

    run_year_rows = db.execute(
        select(RunConferenceYear.run_id, ConferenceYear.id, Conference.name, ConferenceYear.year)
        .join(ConferenceYear, ConferenceYear.id == RunConferenceYear.conference_year_id)
        .join(Conference, Conference.id == ConferenceYear.conference_id)
        .where(RunConferenceYear.run_id.in_(run_ids))
    ).all()

    included_year_ids: set[int] = set()
    year_meta: dict[int, tuple[str, int]] = {}
    conference_year_ids: dict[str, set[int]] = defaultdict(set)

    for run_id, conference_year_id, conference_name, year in run_year_rows:
        if conference_year_id is None or conference_name is None or year is None:
            continue
        conference_name_norm = conference_name.strip() or "Unknown"
        conference_year_id_int = int(conference_year_id)
        included_year_ids.add(conference_year_id_int)
        year_meta[conference_year_id_int] = (conference_name_norm, int(year))
        conference_year_ids[conference_name_norm].add(conference_year_id_int)

    year_appearance_counts: dict[int, int] = {}
    year_unique_speakers: dict[int, int] = {}
    conference_appearance_counts: dict[str, int] = defaultdict(int)
    conference_unique_speakers: dict[str, int] = defaultdict(int)
    total_appearance_count = 0
    total_unique_speakers = 0

    if included_year_ids:
        year_rows = db.execute(
            select(
                Appearance.conference_year_id,
                func.count(Appearance.id),
                func.count(func.distinct(Appearance.physician_id)),
            )
            .where(Appearance.conference_year_id.in_(included_year_ids))
            .group_by(Appearance.conference_year_id)
        ).all()
        for conference_year_id, appearance_count, unique_speakers in year_rows:
            if conference_year_id is None:
                continue
            year_appearance_counts[int(conference_year_id)] = int(appearance_count or 0)
            year_unique_speakers[int(conference_year_id)] = int(unique_speakers or 0)

        conference_rows = db.execute(
            select(
                Conference.name,
                func.count(Appearance.id),
                func.count(func.distinct(Appearance.physician_id)),
            )
            .join(ConferenceYear, ConferenceYear.id == Appearance.conference_year_id)
            .join(Conference, Conference.id == ConferenceYear.conference_id)
            .where(Appearance.conference_year_id.in_(included_year_ids))
            .group_by(Conference.name)
        ).all()
        for conference_name, appearance_count, unique_speakers in conference_rows:
            if conference_name is None:
                continue
            conference_name_norm = conference_name.strip() or "Unknown"
            conference_appearance_counts[conference_name_norm] = int(appearance_count or 0)
            conference_unique_speakers[conference_name_norm] = int(unique_speakers or 0)

        totals_row = db.execute(
            select(func.count(Appearance.id), func.count(func.distinct(Appearance.physician_id))).where(
                Appearance.conference_year_id.in_(included_year_ids)
            )
        ).one()
        total_appearance_count = int(totals_row[0] or 0)
        total_unique_speakers = int(totals_row[1] or 0)

    speakers_found_by_conference: dict[str, int] = defaultdict(int)
    speakers_found_by_run: dict[str, int] = defaultdict(int)
    speaker_rows = db.execute(
        select(ScrapeRun.id, ScrapeRun.conference_name, RunEvent.data_json)
        .join(ScrapeRun, ScrapeRun.id == RunEvent.run_id)
        .where(RunEvent.run_id.in_(run_ids))
        .where(RunEvent.stage == "markdown_extract_end")
    ).all()

    for run_id, run_conference_name, data_json in speaker_rows:
        conference_name = (
            run_conference_name.strip()
            if isinstance(run_conference_name, str) and run_conference_name.strip()
            else conference_for_run.get(str(run_id), "Unknown")
        )
        speaker_count = _speaker_count_from_data_json(data_json)
        speakers_found_by_conference[conference_name] += speaker_count
        speakers_found_by_run[str(run_id)] += speaker_count

    pages_visited_by_conference: dict[str, int] = defaultdict(int)
    good_pages_by_conference: dict[str, int] = defaultdict(int)
    links_by_conference: dict[str, set[str]] = defaultdict(set)
    global_unique_links: set[str] = set()

    for run_id in run_ids:
        if speakers_found_by_run.get(run_id, 0) <= 0:
            continue
        conference_name = conference_for_run.get(run_id, "Unknown")
        log_stats = _read_run_log_stats(run_id)

        if log_stats is not None:
            pages_visited, unique_links, good_pages = log_stats
        else:
            pages_visited = _fallback_pages_visited(db, run_id)
            unique_links = set()
            good_pages = _fallback_good_pages(db, run_id)

        pages_visited_by_conference[conference_name] += pages_visited
        good_pages_by_conference[conference_name] += good_pages
        links_by_conference[conference_name].update(unique_links)
        global_unique_links.update(unique_links)

    conference_names = set(speakers_found_by_conference.keys())
    conference_names.update(pages_visited_by_conference.keys())
    conference_names.update(links_by_conference.keys())
    conference_names.update(
        conference_name
        for conference_name, year_ids in conference_year_ids.items()
        if year_ids and speakers_found_by_conference.get(conference_name, 0) > 0
    )

    conferences: list[DashboardOverviewConferenceOut] = []
    for conference_name in sorted(conference_names, key=str.casefold):
        year_ids = sorted(
            conference_year_ids.get(conference_name, set()),
            key=lambda value: (year_meta.get(value, (conference_name, 0))[1], value),
        )

        years = [
            DashboardOverviewYearOut(
                conference_year_id=conference_year_id,
                conference_name=conference_name,
                year=year_meta.get(conference_year_id, (conference_name, 0))[1],
                unique_speakers_db=int(year_unique_speakers.get(conference_year_id, 0)),
                appearance_count_db=int(year_appearance_counts.get(conference_year_id, 0)),
            )
            for conference_year_id in year_ids
        ]

        conferences.append(
            DashboardOverviewConferenceOut(
                conference_name=conference_name,
                years_scraped=len(years),
                unique_speakers_db=int(conference_unique_speakers.get(conference_name, 0)),
                appearance_count_db=int(conference_appearance_counts.get(conference_name, 0)),
                speakers_found_extracted=int(speakers_found_by_conference.get(conference_name, 0)),
                pages_visited=int(pages_visited_by_conference.get(conference_name, 0)),
                links_discovered_unique=len(links_by_conference.get(conference_name, set())),
                good_pages_with_speakers=int(good_pages_by_conference.get(conference_name, 0)),
                years=years,
            )
        )

    run_ids_with_speakers = {run_id for run_id, count in speakers_found_by_run.items() if count > 0}

    totals = DashboardOverviewTotalsOut(
        complete_runs_considered=len(run_ids_with_speakers),
        conferences_scraped=len(conferences),
        conference_years_scraped=len(included_year_ids),
        speakers_found_extracted=sum(item.speakers_found_extracted for item in conferences),
        unique_speakers_db=total_unique_speakers,
        appearance_count_db=total_appearance_count,
        pages_visited=sum(item.pages_visited for item in conferences),
        links_discovered_unique=len(global_unique_links),
        good_pages_with_speakers=sum(item.good_pages_with_speakers for item in conferences),
    )

    return DashboardOverviewResponse(
        generated_at=datetime.now(timezone.utc),
        totals=totals,
        conferences=conferences,
    )

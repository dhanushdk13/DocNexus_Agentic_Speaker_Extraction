from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from sqlalchemy import inspect, select

from app.db import SessionLocal, engine
from app.models import Appearance, Conference, ConferenceYear, Physician, RunConferenceYear, RunEvent, RunStatus, ScrapeRun
from app.services.runs import run_manager


def test_schema_tables_and_constraints_exist() -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    expected = {
        "conferences",
        "conference_years",
        "sources",
        "physicians",
        "physician_aliases",
        "extractions",
        "appearances",
        "scrape_runs",
        "run_events",
        "run_conference_years",
    }
    assert expected.issubset(tables)

    conference_year_uniques = inspector.get_unique_constraints("conference_years")
    assert any({"conference_id", "year"} == set(item["column_names"]) for item in conference_year_uniques)

    physician_uniques = inspector.get_unique_constraints("physicians")
    assert any({"name_key"} == set(item["column_names"]) for item in physician_uniques)

    appearance_uniques = inspector.get_unique_constraints("appearances")
    assert any(
        {"physician_id", "conference_year_id", "session_title"} == set(item["column_names"]) for item in appearance_uniques
    )

    run_year_uniques = inspector.get_unique_constraints("run_conference_years")
    assert any({"run_id", "conference_year_id"} == set(item["column_names"]) for item in run_year_uniques)


def test_create_scrape_run_accepts_home_url(client, monkeypatch) -> None:
    captured: list[str] = []

    async def fake_enqueue(run_id: str) -> None:
        captured.append(run_id)

    monkeypatch.setattr(run_manager, "enqueue", fake_enqueue)

    response = client.post(
        "/api/v1/scrape-runs",
        json={"home_url": "https://example.org", "conference_name": "Example Conference"},
    )
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["status"] == "pending"
    assert body["home_url"] == "https://example.org/"
    assert body["conference_name"] == "Example Conference"
    assert captured and captured[0] == body["run_id"]


def test_create_scrape_run_validates_home_url(client) -> None:
    response = client.post(
        "/api/v1/scrape-runs",
        json={"home_url": "not-a-valid-url", "conference_name": "Example Conference"},
    )
    assert response.status_code == 422


def test_create_scrape_run_requires_conference_name(client) -> None:
    response = client.post(
        "/api/v1/scrape-runs",
        json={"home_url": "https://example.org"},
    )
    assert response.status_code == 422


def test_run_events_cursor_ordering(client) -> None:
    db = SessionLocal()
    conference = Conference(name="Event Cursor Test", canonical_name="event cursor test")
    db.add(conference)
    db.flush()

    year = ConferenceYear(conference_id=conference.id, year=2026)
    db.add(year)
    db.flush()

    run = ScrapeRun(id=str(uuid4()), home_url="https://example.org", status=RunStatus.running)
    db.add(run)
    db.flush()

    db.add_all(
        [
            RunEvent(run_id=run.id, conference_year_id=year.id, stage="preflight", level="info", message="preflight"),
            RunEvent(run_id=run.id, conference_year_id=year.id, stage="discover_links", level="info", message="discover done"),
            RunEvent(run_id=run.id, conference_year_id=year.id, stage="llm_normalize", level="info", message="normalize done"),
        ]
    )
    db.commit()
    db.close()

    response_one = client.get(f"/api/v1/scrape-runs/{run.id}/events")
    assert response_one.status_code == 200
    body_one = response_one.json()
    assert len(body_one["events"]) == 3

    cursor = body_one["events"][1]["id"]
    response_two = client.get(f"/api/v1/scrape-runs/{run.id}/events?cursor={cursor}")
    assert response_two.status_code == 200
    body_two = response_two.json()
    assert len(body_two["events"]) == 1
    assert body_two["events"][0]["stage"] == "llm_normalize"


def test_get_scrape_run_returns_discovered_year_rows(client) -> None:
    db = SessionLocal()
    conference = Conference(name="Discovery Test", canonical_name="discovery test")
    db.add(conference)
    db.flush()

    year = ConferenceYear(conference_id=conference.id, year=2025)
    db.add(year)
    db.flush()

    run = ScrapeRun(id=str(uuid4()), home_url="https://example.org", status=RunStatus.partial)
    db.add(run)
    db.flush()

    from app.models import RunConferenceYear

    db.add(RunConferenceYear(run_id=run.id, conference_year_id=year.id))
    db.commit()
    db.close()

    response = client.get(f"/api/v1/scrape-runs/{run.id}")
    assert response.status_code == 200
    body = response.json()

    assert body["home_url"] == "https://example.org"
    assert len(body["years"]) == 1
    assert body["years"][0]["conference_name"] == "Discovery Test"
    assert body["years"][0]["year"] == 2025


def test_get_scrape_run_includes_progress_metrics_from_heartbeat(client) -> None:
    db = SessionLocal()
    run = ScrapeRun(id=str(uuid4()), home_url="https://example.org", status=RunStatus.running)
    db.add(run)
    db.flush()

    db.add(
        RunEvent(
            run_id=run.id,
            stage="progress_heartbeat",
            level="info",
            message="Progress heartbeat",
            data_json=json.dumps(
                {
                    "metrics": {
                        "pages_visited": 4,
                        "pages_enqueued": 6,
                        "speaker_candidates_found": 12,
                        "speaker_candidates_new": 9,
                        "normalized_speakers": 7,
                        "physicians_linked": 3,
                        "appearances_linked": 5,
                        "llm_calls": 4,
                        "llm_failures": 1,
                        "llm_calls_saved": 2,
                    },
                    "progress_state": {
                        "queue_estimate": 2,
                        "no_progress_streak": 1,
                        "last_stage": "resolution_checkpoint",
                        "last_update_at": "2026-03-02T10:30:00+00:00",
                    },
                }
            ),
        )
    )
    db.commit()
    db.close()

    response = client.get(f"/api/v1/scrape-runs/{run.id}")
    assert response.status_code == 200
    body = response.json()

    assert body["metrics"]["pages_visited"] == 4
    assert body["metrics"]["physicians_linked"] == 3
    assert body["metrics"]["appearances_linked"] == 5
    assert body["metrics"]["llm_calls_saved"] == 2
    assert body["progress_state"]["queue_estimate"] == 2
    assert body["progress_state"]["no_progress_streak"] == 1
    assert body["progress_state"]["last_stage"] == "resolution_checkpoint"


def test_conference_detail_and_physician_group_endpoints(client) -> None:
    db = SessionLocal()
    conference = Conference(name="Cardiology Summit", canonical_name="cardiology summit")
    db.add(conference)
    db.flush()

    year_2025 = ConferenceYear(conference_id=conference.id, year=2025)
    year_2026 = ConferenceYear(conference_id=conference.id, year=2026)
    db.add_all([year_2025, year_2026])
    db.flush()

    physician = Physician(full_name="Mina Rao", name_key="mina rao", primary_designation="MD")
    db.add(physician)
    db.flush()

    db.add(
        Appearance(
            physician_id=physician.id,
            conference_year_id=year_2025.id,
            session_title="Valve Strategies",
            confidence=0.8,
        )
    )
    db.commit()
    db.close()

    detail_resp = client.get(f"/api/v1/conferences/{conference.id}")
    assert detail_resp.status_code == 200
    detail_body = detail_resp.json()
    assert detail_body["name"] == "Cardiology Summit"
    assert detail_body["total_physicians"] == 1
    assert detail_body["total_appearances"] == 1

    grouped_resp = client.get(f"/api/v1/conferences/{conference.id}/physicians")
    assert grouped_resp.status_code == 200
    grouped_body = grouped_resp.json()
    assert any(group["year"] == 2025 for group in grouped_body)
    assert any(
        card["full_name"] == "Mina Rao"
        for group in grouped_body
        for card in group["physicians"]
    )


def test_physician_cards_endpoint_and_highlight_query(client) -> None:
    db = SessionLocal()
    conference = Conference(name="Neuro Forum", canonical_name="neuro forum")
    db.add(conference)
    db.flush()
    conference_year = ConferenceYear(conference_id=conference.id, year=2024)
    db.add(conference_year)
    db.flush()
    physician = Physician(full_name="Aman Mehta", name_key="aman mehta", primary_designation="PhD")
    db.add(physician)
    db.flush()
    db.add(
        Appearance(
            physician_id=physician.id,
            conference_year_id=conference_year.id,
            session_title="CNS Biomarkers",
            confidence=0.9,
        )
    )
    db.commit()
    db.close()

    cards_resp = client.get("/api/v1/physicians/cards")
    assert cards_resp.status_code == 200
    cards = cards_resp.json()
    assert any(card["full_name"] == "Aman Mehta" for card in cards)

    detail_resp = client.get(f"/api/v1/physicians/{physician.id}?fromConferenceId={conference.id}&fromYear=2024")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["highlight_conference_id"] == conference.id
    assert detail["highlight_year"] == 2024
    assert detail["appearances"][0]["conference_id"] == conference.id


def test_get_scrape_run_dashboard_prefers_run_log_years(client) -> None:
    db = SessionLocal()
    run = ScrapeRun(
        id=str(uuid4()),
        home_url="https://example.org/continuum-2025",
        conference_name="Continuum",
        status=RunStatus.complete,
    )
    db.add(run)
    db.flush()

    db.add(
        RunEvent(
            run_id=run.id,
            stage="progress_heartbeat",
            level="info",
            message="Progress heartbeat",
            data_json=json.dumps(
                {
                    "metrics": {
                        "speaker_candidates_found": 47,
                        "normalized_speakers": 41,
                        "physicians_linked": 18,
                        "appearances_linked": 27,
                        "unresolved_attributions": 4,
                        "llm_calls": 52,
                        "llm_failures": 3,
                        "llm_calls_saved": 6,
                        "nav_reask_attempts": 2,
                        "nav_reask_successes": 1,
                    }
                }
            ),
        )
    )
    db.add(RunEvent(run_id=run.id, stage="physician_enrichment_start", level="info", message="start"))
    db.add(RunEvent(run_id=run.id, stage="physician_enriched", level="info", message="enriched"))
    db.add(RunEvent(run_id=run.id, stage="physician_enrichment_skipped", level="info", message="skipped"))
    db.commit()
    db.close()

    run_log_path = Path(__file__).resolve().parents[1] / "run_logs" / f"{run.id}.json"
    run_log_path.parent.mkdir(parents=True, exist_ok=True)
    run_log_path.write_text(
        json.dumps(
            {
                "years": [
                    {
                        "conference_year_id": 101,
                        "conference_name": "Continuum",
                        "year": 2025,
                        "status": "complete",
                        "linked": 27,
                        "duplicates": 3,
                        "notes": "Primary schedule page",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    try:
        response = client.get(f"/api/v1/scrape-runs/{run.id}/dashboard")
        assert response.status_code == 200
        body = response.json()

        assert body["run_id"] == run.id
        assert body["conference_name"] == "Continuum"
        assert body["summary"]["speakers_discovered"] == 47
        assert body["summary"]["normalized_speakers"] == 41
        assert body["summary"]["profiles_enrichment_started"] == 1
        assert body["summary"]["profiles_enriched"] == 1
        assert body["summary"]["profiles_enrichment_skipped"] == 1
        assert body["summary"]["appearances_linked"] == 27
        assert body["summary"]["llm_failures"] == 3
        assert body["summary"]["nav_reask_successes"] == 1

        assert len(body["conference_years"]) == 1
        assert body["conference_years"][0]["conference_year_id"] == 101
        assert body["conference_years"][0]["linked_appearances"] == 27
        assert body["conference_years"][0]["duplicate_links"] == 3
    finally:
        run_log_path.unlink(missing_ok=True)


def test_get_scrape_run_dashboard_falls_back_without_run_log(client) -> None:
    db = SessionLocal()
    conference = Conference(name="Continuum", canonical_name="continuum")
    db.add(conference)
    db.flush()

    conference_year = ConferenceYear(conference_id=conference.id, year=2024)
    db.add(conference_year)
    db.flush()

    run = ScrapeRun(
        id=str(uuid4()),
        home_url="https://example.org/continuum-2024",
        conference_name="Continuum",
        status=RunStatus.complete,
    )
    db.add(run)
    db.flush()
    db.add(RunConferenceYear(run_id=run.id, conference_year_id=conference_year.id))
    db.add(
        RunEvent(
            run_id=run.id,
            stage="progress_heartbeat",
            level="info",
            message="Progress heartbeat",
            data_json=json.dumps(
                {
                    "metrics": {
                        "speaker_candidates_found": 10,
                        "normalized_speakers": 8,
                        "appearances_linked": 2,
                    }
                }
            ),
        )
    )
    db.add(
        RunEvent(
            run_id=run.id,
            conference_year_id=conference_year.id,
            stage="link_created",
            level="info",
            message="linked appearance",
        )
    )
    db.add(
        RunEvent(
            run_id=run.id,
            conference_year_id=conference_year.id,
            stage="link_completion_created",
            level="info",
            message="completion linked appearance",
        )
    )
    db.commit()
    db.close()

    response = client.get(f"/api/v1/scrape-runs/{run.id}/dashboard")
    assert response.status_code == 200
    body = response.json()

    assert body["summary"]["conferences_scraped"] == 1
    assert body["summary"]["conference_year_entries"] == 1
    assert body["summary"]["unique_years_scraped"] == 1
    assert len(body["conference_years"]) == 1
    assert body["conference_years"][0]["conference_name"] == "Continuum"
    assert body["conference_years"][0]["year"] == 2024
    assert body["conference_years"][0]["linked_appearances"] == 2
    assert body["conference_years"][0]["duplicate_links"] == 0


def test_get_scrape_run_dashboard_returns_404_for_unknown_run(client) -> None:
    response = client.get(f"/api/v1/scrape-runs/{uuid4()}/dashboard")
    assert response.status_code == 404


def test_dashboard_overview_aggregates_db_outcomes_and_crawl_stats(client) -> None:
    db = SessionLocal()

    continuum = Conference(name="Continuum", canonical_name="continuum")
    acthiv = Conference(name="ACTHIV", canonical_name="acthiv")
    db.add_all([continuum, acthiv])
    db.flush()

    cy_2024 = ConferenceYear(conference_id=continuum.id, year=2024)
    cy_2025 = ConferenceYear(conference_id=continuum.id, year=2025)
    cy_2026 = ConferenceYear(conference_id=acthiv.id, year=2026)
    db.add_all([cy_2024, cy_2025, cy_2026])
    db.flush()

    p1 = Physician(full_name="Alice Rao", name_key="alice rao")
    p2 = Physician(full_name="Brian Li", name_key="brian li")
    p3 = Physician(full_name="Carla Diaz", name_key="carla diaz")
    db.add_all([p1, p2, p3])
    db.flush()

    db.add_all(
        [
            Appearance(physician_id=p1.id, conference_year_id=cy_2024.id, session_title="S1"),
            Appearance(physician_id=p2.id, conference_year_id=cy_2024.id, session_title="S2"),
            Appearance(physician_id=p1.id, conference_year_id=cy_2024.id, session_title="S3"),
            Appearance(physician_id=p1.id, conference_year_id=cy_2025.id, session_title="S4"),
            Appearance(physician_id=p3.id, conference_year_id=cy_2025.id, session_title="S5"),
            Appearance(physician_id=p2.id, conference_year_id=cy_2026.id, session_title="S6"),
        ]
    )
    db.flush()

    run_continuum = ScrapeRun(
        id=str(uuid4()),
        home_url="https://example.org/continuum",
        conference_name="Continuum",
        status=RunStatus.complete,
    )
    run_acthiv = ScrapeRun(
        id=str(uuid4()),
        home_url="https://example.org/acthiv",
        conference_name="ACTHIV",
        status=RunStatus.complete,
    )
    run_noncomplete = ScrapeRun(
        id=str(uuid4()),
        home_url="https://example.org/pending",
        conference_name="PendingConf",
        status=RunStatus.running,
    )
    db.add_all([run_continuum, run_acthiv, run_noncomplete])
    db.flush()

    db.add_all(
        [
            RunConferenceYear(run_id=run_continuum.id, conference_year_id=cy_2024.id),
            RunConferenceYear(run_id=run_continuum.id, conference_year_id=cy_2025.id),
            RunConferenceYear(run_id=run_acthiv.id, conference_year_id=cy_2026.id),
        ]
    )
    db.add_all(
        [
            RunEvent(
                run_id=run_continuum.id,
                stage="markdown_extract_end",
                level="info",
                message="extract",
                data_json=json.dumps({"speaker_count": 5}),
            ),
            RunEvent(
                run_id=run_continuum.id,
                stage="markdown_extract_end",
                level="info",
                message="extract",
                data_json=json.dumps({"speaker_count": 4}),
            ),
            RunEvent(
                run_id=run_acthiv.id,
                stage="markdown_extract_end",
                level="info",
                message="extract",
                data_json=json.dumps({"speaker_count": 3}),
            ),
            RunEvent(
                run_id=run_noncomplete.id,
                stage="markdown_extract_end",
                level="info",
                message="extract",
                data_json=json.dumps({"speaker_count": 999}),
            ),
        ]
    )
    db.commit()
    db.close()

    run_log_dir = Path(__file__).resolve().parents[1] / "run_logs"
    run_log_dir.mkdir(parents=True, exist_ok=True)
    log_continuum = run_log_dir / f"{run_continuum.id}.json"
    log_acthiv = run_log_dir / f"{run_acthiv.id}.json"

    log_continuum.write_text(
        json.dumps(
            {
                "urls": [
                    {"normalized_records": 10, "nav_next_urls": ["https://c.example/a", "https://c.example/b"]},
                    {"normalized_records": 1, "nav_next_urls": ["https://c.example/b", "https://c.example/c"]},
                    {"normalized_records": 0, "nav_next_urls": []},
                    {"normalized_records": 2, "nav_next_urls": ["https://c.example/d"]},
                ]
            }
        ),
        encoding="utf-8",
    )
    log_acthiv.write_text(
        json.dumps(
            {
                "urls": [
                    {"normalized_records": 5, "nav_next_urls": ["https://a.example/x"]},
                    {"normalized_records": 0, "nav_next_urls": ["https://a.example/x", "https://a.example/y"]},
                    {"normalized_records": 0, "nav_next_urls": []},
                ]
            }
        ),
        encoding="utf-8",
    )

    try:
        response = client.get("/api/v1/dashboard/overview")
        assert response.status_code == 200
        body = response.json()

        totals = body["totals"]
        assert totals["complete_runs_considered"] == 2
        assert totals["conferences_scraped"] == 2
        assert totals["conference_years_scraped"] == 3
        assert totals["speakers_found_extracted"] == 12
        assert totals["unique_speakers_db"] == 3
        assert totals["appearance_count_db"] == 6
        assert totals["pages_visited"] == 7
        assert totals["links_discovered_unique"] == 6
        assert totals["good_pages_with_speakers"] == 4

        by_conf = {item["conference_name"]: item for item in body["conferences"]}

        continuum_out = by_conf["Continuum"]
        assert continuum_out["years_scraped"] == 2
        assert continuum_out["unique_speakers_db"] == 3
        assert continuum_out["appearance_count_db"] == 5
        assert continuum_out["speakers_found_extracted"] == 9
        assert continuum_out["pages_visited"] == 4
        assert continuum_out["links_discovered_unique"] == 4
        assert continuum_out["good_pages_with_speakers"] == 3
        by_year_cont = {row["year"]: row for row in continuum_out["years"]}
        assert by_year_cont[2024]["unique_speakers_db"] == 2
        assert by_year_cont[2024]["appearance_count_db"] == 3
        assert by_year_cont[2025]["unique_speakers_db"] == 2
        assert by_year_cont[2025]["appearance_count_db"] == 2

        acthiv_out = by_conf["ACTHIV"]
        assert acthiv_out["years_scraped"] == 1
        assert acthiv_out["unique_speakers_db"] == 1
        assert acthiv_out["appearance_count_db"] == 1
        assert acthiv_out["speakers_found_extracted"] == 3
        assert acthiv_out["pages_visited"] == 3
        assert acthiv_out["links_discovered_unique"] == 2
        assert acthiv_out["good_pages_with_speakers"] == 1
        assert acthiv_out["years"][0]["year"] == 2026
        assert acthiv_out["years"][0]["unique_speakers_db"] == 1
        assert acthiv_out["years"][0]["appearance_count_db"] == 1
    finally:
        log_continuum.unlink(missing_ok=True)
        log_acthiv.unlink(missing_ok=True)


def test_dashboard_overview_uses_event_fallback_when_run_log_missing(client) -> None:
    db = SessionLocal()

    conference = Conference(name="FallbackConf", canonical_name="fallbackconf")
    db.add(conference)
    db.flush()
    conference_year = ConferenceYear(conference_id=conference.id, year=2026)
    db.add(conference_year)
    db.flush()

    physician = Physician(full_name="Demo Physician", name_key="demo physician")
    db.add(physician)
    db.flush()

    db.add(Appearance(physician_id=physician.id, conference_year_id=conference_year.id, session_title="A"))
    db.flush()

    run = ScrapeRun(
        id=str(uuid4()),
        home_url="https://example.org/fallback",
        conference_name="FallbackConf",
        status=RunStatus.complete,
    )
    db.add(run)
    db.flush()

    db.add(RunConferenceYear(run_id=run.id, conference_year_id=conference_year.id))
    db.add_all(
        [
            RunEvent(run_id=run.id, stage="fetch_route", level="info", message="fetch 1"),
            RunEvent(run_id=run.id, stage="fetch_route", level="info", message="fetch 2"),
            RunEvent(
                run_id=run.id,
                stage="markdown_extract_end",
                level="info",
                message="extract",
                data_json=json.dumps({"speaker_count": 1}),
            ),
            RunEvent(
                run_id=run.id,
                stage="markdown_extract_end",
                level="info",
                message="extract",
                data_json=json.dumps({"speaker_count": 0}),
            ),
        ]
    )
    db.commit()
    db.close()

    response = client.get("/api/v1/dashboard/overview")
    assert response.status_code == 200
    body = response.json()

    conference_out = next(item for item in body["conferences"] if item["conference_name"] == "FallbackConf")
    assert conference_out["pages_visited"] == 2
    assert conference_out["links_discovered_unique"] == 0
    assert conference_out["good_pages_with_speakers"] == 1

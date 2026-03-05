from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select

from app.db import SessionLocal
from app.models import (
    Appearance,
    Conference,
    ConferenceYear,
    ConferenceYearStatus,
    Extraction,
    ExtractionArtifactType,
    FetchStatus,
    Physician,
    RunConferenceYear,
    RunEvent,
    RunStatus,
    ScrapeRun,
    Source,
    SourceCategory,
    SourceMethod,
)
from app.services import data_admin


def _seed_backup_fixture_data() -> None:
    db = SessionLocal()
    try:
        acthiv = Conference(name="ACTHIV", canonical_name="acthiv", event_series_name="ACTHIV")
        other = Conference(name="Other Neuro Congress", canonical_name="other-neuro", event_series_name="Neuro")
        db.add_all([acthiv, other])
        db.flush()

        y1 = ConferenceYear(conference_id=acthiv.id, year=2026, status=ConferenceYearStatus.complete)
        y2 = ConferenceYear(conference_id=other.id, year=2026, status=ConferenceYearStatus.complete)
        db.add_all([y1, y2])
        db.flush()

        physician = Physician(full_name="Rupa Patel", name_key="rupa patel", primary_designation="MD")
        db.add(physician)
        db.flush()

        db.add_all(
            [
                Appearance(physician_id=physician.id, conference_year_id=y1.id, session_title="ACTHIV Session", confidence=0.9),
                Appearance(physician_id=physician.id, conference_year_id=y2.id, session_title="Neuro Session", confidence=0.8),
            ]
        )
        db.flush()

        source = Source(
            conference_year_id=y1.id,
            url="https://acthiv.org/program",
            category=SourceCategory.official_program,
            method=SourceMethod.http_static,
            fetch_status=FetchStatus.fetched,
            content_type="html",
        )
        db.add(source)
        db.flush()
        db.add(
            Extraction(
                source_id=source.id,
                artifact_type=ExtractionArtifactType.candidate_blocks,
                data=json.dumps([{"name": "Rupa Patel"}]),
            )
        )

        run = ScrapeRun(
            home_url="https://acthiv.org",
            conference_name="ACTHIV",
            conference_id=acthiv.id,
            status=RunStatus.complete,
        )
        db.add(run)
        db.flush()
        db.add(RunConferenceYear(run_id=run.id, conference_year_id=y1.id))
        db.add(RunEvent(run_id=run.id, stage="run_complete", level="info", message="done", data_json="{}"))

        db.commit()
    finally:
        db.close()


def test_export_backup_includes_full_physician_history(tmp_path: Path) -> None:
    _seed_backup_fixture_data()

    bundle_dir = data_admin.export_backup(
        conference_tokens=["acthiv", "continuum"],
        include_full_physician_history=True,
        output_root=tmp_path / "backups",
    )

    manifest = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
    conferences = json.loads((bundle_dir / "conferences.json").read_text(encoding="utf-8"))
    appearances = json.loads((bundle_dir / "appearances.json").read_text(encoding="utf-8"))
    conference_years = json.loads((bundle_dir / "conference_years.json").read_text(encoding="utf-8"))

    assert manifest["total_rows"] > 0
    assert any(item["name"] == "ACTHIV" for item in conferences)
    assert any(item["name"] == "Other Neuro Congress" for item in conferences)
    assert len(appearances) == 2
    assert len(conference_years) == 2


def test_wipe_and_restore_round_trip(tmp_path: Path, monkeypatch) -> None:
    _seed_backup_fixture_data()
    bundle_dir = data_admin.export_backup(
        conference_tokens=["acthiv", "continuum"],
        include_full_physician_history=True,
        output_root=tmp_path / "backups",
    )

    fake_root = tmp_path / "repo"
    (fake_root / "backend" / "run_logs").mkdir(parents=True, exist_ok=True)
    (fake_root / "backend" / ".pytest_cache").mkdir(parents=True, exist_ok=True)
    (fake_root / "frontend" / ".next").mkdir(parents=True, exist_ok=True)
    (fake_root / "backend" / "app" / "__pycache__").mkdir(parents=True, exist_ok=True)
    (fake_root / "backend" / "run_logs" / "sample.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(data_admin, "_repo_root", lambda: fake_root)
    wipe_result = data_admin.wipe_all_data_and_artifacts()
    assert wipe_result["removed_paths"]

    db = SessionLocal()
    try:
        assert db.execute(select(Conference)).scalars().all() == []
    finally:
        db.close()

    restore_result = data_admin.restore_backup(bundle_dir)
    assert restore_result["rows_restored"]["conferences"] >= 2

    db = SessionLocal()
    try:
        conferences = db.execute(select(Conference).order_by(Conference.name.asc())).scalars().all()
        assert len(conferences) >= 2
    finally:
        db.close()

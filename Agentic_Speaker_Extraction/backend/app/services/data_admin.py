from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.db import SessionLocal, engine
from app.models import (
    Appearance,
    Conference,
    ConferenceYear,
    Extraction,
    Physician,
    PhysicianAlias,
    RunConferenceYear,
    RunEvent,
    ScrapeRun,
    Source,
)


TABLE_EXPORT_ORDER: list[tuple[str, Any]] = [
    ("conferences", Conference),
    ("conference_years", ConferenceYear),
    ("sources", Source),
    ("extractions", Extraction),
    ("physicians", Physician),
    ("physician_aliases", PhysicianAlias),
    ("appearances", Appearance),
    ("scrape_runs", ScrapeRun),
    ("run_conference_years", RunConferenceYear),
    ("run_events", RunEvent),
]

TABLE_IMPORT_ORDER = [
    "conferences",
    "conference_years",
    "physicians",
    "physician_aliases",
    "scrape_runs",
    "run_conference_years",
    "sources",
    "extractions",
    "appearances",
    "run_events",
]

TRUNCATE_TABLES = [
    "run_events",
    "run_conference_years",
    "scrape_runs",
    "extractions",
    "sources",
    "appearances",
    "physician_aliases",
    "physicians",
    "conference_years",
    "conferences",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_name(name: str) -> str:
    out = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in name.strip().lower())
    return out.strip("-_") or "backup"


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _row_to_dict(row: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for column in row.__table__.columns:
        payload[column.name] = _json_default(getattr(row, column.name))
    return payload


def _write_json(path: Path, payload: Any) -> str:
    rendered = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    path.write_text(rendered, encoding="utf-8")
    return sha256(rendered.encode("utf-8")).hexdigest()


def _load_target_conference_ids(db: Session, tokens: list[str]) -> set[int]:
    lowered_tokens = [token.strip().lower() for token in tokens if token.strip()]
    if not lowered_tokens:
        return set()
    filters = []
    for token in lowered_tokens:
        like_value = f"%{token}%"
        filters.append(func.lower(Conference.name).like(like_value))
        filters.append(func.lower(func.coalesce(Conference.canonical_name, "")).like(like_value))
        filters.append(func.lower(func.coalesce(Conference.event_series_name, "")).like(like_value))
    combined = filters[0]
    for item in filters[1:]:
        combined = combined | item
    stmt = select(Conference.id).where(combined)
    return {int(row[0]) for row in db.execute(stmt).all()}


def _load_rows_by_ids(db: Session, model: Any, id_field: str, ids: set[int] | set[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    column = getattr(model, id_field)
    rows = db.execute(select(model).where(column.in_(ids))).scalars().all()
    return [_row_to_dict(row) for row in rows]


def export_backup(
    *,
    conference_tokens: list[str] | None = None,
    include_full_physician_history: bool = True,
    output_root: Path | None = None,
) -> Path:
    tokens = conference_tokens or ["acthiv", "continuum"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_root = output_root or (_repo_root() / "backups")
    bundle_dir = output_root / f"prewipe-{stamp}"
    bundle_dir.mkdir(parents=True, exist_ok=False)

    with SessionLocal() as db:
        conference_ids = _load_target_conference_ids(db, tokens)
        conference_year_ids = {
            int(row[0])
            for row in db.execute(select(ConferenceYear.id).where(ConferenceYear.conference_id.in_(conference_ids))).all()
        } if conference_ids else set()

        seed_physician_ids = {
            int(row[0])
            for row in db.execute(
                select(Appearance.physician_id).where(Appearance.conference_year_id.in_(conference_year_ids))
            ).all()
        } if conference_year_ids else set()

        if include_full_physician_history and seed_physician_ids:
            appearances = db.execute(select(Appearance).where(Appearance.physician_id.in_(seed_physician_ids))).scalars().all()
        elif seed_physician_ids:
            appearances = db.execute(select(Appearance).where(Appearance.conference_year_id.in_(conference_year_ids))).scalars().all()
        else:
            appearances = []

        appearance_rows = [_row_to_dict(item) for item in appearances]
        physician_ids = {int(item["physician_id"]) for item in appearance_rows} | seed_physician_ids
        all_conference_year_ids = {int(item["conference_year_id"]) for item in appearance_rows} | conference_year_ids
        all_conference_ids = conference_ids | {
            int(row[0])
            for row in db.execute(select(ConferenceYear.conference_id).where(ConferenceYear.id.in_(all_conference_year_ids))).all()
        } if all_conference_year_ids else conference_ids

        source_rows = _load_rows_by_ids(db, Source, "conference_year_id", all_conference_year_ids)
        source_ids = {int(item["id"]) for item in source_rows}
        extraction_rows = _load_rows_by_ids(db, Extraction, "source_id", source_ids)

        run_ids_from_years = {
            str(row[0])
            for row in db.execute(
                select(RunConferenceYear.run_id).where(RunConferenceYear.conference_year_id.in_(all_conference_year_ids))
            ).all()
        } if all_conference_year_ids else set()
        run_ids_from_conference = {
            str(row[0])
            for row in db.execute(select(ScrapeRun.id).where(ScrapeRun.conference_id.in_(all_conference_ids))).all()
        } if all_conference_ids else set()
        run_ids = run_ids_from_years | run_ids_from_conference

        payloads: dict[str, list[dict[str, Any]]] = {
            "conferences": _load_rows_by_ids(db, Conference, "id", all_conference_ids),
            "conference_years": _load_rows_by_ids(db, ConferenceYear, "id", all_conference_year_ids),
            "sources": source_rows,
            "extractions": extraction_rows,
            "physicians": _load_rows_by_ids(db, Physician, "id", physician_ids),
            "physician_aliases": _load_rows_by_ids(db, PhysicianAlias, "physician_id", physician_ids),
            "appearances": appearance_rows,
            "scrape_runs": _load_rows_by_ids(db, ScrapeRun, "id", run_ids),
            "run_conference_years": _load_rows_by_ids(db, RunConferenceYear, "run_id", run_ids),
            "run_events": _load_rows_by_ids(db, RunEvent, "run_id", run_ids),
        }

    files_meta: list[dict[str, Any]] = []
    total_rows = 0
    for key, rows in payloads.items():
        rows_sorted = sorted(rows, key=lambda item: json.dumps(item, sort_keys=True, default=str))
        out_path = bundle_dir / f"{key}.json"
        digest = _write_json(out_path, rows_sorted)
        files_meta.append({"name": out_path.name, "rows": len(rows_sorted), "sha256": digest})
        total_rows += len(rows_sorted)

    manifest = {
        "created_at": _utc_now_iso(),
        "conference_tokens": tokens,
        "include_full_physician_history": include_full_physician_history,
        "total_rows": total_rows,
        "files": files_meta,
    }
    _write_json(bundle_dir / "manifest.json", manifest)
    return bundle_dir


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
        return
    path.unlink(missing_ok=True)


def wipe_all_data_and_artifacts() -> dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {', '.join(TRUNCATE_TABLES)} RESTART IDENTITY CASCADE"))
        conn.commit()

    repo_root = _repo_root()
    removed: list[str] = []

    for pattern in ["backend/run_logs/*.json"]:
        for file_path in repo_root.glob(pattern):
            file_path.unlink(missing_ok=True)
            removed.append(str(file_path.relative_to(repo_root)))

    for relative in ["backend/.pytest_cache", "frontend/.next"]:
        target = repo_root / relative
        if target.exists():
            _remove_path(target)
            removed.append(relative)

    skip_roots = {".git", ".venv", "node_modules"}
    for pycache_path in repo_root.rglob("__pycache__"):
        if any(part in skip_roots for part in pycache_path.parts):
            continue
        _remove_path(pycache_path)
        removed.append(str(pycache_path.relative_to(repo_root)))

    return {"wiped_at": _utc_now_iso(), "removed_paths": sorted(set(removed))}


def _load_backup_payloads(backup_dir: Path) -> dict[str, list[dict[str, Any]]]:
    payloads: dict[str, list[dict[str, Any]]] = {}
    for table_name in TABLE_IMPORT_ORDER:
        file_path = backup_dir / f"{table_name}.json"
        if not file_path.exists():
            payloads[table_name] = []
            continue
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"Invalid payload in {file_path}")
        payloads[table_name] = payload
    return payloads


def restore_backup(backup_path: str | Path) -> dict[str, Any]:
    backup_dir = Path(backup_path)
    if not backup_dir.exists():
        raise FileNotFoundError(f"Backup path not found: {backup_dir}")

    payloads = _load_backup_payloads(backup_dir)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {', '.join(TRUNCATE_TABLES)} RESTART IDENTITY CASCADE"))
        for table_name in TABLE_IMPORT_ORDER:
            rows = payloads.get(table_name, [])
            if not rows:
                continue
            table = next(model.__table__ for name, model in TABLE_EXPORT_ORDER if name == table_name)
            conn.execute(table.insert(), rows)

        for table_name in [
            "conferences",
            "conference_years",
            "sources",
            "extractions",
            "physicians",
            "physician_aliases",
            "appearances",
            "run_conference_years",
            "run_events",
        ]:
            conn.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), "
                    f"COALESCE((SELECT MAX(id) FROM {table_name}), 1), "
                    f"(SELECT COUNT(*) > 0 FROM {table_name}))"
                )
            )

    return {
        "restored_at": _utc_now_iso(),
        "backup_path": str(backup_dir),
        "rows_restored": {table: len(rows) for table, rows in payloads.items()},
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data admin for backup/wipe/restore.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export backup only")
    export_parser.add_argument(
        "--conference-token",
        dest="conference_tokens",
        action="append",
        default=[],
        help="Conference token to include (repeatable). Defaults to acthiv + continuum.",
    )
    export_parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=None,
        help="Output root directory for backup bundles.",
    )
    export_parser.add_argument(
        "--no-full-physician-history",
        dest="full_history",
        action="store_false",
        help="Include only appearances directly tied to target conferences.",
    )
    export_parser.set_defaults(full_history=True)

    subparsers.add_parser("wipe", help="Wipe DB + runtime artifacts")

    backup_wipe_parser = subparsers.add_parser("backup-and-wipe", help="Export backup then wipe")
    backup_wipe_parser.add_argument(
        "--conference-token",
        dest="conference_tokens",
        action="append",
        default=[],
        help="Conference token to include (repeatable). Defaults to acthiv + continuum.",
    )
    backup_wipe_parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=None,
        help="Output root directory for backup bundles.",
    )
    backup_wipe_parser.add_argument(
        "--no-full-physician-history",
        dest="full_history",
        action="store_false",
        help="Include only appearances directly tied to target conferences.",
    )
    backup_wipe_parser.set_defaults(full_history=True)

    restore_parser = subparsers.add_parser("restore", help="Restore backup bundle")
    restore_parser.add_argument("--backup-path", dest="backup_path", required=True, help="Path to backup bundle")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    command = args.command

    if command == "export":
        output_dir = Path(args.output_dir) if args.output_dir else None
        bundle_dir = export_backup(
            conference_tokens=args.conference_tokens or ["acthiv", "continuum"],
            include_full_physician_history=bool(args.full_history),
            output_root=output_dir,
        )
        print(json.dumps({"status": "ok", "backup_dir": str(bundle_dir)}, ensure_ascii=True))
        return

    if command == "wipe":
        result = wipe_all_data_and_artifacts()
        print(json.dumps({"status": "ok", **result}, ensure_ascii=True))
        return

    if command == "backup-and-wipe":
        output_dir = Path(args.output_dir) if args.output_dir else None
        bundle_dir = export_backup(
            conference_tokens=args.conference_tokens or ["acthiv", "continuum"],
            include_full_physician_history=bool(args.full_history),
            output_root=output_dir,
        )
        wipe_result = wipe_all_data_and_artifacts()
        print(
            json.dumps(
                {"status": "ok", "backup_dir": str(bundle_dir), "wipe": wipe_result},
                ensure_ascii=True,
            )
        )
        return

    if command == "restore":
        result = restore_backup(args.backup_path)
        print(json.dumps({"status": "ok", **result}, ensure_ascii=True))
        return

    raise SystemExit(f"Unsupported command: {command}")


if __name__ == "__main__":
    main()

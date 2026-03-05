from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Conference, ConferenceYear, RunConferenceYear, RunEvent, RunStatus, ScrapeRun
from app.schemas.scrape_runs import (
    DashboardOverviewResponse,
    DiscoveredConferenceYearStatus,
    RunDashboardResponse,
    RunEventOut,
    RunEventsResponse,
    ScrapeRunListItemOut,
    ScrapeRunListResponse,
    ScrapeRunCancelResponse,
    RunMetricsOut,
    RunProgressStateOut,
    ScrapeRunCreate,
    ScrapeRunCreateResponse,
    ScrapeRunStatusResponse,
)
from app.services.dashboard_overview import build_dashboard_overview
from app.services.run_dashboard import build_run_dashboard
from app.services.runs import run_manager

router = APIRouter(tags=["scrape-runs"])


def _default_metrics() -> RunMetricsOut:
    return RunMetricsOut()


def _default_progress_state() -> RunProgressStateOut:
    return RunProgressStateOut()


def _extract_run_progress(db: Session, run_id: str) -> tuple[RunMetricsOut, RunProgressStateOut]:
    heartbeat = db.execute(
        select(RunEvent)
        .where(RunEvent.run_id == run_id)
        .where(RunEvent.stage == "progress_heartbeat")
        .order_by(RunEvent.id.desc())
        .limit(1)
    ).scalar_one_or_none()

    metrics = _default_metrics()
    progress = _default_progress_state()
    if heartbeat and heartbeat.data_json:
        try:
            payload = json.loads(heartbeat.data_json)
            if isinstance(payload, dict):
                metrics = RunMetricsOut.model_validate(payload.get("metrics") or {})
                progress_payload = payload.get("progress_state") or {}
                if isinstance(progress_payload, dict) and progress_payload.get("last_update_at"):
                    try:
                        progress_payload["last_update_at"] = datetime.fromisoformat(progress_payload["last_update_at"])
                    except ValueError:
                        progress_payload["last_update_at"] = None
                progress = RunProgressStateOut.model_validate(progress_payload)
                return metrics, progress
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    run_complete = db.execute(
        select(RunEvent)
        .where(RunEvent.run_id == run_id)
        .where(RunEvent.stage == "run_complete")
        .order_by(RunEvent.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if run_complete and run_complete.data_json:
        try:
            payload = json.loads(run_complete.data_json)
            if isinstance(payload, dict):
                metrics = RunMetricsOut.model_validate(payload.get("metrics") or {})
                progress_payload = payload.get("progress_state") or {}
                if isinstance(progress_payload, dict) and progress_payload.get("last_update_at"):
                    try:
                        progress_payload["last_update_at"] = datetime.fromisoformat(progress_payload["last_update_at"])
                    except ValueError:
                        progress_payload["last_update_at"] = None
                progress = RunProgressStateOut.model_validate(progress_payload)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    return metrics, progress


@router.post("/scrape-runs", response_model=ScrapeRunCreateResponse)
async def create_scrape_run(payload: ScrapeRunCreate, db: Session = Depends(get_db)) -> ScrapeRunCreateResponse:
    run = ScrapeRun(
        home_url=str(payload.home_url),
        conference_name=payload.conference_name.strip()[:255],
    )
    db.add(run)
    db.commit()

    await run_manager.enqueue(run.id)

    return ScrapeRunCreateResponse(
        run_id=run.id,
        status=run.status,
        home_url=run.home_url,
        conference_name=run.conference_name,
    )


@router.post("/scrape-runs/{run_id}/cancel", response_model=ScrapeRunCancelResponse)
async def cancel_scrape_run(run_id: str, db: Session = Depends(get_db)) -> ScrapeRunCancelResponse:
    run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    if run.status in {RunStatus.complete, RunStatus.partial, RunStatus.error}:
        return ScrapeRunCancelResponse(
            run_id=run.id,
            status=run.status,
            cancelled=False,
            message=f"Run already in terminal state: {run.status.value}",
        )

    await run_manager.cancel(run.id)
    db.add(
        RunEvent(
            run_id=run.id,
            conference_year_id=None,
            stage="run_cancel_requested",
            level="warning",
            message="Cancellation requested by user",
            data_json='{"source":"ui"}',
        )
    )
    db.commit()

    return ScrapeRunCancelResponse(
        run_id=run.id,
        status=run.status,
        cancelled=True,
        message="Cancellation requested",
    )


@router.get("/scrape-runs", response_model=ScrapeRunListResponse)
def list_scrape_runs(
    limit: int = Query(default=20, ge=1, le=100),
    status: list[RunStatus] | None = Query(default=None),
    db: Session = Depends(get_db),
) -> ScrapeRunListResponse:
    stmt = select(ScrapeRun).order_by(ScrapeRun.created_at.desc())
    if status:
        stmt = stmt.where(ScrapeRun.status.in_(status))
    rows = db.execute(stmt.limit(limit)).scalars().all()

    return ScrapeRunListResponse(
        runs=[
            ScrapeRunListItemOut(
                run_id=row.id,
                status=row.status,
                home_url=row.home_url,
                conference_name=row.conference_name,
                created_at=row.created_at,
                finished_at=row.finished_at,
            )
            for row in rows
        ]
    )


@router.get("/scrape-runs/{run_id}", response_model=ScrapeRunStatusResponse)
def get_scrape_run(run_id: str, db: Session = Depends(get_db)) -> ScrapeRunStatusResponse:
    run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    discovered_rows = db.execute(
        select(RunConferenceYear, ConferenceYear, Conference)
        .join(ConferenceYear, ConferenceYear.id == RunConferenceYear.conference_year_id)
        .join(Conference, Conference.id == ConferenceYear.conference_id)
        .where(RunConferenceYear.run_id == run.id)
        .order_by(Conference.name.asc(), ConferenceYear.year.asc())
    ).all()

    years: list[DiscoveredConferenceYearStatus] = []
    for _, conference_year, conference in discovered_rows:
        years.append(
            DiscoveredConferenceYearStatus(
                conference_year_id=conference_year.id,
                conference_name=conference.name,
                year=conference_year.year,
                status=conference_year.status,
                notes=conference_year.notes,
            )
        )

    metrics, progress_state = _extract_run_progress(db, run.id)

    return ScrapeRunStatusResponse(
        run_id=run.id,
        home_url=run.home_url,
        conference_name=run.conference_name,
        status=run.status,
        created_at=run.created_at,
        finished_at=run.finished_at,
        years=years,
        metrics=metrics,
        progress_state=progress_state,
    )


@router.get("/scrape-runs/{run_id}/dashboard", response_model=RunDashboardResponse)
def get_scrape_run_dashboard(run_id: str, db: Session = Depends(get_db)) -> RunDashboardResponse:
    run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    metrics, _ = _extract_run_progress(db, run.id)
    return build_run_dashboard(db, run=run, metrics=metrics)


@router.get("/dashboard/overview", response_model=DashboardOverviewResponse)
def get_dashboard_overview(db: Session = Depends(get_db)) -> DashboardOverviewResponse:
    return build_dashboard_overview(db)


@router.get("/scrape-runs/{run_id}/events", response_model=RunEventsResponse)
def get_run_events(
    run_id: str,
    cursor: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> RunEventsResponse:
    run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    stmt = select(RunEvent).where(RunEvent.run_id == run_id)
    if cursor is not None:
        stmt = stmt.where(RunEvent.id > cursor)
    stmt = stmt.order_by(RunEvent.id.asc()).limit(limit)

    rows = db.execute(stmt).scalars().all()
    next_cursor = rows[-1].id if rows else cursor

    return RunEventsResponse(
        next_cursor=next_cursor,
        events=[
            RunEventOut(
                id=row.id,
                run_id=row.run_id,
                conference_year_id=row.conference_year_id,
                stage=row.stage,
                level=row.level,
                message=row.message,
                data_json=row.data_json,
                created_at=row.created_at,
            )
            for row in rows
        ],
    )

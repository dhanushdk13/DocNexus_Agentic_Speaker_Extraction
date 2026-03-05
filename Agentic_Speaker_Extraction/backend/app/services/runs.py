from __future__ import annotations

import asyncio
import heapq
import hashlib
import json
import random
import re
import time
import threading
import queue as thread_queue
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import SessionLocal
from app.models import (
    Appearance,
    Conference,
    ConferenceYear,
    ConferenceYearStatus,
    Extraction,
    ExtractionArtifactType,
    FetchStatus,
    RunConferenceYear,
    RunEvent,
    RunStatus,
    ScrapeRun,
    Source,
    SourceCategory,
    SourceMethod,
)
from app.services.attribution_llm import AttributionTarget, resolve_attribution, resolve_attribution_batch
from app.services.crawl_fetch import CrawlPageResult, fetch_crawl_page
from app.services.dedupe import get_or_create_physician, is_physician_like, merge_close_physicians, normalize_text
from app.services.extract_candidates import (
    extract_blocks_from_html,
    extract_blocks_from_pdf_text,
    extract_embedded_candidates,
    extract_network_candidates,
    extract_page_title,
    extract_pdf_text_with_scan_flag,
    extract_session_speaker_pairs,
    extract_visible_text,
    prioritize_event_content_html,
    sanitize_conference_context_text,
)
from app.services.extract_llm import (
    ExtractedSpeaker,
    generate_talk_brief,
    heuristic_normalize_candidates,
    normalize_candidates,
)
from app.services.frontier import BranchStats, branch_id_for_url, branch_yield_score, template_key_for_url
from app.services.interaction_explorer import explore_interactions
from app.services.llm_routing import select_llm_model, select_llm_timeout
from app.services.name_cleaner import canonicalize_person_name
from app.services.fetchers import (
    PlaywrightDomainSessionManager,
    fetch_http,
    serialize_network_payloads,
)
from app.services.conference_identity import infer_conference_identity
from app.services.extractor_agent import expand_all_js_script, should_attempt_modal_breaker
from app.services.navigation_llm import NavigationCandidate, NavigationDecisionDebug, decide_next
from app.services.pathfinder_agent import PathfinderDecision, decide_pathfinder, infer_page_intent
from app.services.page_reasoner import extract_and_decide
from app.services.memory_store import get_template_memory_scores, update_template_memory
from app.services.physician_enrichment import enrich_physician_profile


@dataclass(slots=True)
class RunTask:
    run_id: str


class RunManager:
    def __init__(self) -> None:
        # Run execution is CPU-heavy and uses sync DB calls; running it on the
        # same asyncio loop as FastAPI request handling can starve the server.
        # Keep the worker on its own thread+event loop.
        self._queue: thread_queue.Queue[RunTask | None] | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cancel_requested: set[str] = set()
        self._active_run_id: str | None = None
        self._active_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._queue = thread_queue.Queue()
        self._thread = threading.Thread(target=self._thread_main, name="scrape-run-worker", daemon=True)
        self._thread.start()

    async def stop(self) -> None:
        if not self._thread or not self._queue:
            return
        self._queue.put(None)
        self._thread.join(timeout=10)
        self._thread = None
        self._queue = None
        self._loop = None

    async def enqueue(self, run_id: str) -> None:
        if self._queue is None:
            await self.start()
        assert self._queue is not None
        self._queue.put(RunTask(run_id=run_id))

    async def cancel(self, run_id: str) -> None:
        self._cancel_requested.add(run_id)
        # Best-effort: if the worker is currently executing this run and is blocked
        # in a long await (LLM/fetch), cooperatively cancelling via a flag may take
        # a long time. Cancel the active asyncio task so the run can exit promptly.
        if (
            self._active_run_id == run_id
            and self._active_task
            and not self._active_task.done()
            and self._loop is not None
        ):
            self._loop.call_soon_threadsafe(self._active_task.cancel)

    def is_cancel_requested(self, run_id: str) -> bool:
        return run_id in self._cancel_requested

    def clear_cancel_request(self, run_id: str) -> None:
        self._cancel_requested.discard(run_id)

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._worker_loop())
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()

    async def _worker_loop(self) -> None:
        assert self._queue is not None
        while True:
            task = await asyncio.to_thread(self._queue.get)
            if task is None:
                break
            try:
                self._active_run_id = task.run_id
                self._active_task = asyncio.create_task(execute_run(task.run_id), name=f"scrape-run:{task.run_id}")
                try:
                    await self._active_task
                finally:
                    self._active_task = None
                    self._active_run_id = None
            finally:
                try:
                    self._queue.task_done()
                except Exception:
                    pass


run_manager = RunManager()


@dataclass(slots=True)
class DomainGuardState:
    request_total: int = 0
    blocked_count: int = 0
    minute_window: deque[float] = field(default_factory=deque)
    last_request_at: float | None = None
    stopped: bool = False


@dataclass(slots=True)
class FrontierNode:
    priority: float
    sequence: int
    url: str
    canonical_url: str
    depth: int
    branch_id: str
    llm_priority: float
    estimated_yield: float
    novelty_score: float
    enqueued_at: float


def _event(
    db: Session,
    run_id: str,
    stage: str,
    message: str,
    *,
    conference_year_id: int | None = None,
    level: str = "info",
    data: dict[str, Any] | None = None,
) -> None:
    db.add(
        RunEvent(
            run_id=run_id,
            conference_year_id=conference_year_id,
            stage=stage,
            level=level,
            message=message,
            data_json=json.dumps(data, ensure_ascii=True) if data else None,
        )
    )
    db.flush()


def _run_log_path(run_id: str) -> Path:
    log_dir = Path(__file__).resolve().parents[2] / "run_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"{run_id}.json"


def _write_run_debug_log(run_id: str, payload: dict[str, Any]) -> str:
    path = _run_log_path(run_id)
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return str(path)


def _new_debug_counters() -> dict[str, int]:
    return {
        "llm_attempts": 0,
        "llm_failures": 0,
        "heuristic_fallbacks": 0,
        "blocked_pages": 0,
        "unresolved_attributions": 0,
        "linked_appearances": 0,
        "duplicate_links_skipped": 0,
        "llm_calls_saved": 0,
        "llm_batches_started": 0,
        "llm_batches_completed": 0,
        "llm_batches_timed_out": 0,
        "stalls_recovered": 0,
        "stalls_terminal": 0,
        "pathfinder_llm_attempts": 0,
        "pathfinder_llm_failures": 0,
        "attribution_resolved_count": 0,
        "attribution_reconcile_resolved_count": 0,
        "attribution_final_unresolved_count": 0,
    }


def _new_run_metrics() -> dict[str, int]:
    return {
        "pages_visited": 0,
        "pages_enqueued": 0,
        "unique_url_states": 0,
        "frontier_size": 0,
        "branch_count": 0,
        "adaptive_budget_current": 0,
        "adaptive_budget_max": 0,
        "interaction_actions_total": 0,
        "high_yield_branches": 0,
        "pages_skipped_budget": 0,
        "template_clusters_discovered": 0,
        "speaker_candidates_found": 0,
        "speaker_candidates_new": 0,
        "normalized_speakers": 0,
        "physicians_linked": 0,
        "appearances_linked": 0,
        "unresolved_attributions": 0,
        "llm_calls": 0,
        "llm_failures": 0,
        "llm_calls_saved": 0,
        "llm_batches_started": 0,
        "llm_batches_completed": 0,
        "llm_batches_timed_out": 0,
        "stalls_recovered": 0,
        "stalls_terminal": 0,
        "repeated_state_skips": 0,
        "gatekeeper_links_found": 0,
        "modal_breaker_attempts": 0,
        "modal_breaker_successes": 0,
        "dynamic_pages_detected": 0,
        "pathfinder_llm_calls": 0,
        "pathfinder_llm_failures": 0,
        "novelty_windows_without_progress": 0,
        "markdown_pages_processed": 0,
        "markdown_chars_processed": 0,
        "markdown_segments_used": 0,
        "memory_templates_hit": 0,
        "memory_templates_promoted": 0,
        "legacy_fallback_pages": 0,
        "attribution_resolved_count": 0,
        "attribution_reconcile_resolved_count": 0,
        "attribution_final_unresolved_count": 0,
        "pages_with_zero_speakers_nonzero_links": 0,
        "branches_closed_no_links": 0,
        "nav_reask_attempts": 0,
        "nav_reask_successes": 0,
    }


def _new_progress_state() -> dict[str, Any]:
    return {
        "queue_estimate": 0,
        "no_progress_streak": 0,
        "last_stage": "run_start",
        "last_update_at": datetime.now(timezone.utc).isoformat(),
    }


TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {"fbclid", "gclid", "ref", "ref_src", "mc_cid", "mc_eid"}
NON_PERSON_PHRASES = {
    "conference program",
    "general guidelines",
    "eastern standard",
    "central daylight",
    "time zone",
    "abstract submission",
    "conference registration",
    "program schedule",
}
NAV_FOCUS_IGNORE_TOKENS = {
    "www",
    "com",
    "org",
    "net",
    "home",
    "about",
    "contact",
    "press",
    "blog",
    "news",
    "post",
    "article",
    "resource",
    "resources",
    "privacy",
    "policy",
    "terms",
    "code",
    "ethics",
    "staff",
    "membership",
    "partnerships",
    "education",
    "advocacy",
    "conference",
    "conferences",
    "event",
    "events",
}
NAV_SIGNAL_TOKENS = {
    "program",
    "agenda",
    "session",
    "sessions",
    "speaker",
    "speakers",
    "faculty",
    "presenter",
    "presenters",
    "abstract",
    "abstracts",
    "plenary",
    "poster",
    "posters",
    "oral",
    "schedule",
    "workshop",
    "workshops",
    "symposium",
    "symposia",
}
NAV_EXCLUDE_TOKENS = {
    "housing",
    "hotel",
    "travel",
    "venue",
    "sponsor",
    "sponsors",
    "sponsorship",
    "exhibitor",
    "exhibit",
    "register",
    "registration",
    "book",
    "room",
    "donate",
    "press",
    "media",
    "career",
    "careers",
    "job",
    "jobs",
    "advertise",
    "advertising",
    "cookie",
}
NAV_GENERIC_ANCHOR_PHRASES = {
    "view archives",
    "view archive",
    "view details",
    "learn more",
    "read more",
    "details",
}
DESIGNATION_MAP = {
    "MD": "MD",
    "M.D": "MD",
    "M.D.": "MD",
    "DO": "DO",
    "D.O": "DO",
    "D.O.": "DO",
    "OD": "OD",
    "O.D": "OD",
    "O.D.": "OD",
    "PHD": "PhD",
    "PH.D": "PhD",
    "PH.D.": "PhD",
    "MBBS": "MBBS",
    "MS": "MS",
    "MPH": "MPH",
    "FAAO": "FAAO",
    "FACS": "FACS",
    "FRCS": "FRCS",
}
DESIGNATION_PATTERN = re.compile(r"\b(MD|M\.D\.?|DO|D\.O\.?|OD|O\.D\.?|PhD|Ph\.D\.?|MBBS|MS|MPH|FAAO|FACS|FRCS)\b", re.I)


def _source_category_for_url(url: str) -> SourceCategory:
    lowered = url.lower()
    if lowered.endswith(".pdf") or "pdf" in lowered:
        return SourceCategory.pdf_program
    if any(term in lowered for term in ["speaker", "faculty", "presenter"]):
        return SourceCategory.official_speakers
    if any(term in lowered for term in ["program", "agenda", "schedule", "session"]):
        return SourceCategory.official_program
    if any(term in lowered for term in ["cvent", "swapcard", "whova", "eventsair", "mapyourshow", "rainfocus"]):
        return SourceCategory.platform
    return SourceCategory.unknown


def _normalize_conference_name(value: str) -> str:
    cleaned = " ".join(value.split())
    return cleaned[:255]


def _title_from_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    root = host.split(".")[0] if host else "conference"
    root = re.sub(r"[^a-z0-9]+", " ", root).strip()
    if not root:
        root = "conference"
    return " ".join(word.capitalize() for word in root.split())


def _extract_years(value: str) -> list[int]:
    found = sorted({int(y) for y in re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", value or "")})
    return [y for y in found if 1990 <= y <= 2100]


def _canonical_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "https").lower()
    host = parsed.netloc.lower()
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    filtered_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        lower = key.lower()
        if any(lower.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        if lower in TRACKING_QUERY_KEYS:
            continue
        filtered_query.append((key, value))

    query = urlencode(sorted(filtered_query), doseq=True)
    return urlunparse((scheme, host, path, "", query, ""))


def _stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def _fingerprint_page(clean_text: str) -> str:
    clipped = re.sub(r"\s+", " ", clean_text).strip()[:12000]
    return _stable_hash(clipped)


def _candidate_hash(candidate: dict[str, Any]) -> str:
    payload = json.dumps(candidate, sort_keys=True, ensure_ascii=True)
    return _stable_hash(payload)


def _speaker_record_key(full_name: str, session_title: str | None, role: str | None) -> str:
    parts = [full_name.strip().lower(), (session_title or "").strip().lower(), (role or "").strip().lower()]
    return _stable_hash("::".join(parts))


def _looks_like_person_name(value: str) -> bool:
    cleaned = re.sub(r"[^A-Za-z\s\-']", " ", value or "")
    words = [word for word in cleaned.split() if word]
    if len(words) < 2:
        return False
    if len(words) > 5:
        return False
    if any(word.lower() in {"program", "session", "agenda", "conference", "guidelines", "standard", "daylight"} for word in words):
        return False
    return True


def _is_non_person_record(full_name: str, evidence: str | None) -> bool:
    blob = f"{full_name} {evidence or ''}".lower()
    if any(phrase in blob for phrase in NON_PERSON_PHRASES):
        return True
    return not _looks_like_person_name(full_name)


def _normalize_designation_token(token: str) -> str | None:
    canonical = DESIGNATION_MAP.get(token.upper().replace(" ", ""))
    return canonical


def _clean_name_and_designation(
    full_name: str,
    designation: str | None,
    *,
    role: str | None,
    evidence: str | None,
) -> tuple[str, str | None, list[str], bool]:
    canonical = canonicalize_person_name(
        full_name=full_name,
        designation=designation,
        role=role,
        evidence=evidence,
    )
    if not canonical.is_valid:
        return "", canonical.designation, canonical.aliases, False
    return canonical.full_name, canonical.designation, canonical.aliases, True


def _refresh_metrics_and_progress(
    run_debug: dict[str, Any],
    *,
    queue_estimate: int,
    no_progress_streak: int,
    last_stage: str,
) -> None:
    metrics = run_debug["metrics"]
    counters = run_debug["counters"]
    metrics["appearances_linked"] = counters["linked_appearances"]
    metrics["unresolved_attributions"] = counters["unresolved_attributions"]
    metrics["llm_calls"] = counters["llm_attempts"]
    metrics["llm_failures"] = counters["llm_failures"]
    metrics["llm_calls_saved"] = counters["llm_calls_saved"]
    metrics["llm_batches_started"] = counters["llm_batches_started"]
    metrics["llm_batches_completed"] = counters["llm_batches_completed"]
    metrics["llm_batches_timed_out"] = counters["llm_batches_timed_out"]
    metrics["stalls_recovered"] = counters["stalls_recovered"]
    metrics["stalls_terminal"] = counters["stalls_terminal"]
    metrics["pathfinder_llm_calls"] = counters["pathfinder_llm_attempts"]
    metrics["pathfinder_llm_failures"] = counters["pathfinder_llm_failures"]
    metrics["attribution_resolved_count"] = counters["attribution_resolved_count"]
    metrics["attribution_reconcile_resolved_count"] = counters["attribution_reconcile_resolved_count"]
    metrics["attribution_final_unresolved_count"] = counters["attribution_final_unresolved_count"]

    progress = run_debug["progress_state"]
    progress["queue_estimate"] = max(0, queue_estimate)
    progress["no_progress_streak"] = max(0, no_progress_streak)
    progress["last_stage"] = last_stage
    progress["last_update_at"] = datetime.now(timezone.utc).isoformat()


def _emit_progress_heartbeat(
    db: Session,
    run_id: str,
    run_debug: dict[str, Any],
    *,
    queue_estimate: int,
    no_progress_streak: int,
    last_stage: str,
) -> None:
    _refresh_metrics_and_progress(
        run_debug,
        queue_estimate=queue_estimate,
        no_progress_streak=no_progress_streak,
        last_stage=last_stage,
    )
    _event(
        db,
        run_id,
        "progress_heartbeat",
        "Run progress heartbeat",
        data={
            "metrics": run_debug["metrics"],
            "progress_state": run_debug["progress_state"],
        },
    )


def _infer_seed_targets(
    *,
    home_url: str,
    page_title: str,
    year_hints: list[int],
    conference_name_override: str | None = None,
) -> list[dict[str, Any]]:
    segment = page_title.strip()
    if segment:
        segment = re.split(r"[|:\-–—]", segment)[0].strip()
    if conference_name_override and conference_name_override.strip():
        conference_name = _normalize_conference_name(conference_name_override)
    else:
        conference_name = _normalize_conference_name(segment) if segment else _title_from_domain(home_url)
    if not conference_name or len(conference_name) < 3:
        conference_name = _title_from_domain(home_url)

    years = [int(y) for y in year_hints if 1990 <= int(y) <= 2100]
    unique_years = sorted(set(years), reverse=True)
    if not unique_years:
        return []

    current_year = datetime.now(timezone.utc).year
    preferred = [year for year in unique_years if current_year - 1 <= year <= current_year + 2]
    selected_desc = preferred[:3] if preferred else unique_years[:3]
    years = sorted(selected_desc)

    return [
        {"conference_name": conference_name, "year": year}
        for year in years
    ]


def _merge_llm_debug(counters: dict[str, int], debug: Any) -> None:
    attempts = int(getattr(debug, "llm_attempts", 0) or 0)
    failures = int(getattr(debug, "llm_failures", 0) or 0)
    counters["llm_attempts"] += attempts
    counters["llm_failures"] += failures
    counters["llm_batches_started"] += int(getattr(debug, "llm_batches_started", 0) or 0)
    counters["llm_batches_completed"] += int(getattr(debug, "llm_batches_completed", 0) or 0)
    counters["llm_batches_timed_out"] += int(getattr(debug, "llm_batches_timed_out", 0) or 0)
    if bool(getattr(debug, "used_fallback", False)):
        counters["heuristic_fallbacks"] += 1


def _merge_pathfinder_debug(counters: dict[str, int], debug: Any) -> None:
    counters["pathfinder_llm_attempts"] += int(getattr(debug, "llm_attempts", 0) or 0)
    counters["pathfinder_llm_failures"] += int(getattr(debug, "llm_failures", 0) or 0)


def _artifact_target_for_page(
    *,
    known_targets: list[dict[str, Any]],
    url: str,
) -> tuple[str, int] | None:
    if known_targets:
        url_years = _extract_years(url)
        if url_years:
            preferred_year = max(url_years)
            for target in known_targets:
                if int(target["year"]) == preferred_year:
                    return str(target["conference_name"]), int(target["year"])
        if len(known_targets) == 1:
            only = known_targets[0]
            return str(only["conference_name"]), int(only["year"])
    return None


def _conference_year_hints_for_page(
    *,
    known_targets: list[dict[str, Any]],
    default_conference_name: str | None,
    page_year_hints: list[int],
) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for item in known_targets:
        conference_name = str(item.get("conference_name") or "").strip()
        year = int(item.get("year") or 0)
        if not conference_name or not (1990 <= year <= 2100):
            continue
        key = (conference_name.lower(), year)
        if key in seen:
            continue
        seen.add(key)
        hints.append({"conference_name": conference_name, "year": year})

    if default_conference_name:
        conference_name = _normalize_conference_name(default_conference_name)
        for year in sorted({int(y) for y in page_year_hints if 1990 <= int(y) <= 2100}):
            key = (conference_name.lower(), year)
            if key in seen:
                continue
            seen.add(key)
            hints.append({"conference_name": conference_name, "year": year})

    return hints[:8]


def _ensure_conference_year(
    db: Session,
    conference_name: str,
    year: int,
    *,
    organizer_name: str | None = None,
    event_series_name: str | None = None,
    name_confidence: float | None = None,
) -> ConferenceYear:
    normalized_name = _normalize_conference_name(conference_name)
    conference = db.execute(select(Conference).where(Conference.name == normalized_name)).scalar_one_or_none()
    if not conference:
        conference = Conference(
            name=normalized_name,
            canonical_name=normalized_name.lower(),
            organizer_name=(organizer_name or None),
            event_series_name=(event_series_name or None),
            name_confidence=name_confidence,
        )
        db.add(conference)
        db.flush()
    else:
        if organizer_name and not conference.organizer_name:
            conference.organizer_name = organizer_name
        if event_series_name and not conference.event_series_name:
            conference.event_series_name = event_series_name
        if name_confidence is not None:
            existing = conference.name_confidence
            if existing is None or float(name_confidence) > float(existing):
                conference.name_confidence = float(name_confidence)

    conference_year = db.execute(
        select(ConferenceYear).where(
            and_(
                ConferenceYear.conference_id == conference.id,
                ConferenceYear.year == year,
            )
        )
    ).scalar_one_or_none()
    if not conference_year:
        conference_year = ConferenceYear(
            conference_id=conference.id,
            year=year,
            status=ConferenceYearStatus.pending,
        )
        db.add(conference_year)
        db.flush()

    return conference_year


def _link_run_conference_year(db: Session, run_id: str, conference_year_id: int) -> None:
    existing = db.execute(
        select(RunConferenceYear).where(
            and_(
                RunConferenceYear.run_id == run_id,
                RunConferenceYear.conference_year_id == conference_year_id,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return

    db.add(RunConferenceYear(run_id=run_id, conference_year_id=conference_year_id))
    db.flush()


def _safe_fetch_status(value: str) -> FetchStatus:
    try:
        return FetchStatus(value)
    except ValueError:
        return FetchStatus.error


def _sanitize_text_for_db(value: str | None, *, max_len: int = 250000) -> str:
    text = str(value or "")
    if "\x00" in text:
        text = text.replace("\x00", "")
    return text[:max_len]


def _get_or_create_source(
    db: Session,
    source_cache: dict[tuple[int, str], Source],
    *,
    conference_year_id: int,
    url: str,
    category: SourceCategory,
    method: SourceMethod,
    fetch_status: str,
    http_status: int | None,
    content_type: str,
) -> Source:
    key = (conference_year_id, url)
    existing = source_cache.get(key)
    if existing:
        return existing

    source = Source(
        conference_year_id=conference_year_id,
        url=url,
        category=category,
        method=method,
        fetch_status=_safe_fetch_status(fetch_status),
        http_status=http_status,
        content_type=content_type,
        score=None,
    )
    db.add(source)
    db.flush()
    source_cache[key] = source
    return source


def _store_source_artifacts(
    db: Session,
    *,
    source_id: int,
    raw_text: str,
    pdf_text: str | None,
    network_payloads: list[dict[str, Any]],
    candidates_for_llm: list[dict[str, Any]],
    llm_records: list[dict[str, Any]],
) -> None:
    db.add(
        Extraction(
            source_id=source_id,
            artifact_type=ExtractionArtifactType.clean_text,
            data=_sanitize_text_for_db(raw_text),
        )
    )

    if pdf_text:
        db.add(
            Extraction(
                source_id=source_id,
                artifact_type=ExtractionArtifactType.pdf_text,
                data=_sanitize_text_for_db(pdf_text),
            )
        )

    if network_payloads:
        db.add(
            Extraction(
                source_id=source_id,
                artifact_type=ExtractionArtifactType.network_json_sample,
                data=_sanitize_text_for_db(serialize_network_payloads(network_payloads)),
            )
        )

    if candidates_for_llm:
        db.add(
            Extraction(
                source_id=source_id,
                artifact_type=ExtractionArtifactType.candidate_blocks,
                data=_sanitize_text_for_db(json.dumps(candidates_for_llm, ensure_ascii=True)),
            )
        )

    if llm_records:
        db.add(
            Extraction(
                source_id=source_id,
                artifact_type=ExtractionArtifactType.llm_output,
                data=_sanitize_text_for_db(json.dumps({"records": llm_records}, ensure_ascii=True)),
            )
        )


def _build_llm_candidates(
    *,
    settings,
    source_url: str,
    embedded_candidates: list[dict[str, Any]],
    network_candidates: list[dict[str, Any]],
    dom_blocks: list[str],
    pdf_blocks: list[str],
    session_speaker_pairs: list[dict[str, Any]],
    page_context_text: str = "",
) -> list[dict[str, Any]]:
    def _clip(value: Any, limit: int) -> str:
        text = " ".join(str(value or "").split())
        return text[:limit]

    out: list[dict[str, Any]] = []
    max_dom_candidates = max(1, int(settings.llm_dom_candidate_cap))
    max_pdf_candidates = max(1, int(settings.llm_pdf_candidate_cap))
    page_segment_chars = max(600, int(settings.llm_page_segment_chars))
    page_segment_overlap = max(0, int(settings.llm_page_segment_overlap))
    if page_segment_overlap >= page_segment_chars:
        page_segment_overlap = min(250, page_segment_chars // 3)
    max_page_segments = max(1, int(settings.llm_page_max_segments))
    page_context_max_chars = max(5000, int(settings.llm_page_context_max_chars))
    candidate_cap = max(100, int(settings.llm_candidate_cap))
    generated_page_segments = 0

    for row in embedded_candidates[:220]:
        out.append(
            {
                "candidate_type": "embedded_json",
                "source_url": source_url,
                "raw": _clip(json.dumps(row, ensure_ascii=True), 900),
                "text": _clip(json.dumps(row.get("data", {}), ensure_ascii=True), 1100),
            }
        )

    for row in network_candidates[:250]:
        out.append(
            {
                "candidate_type": "network_json",
                "source_url": source_url,
                "raw": _clip(json.dumps(row, ensure_ascii=True), 900),
                "text": _clip(json.dumps(row.get("data", {}), ensure_ascii=True), 1100),
            }
        )

    for row in session_speaker_pairs[:260]:
        session_title = _clip(row.get("session_title"), 260)
        speaker_name = _clip(row.get("speaker_name_raw"), 140)
        context_snippet = _clip(row.get("context_snippet"), 500)
        out.append(
            {
                "candidate_type": "session_speaker_pair",
                "source_url": source_url,
                "session_title": session_title,
                "speaker_name_raw": speaker_name,
                "context_snippet": context_snippet,
                "text": _clip(
                    row.get("text")
                    or f"Session: {session_title}. Speaker: {speaker_name}. Context: {context_snippet}",
                    1200,
                ),
            }
        )

    compact_page = _clip(page_context_text, page_context_max_chars)
    if compact_page:
        start = 0
        segment_index = 0
        while start < len(compact_page) and segment_index < max_page_segments:
            end = min(len(compact_page), start + page_segment_chars)
            segment = compact_page[start:end]
            if segment.strip():
                out.append(
                    {
                        "candidate_type": "page_segment",
                        "source_url": source_url,
                        "segment_index": segment_index,
                        "text": segment,
                    }
                )
                segment_index += 1
                generated_page_segments = segment_index
            if end >= len(compact_page):
                break
            start = max(0, end - page_segment_overlap)

    if generated_page_segments >= 6:
        max_dom_candidates = min(max_dom_candidates, 8)
    elif generated_page_segments >= 3:
        max_dom_candidates = min(max_dom_candidates, 12)

    def _dom_block_priority(block: str) -> int:
        lowered = block.lower()
        if "[block person_span]" in lowered:
            return 3
        if "[block session_span]" in lowered:
            return 2
        if "[block dom_card]" in lowered:
            return 1
        return 0

    prioritized_dom = sorted(dom_blocks, key=_dom_block_priority, reverse=True)[:max_dom_candidates]
    for block in prioritized_dom:
        out.append(
            {
                "candidate_type": "dom_block",
                "source_url": source_url,
                "text": _clip(block, 1100),
            }
        )

    for block in pdf_blocks[:max_pdf_candidates]:
        out.append(
            {
                "candidate_type": "pdf_block",
                "source_url": source_url,
                "text": _clip(block, 1100),
            }
        )

    dedup: dict[str, dict[str, Any]] = {}
    for row in out:
        key = (
            f"{row.get('candidate_type')}::{row.get('segment_index', '')}::{row.get('text')[:300]}"
        )
        dedup[key] = row
    return list(dedup.values())[:candidate_cap]


async def _maybe_enrich_physician(
    db: Session,
    settings,
    *,
    run_id: str,
    physician: Any,
    full_name: str,
    conference_name: str,
    year: int,
    session_title: str | None,
    designation_hint: str | None,
    affiliation_hint: str | None,
    location_hint: str | None,
    run_counters: dict[str, int],
    attempted_physician_ids: set[int],
    source_url: str | None = None,
    evidence_span: str | None = None,
) -> None:
    if not getattr(settings, "physician_enrichment_enabled", True):
        return
    physician_id = int(getattr(physician, "id"))
    if physician_id in attempted_physician_ids:
        return
    attempted_physician_ids.add(physician_id)

    has_profile = bool(
        (physician.primary_specialty and physician.primary_specialty.strip())
        and (physician.bio_short and physician.bio_short.strip())
        and (physician.primary_profile_url and physician.primary_profile_url.strip())
    )
    if has_profile:
        return

    local_evidence: list[dict[str, str]] = []
    if source_url and (session_title or evidence_span):
        local_evidence.append(
            {
                "url": source_url,
                "title": session_title or "Session context",
                "snippet": (evidence_span or session_title or "")[:1000],
            }
        )

    enrichment_model = select_llm_model(settings, stage="enrichment")
    enrichment_timeout = select_llm_timeout(
        settings,
        stage="enrichment",
        default_timeout_seconds=max(float(getattr(settings, "llm_request_timeout_seconds", 150) or 150), 10.0),
    )
    _event(
        db,
        run_id,
        "physician_enrichment_start",
        f"Starting web enrichment for {full_name}",
        data={
            "physician_id": physician_id,
            "selected_model": enrichment_model,
            "selected_timeout_seconds": enrichment_timeout,
            "conference_name": conference_name,
            "year": year,
        },
    )

    result = await enrich_physician_profile(
        settings,
        full_name=full_name,
        conference_name=conference_name,
        year=year,
        session_title=session_title,
        designation_hint=designation_hint,
        affiliation_hint=affiliation_hint,
        location_hint=location_hint,
        local_evidence=local_evidence,
    )
    run_counters["llm_attempts"] += int(result.debug.llm_attempts or 0)
    run_counters["llm_failures"] += int(result.debug.llm_failures or 0)
    if result.debug.used_fallback:
        run_counters["heuristic_fallbacks"] += 1

    confidence_floor = float(getattr(settings, "physician_enrichment_min_confidence", 0.7) or 0.7)
    if result.ambiguous or result.confidence < confidence_floor:
        _event(
            db,
            run_id,
            "physician_enrichment_skipped",
            f"Skipped web enrichment for {full_name} due to ambiguous/low-confidence profile",
            level="warning",
            data={
                "physician_id": physician_id,
                "confidence": result.confidence,
                "ambiguous": result.ambiguous,
                "reason": result.reason,
                "search_results": result.debug.search_results,
                "selected_model": result.debug.selected_model,
                "selected_timeout_seconds": result.debug.selected_timeout_seconds,
            },
        )
        return

    changed = False
    if result.full_name_normalized and normalize_text(result.full_name_normalized) != normalize_text(physician.full_name):
        physician.full_name = result.full_name_normalized[:255]
        changed = True
    if result.designation_normalized and not physician.primary_designation:
        physician.primary_designation = result.designation_normalized[:255]
        changed = True
    if result.specialty and not physician.primary_specialty:
        physician.primary_specialty = result.specialty[:255]
        changed = True
    if result.education and not physician.primary_education:
        physician.primary_education = result.education[:255]
        changed = True
    if result.affiliation and not physician.primary_affiliation:
        physician.primary_affiliation = result.affiliation[:255]
        changed = True
    if result.location and not physician.primary_location:
        physician.primary_location = result.location[:255]
        changed = True
    if result.profile_url and not physician.primary_profile_url:
        physician.primary_profile_url = result.profile_url[:1000]
        changed = True
    if result.bio_short and not physician.bio_short:
        physician.bio_short = result.bio_short[:500]
        changed = True
    if result.bio_source_url and not physician.bio_source_url:
        physician.bio_source_url = result.bio_source_url[:1000]
        changed = True
    if result.confidence:
        physician.enrichment_confidence = result.confidence
        physician.enrichment_updated_at = datetime.now(timezone.utc)
        changed = True

    _event(
        db,
        run_id,
        "physician_enriched" if changed else "physician_enrichment_skipped",
        (
            f"Enriched physician profile for {full_name}"
            if changed
            else f"Web enrichment produced no new profile fields for {full_name}"
        ),
        data={
            "physician_id": physician_id,
            "confidence": result.confidence,
            "specialty": result.specialty,
            "education": result.education,
            "affiliation": result.affiliation,
            "location": result.location,
            "profile_url": result.profile_url,
            "bio_short": result.bio_short,
            "identity_signature": result.identity_signature,
            "search_results": result.debug.search_results,
            "selected_model": result.debug.selected_model,
            "selected_timeout_seconds": result.debug.selected_timeout_seconds,
        },
    )


def _valid_target(target: AttributionTarget) -> bool:
    return bool(str(target.conference_name).strip()) and 1990 <= int(target.year) <= 2100


def _build_relaxed_attribution_targets(
    *,
    record: ExtractedSpeaker,
    source_url: str,
    page_url: str,
    page_title: str,
    run_conference_name: str | None,
    default_conference_name: str | None,
    page_year_hints: list[int],
    known_targets: list[dict[str, Any]],
) -> list[AttributionTarget]:
    conference_name = (
        (run_conference_name or "").strip()
        or (default_conference_name or "").strip()
    )
    if not conference_name and known_targets:
        candidate_names = [
            str(item.get("conference_name") or "").strip()
            for item in known_targets
            if str(item.get("conference_name") or "").strip()
        ]
        if candidate_names:
            conference_name = candidate_names[-1]

    if not conference_name:
        return []

    # Prefer URL/title-local year hints over broad page-level year hints (archives).
    url_years = _extract_years(f"{page_url} {source_url}")
    title_years = _extract_years(page_title or "")
    record_years = _extract_years(f"{record.session_title or ''} {record.evidence_span or ''}")
    page_years = sorted({int(y) for y in page_year_hints if 1990 <= int(y) <= 2100})

    selected_year: int | None = None
    if url_years:
        selected_year = url_years[-1]
    elif title_years:
        selected_year = title_years[-1]
    elif len(record_years) == 1:
        selected_year = record_years[0]
    elif len(page_years) == 1:
        selected_year = page_years[0]
    elif page_years:
        selected_year = page_years[-1]
    elif known_targets:
        candidate_years = [
            int(item.get("year"))
            for item in known_targets
            if isinstance(item.get("year"), int) and 1990 <= int(item.get("year")) <= 2100
        ]
        if candidate_years:
            selected_year = max(candidate_years)

    if selected_year is None or selected_year < 1990 or selected_year > 2100:
        return []

    confidence_value = float(record.confidence or 0.55)
    return [
        AttributionTarget(
            conference_name=conference_name,
            year=int(selected_year),
            confidence=max(0.35, min(0.8, confidence_value)),
            reason="relaxed_context_fallback",
        )
    ]


def _should_link_extracted_record(
    *,
    record: ExtractedSpeaker,
    clean_name: str,
    designation_out: str | None,
) -> bool:
    if record.is_physician_candidate:
        return True

    if is_physician_like(
        clean_name,
        designation_out,
        record.affiliation,
        record.role,
        session_title=record.session_title,
        evidence_span=record.evidence_span,
    ):
        return True

    role_text = (record.role or "").lower()
    speakerish_tokens = {
        "speaker",
        "panelist",
        "moderator",
        "presenter",
        "chair",
        "cochair",
        "co-chair",
        "expert",
        "faculty",
    }
    if any(token in role_text for token in speakerish_tokens):
        return True

    if (record.session_title or "").strip() and float(record.confidence or 0.0) >= 0.6:
        return True

    return False


def _effective_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _is_pdf_like_url(url: str) -> bool:
    lowered = url.lower()
    return lowered.endswith(".pdf") or "pdf" in lowered or "brochure" in lowered


def _source_method_for_page(page: CrawlPageResult) -> SourceMethod:
    if page.fetch_method in {SourceMethod.http_static, SourceMethod.playwright_dom, SourceMethod.playwright_network, SourceMethod.pdf_text}:
        return page.fetch_method
    if page.content_type == "pdf":
        return SourceMethod.pdf_text
    return SourceMethod.http_static


def _novelty_score_for_url(
    *,
    canonical_url: str,
    template_key: str,
    seen_url_states: set[str],
    template_clusters: dict[str, set[str]],
) -> float:
    if canonical_url in seen_url_states:
        return 0.0
    cluster_size = len(template_clusters.get(template_key, set()))
    if cluster_size <= 1:
        return 0.95
    if cluster_size <= 4:
        return 0.75
    return 0.55


def _push_frontier(
    frontier: list[tuple[float, int, FrontierNode]],
    node: FrontierNode,
) -> None:
    heapq.heappush(frontier, (-float(node.priority), int(node.sequence), node))


def _pop_frontier(frontier: list[tuple[float, int, FrontierNode]]) -> FrontierNode:
    _, _, node = heapq.heappop(frontier)
    return node


def _links_for_navigation(page: CrawlPageResult) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in page.internal_links:
        text = (row.text or "").strip()
        if not text:
            path = urlparse(row.url).path.strip("/").split("/")
            text = (path[-1] if path else "")[:120]
        out.append(
            {
                "url": row.url,
                "text": text,
                "context": (row.context or "link")[:120],
            }
        )
    return out


def _pdf_links_for_navigation(page: CrawlPageResult) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for url in page.pdf_links:
        out.append({"url": url, "text": "pdf", "context": "pdf_link"})
    return out


def _nav_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    for raw in re.findall(r"[a-z0-9]+", (value or "").lower()):
        if len(raw) < 3:
            continue
        if raw.isdigit():
            continue
        if raw in NAV_FOCUS_IGNORE_TOKENS:
            continue
        tokens.add(raw)
        if raw.endswith("s") and len(raw) > 4:
            tokens.add(raw[:-1])
    return tokens


def _conference_focus_tokens(seed_url: str, conference_name: str | None) -> set[str]:
    seed_parts = urlparse(seed_url)
    base = f"{seed_parts.path} {conference_name or ''}"
    tokens = _nav_tokens(base)
    if not tokens:
        tokens = _nav_tokens(seed_parts.path or "")
    return tokens


def _path_prefix(url: str, depth: int = 2) -> str:
    segments = [segment for segment in urlparse(url).path.lower().split("/") if segment]
    if not segments:
        return "/"
    return "/" + "/".join(segments[:depth])


def _primary_section(url: str) -> str:
    segments = [segment for segment in urlparse(url).path.lower().split("/") if segment]
    if not segments:
        return ""
    return segments[0]


def _same_primary_section(seed_url: str, candidate_url: str) -> bool:
    seed_section = _primary_section(seed_url)
    candidate_section = _primary_section(candidate_url)
    return bool(seed_section and candidate_section and seed_section == candidate_section)


def _year_priority_delta(url: str, seed_year: int | None) -> float:
    if seed_year is None:
        return 0.0
    years = _extract_years(url)
    if not years:
        return 0.0
    year = min(years, key=lambda value: abs(value - seed_year))
    if year <= seed_year:
        distance = max(0, seed_year - year)
        if distance <= 1:
            return 0.12
        if distance <= 5:
            return 0.10
        if distance <= 10:
            return 0.08
        return 0.06
    return -0.04


def _is_nav_link_relevant(
    *,
    seed_url: str,
    page_url: str,
    candidate_url: str,
    text: str,
    context: str,
    focus_tokens: set[str],
) -> bool:
    if _effective_domain(candidate_url) != _effective_domain(seed_url):
        return False

    candidate_blob = f"{urlparse(candidate_url).path} {text} {context}"
    candidate_tokens = _nav_tokens(candidate_blob)
    text_clean = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    context_clean = re.sub(r"\s+", " ", str(context or "")).strip().lower()
    generic_anchor = text_clean in NAV_GENERIC_ANCHOR_PHRASES or (
        text_clean in {"archive", "archives"} and context_clean in {"menu", "link", "html_anchor"}
    )
    signal_hit = bool(candidate_tokens.intersection(NAV_SIGNAL_TOKENS))
    has_excluded_utility = bool(candidate_tokens.intersection(NAV_EXCLUDE_TOKENS))
    if has_excluded_utility and not signal_hit:
        return False
    if generic_anchor and not candidate_tokens.intersection(focus_tokens):
        return False
    if candidate_tokens.intersection(focus_tokens):
        return True

    candidate_years = _extract_years(f"{candidate_url} {text}")
    if (
        candidate_years
        and _same_primary_section(seed_url, candidate_url)
        and _same_primary_section(seed_url, page_url)
    ):
        return True

    return signal_hit


def _filter_navigation_links(
    *,
    seed_url: str,
    page_url: str,
    nav_links: list[dict[str, str]],
    focus_tokens: set[str],
    allow_pdf: bool,
) -> tuple[list[dict[str, str]], int]:
    kept: list[dict[str, str]] = []
    dropped = 0
    seen: set[str] = set()
    for row in nav_links:
        candidate_url = str(row.get("url", "")).strip()
        if not candidate_url:
            dropped += 1
            continue
        candidate_canonical = _canonical_url(candidate_url)
        if candidate_canonical in seen:
            dropped += 1
            continue
        seen.add(candidate_canonical)
        if not allow_pdf and _is_pdf_like_url(candidate_url):
            dropped += 1
            continue
        text = str(row.get("text", ""))
        context = str(row.get("context", ""))
        if not _is_nav_link_relevant(
            seed_url=seed_url,
            page_url=page_url,
            candidate_url=candidate_url,
            text=text,
            context=context,
            focus_tokens=focus_tokens,
        ):
            dropped += 1
            continue
        kept.append(row)
    return kept, dropped


def _filter_navigation_candidates(
    *,
    seed_url: str,
    page_url: str,
    candidates: list[NavigationCandidate],
    focus_tokens: set[str],
    allow_pdf: bool,
) -> tuple[list[NavigationCandidate], int]:
    kept: list[NavigationCandidate] = []
    dropped = 0
    seen: set[str] = set()
    relevance_tokens = set(focus_tokens).union(NAV_SIGNAL_TOKENS)

    for candidate in candidates:
        candidate_url = str(candidate.url or "").strip()
        if not candidate_url:
            dropped += 1
            continue
        candidate_canonical = _canonical_url(candidate_url)
        if candidate_canonical in seen:
            dropped += 1
            continue
        seen.add(candidate_canonical)

        if not allow_pdf and _is_pdf_like_url(candidate_url):
            dropped += 1
            continue

        page_type = (candidate.page_type or "unknown").strip().lower()
        if page_type == "non_content":
            dropped += 1
            continue

        reason_text = f"{candidate.reason or ''} {candidate.branch_hint or ''}".strip()
        candidate_tokens = _nav_tokens(f"{candidate_url} {reason_text} {page_type}")
        if page_type == "unknown" and not candidate_tokens.intersection(relevance_tokens):
            dropped += 1
            continue

        if not _is_nav_link_relevant(
            seed_url=seed_url,
            page_url=page_url,
            candidate_url=candidate_url,
            text=reason_text,
            context=page_type,
            focus_tokens=focus_tokens,
        ):
            dropped += 1
            continue
        kept.append(candidate)

    return kept, dropped


def _delay_bounds(settings) -> tuple[float, float]:
    min_delay = max(float(settings.domain_min_delay_seconds), 0.0)
    max_delay = max(float(settings.domain_max_delay_seconds), min_delay)
    if settings.app_env.lower() == "test":
        return 0.0, 0.0
    return min_delay, max_delay


def _cooldown_bounds(settings) -> tuple[int, int]:
    min_cooldown = max(int(settings.domain_block_cooldown_min_seconds), 0)
    max_cooldown = max(int(settings.domain_block_cooldown_max_seconds), min_cooldown)
    if settings.app_env.lower() == "test":
        return 0, 0
    return min_cooldown, max_cooldown


async def _apply_domain_throttle(settings, state: DomainGuardState) -> None:
    min_delay, max_delay = _delay_bounds(settings)
    now = time.monotonic()

    while state.minute_window and now - state.minute_window[0] >= 60.0:
        state.minute_window.popleft()

    max_per_minute = max(int(settings.domain_max_pages_per_minute), 1)
    if len(state.minute_window) >= max_per_minute:
        wait_seconds = 60.0 - (now - state.minute_window[0]) + 0.05
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)

    if state.last_request_at is not None and max_delay > 0:
        wait_between = random.uniform(min_delay, max_delay)
        if wait_between > 0:
            await asyncio.sleep(wait_between)

    now = time.monotonic()
    state.minute_window.append(now)
    state.last_request_at = now
    state.request_total += 1


async def _resolve_targets_for_record(
    settings,
    *,
    record: ExtractedSpeaker,
    source_url: str,
    page_url: str,
    page_title: str,
    run_conference_name: str | None,
    page_text_hint: str,
    page_year_hints: list[int],
    known_targets: list[dict[str, Any]],
    default_conference_name: str | None,
    counters: dict[str, int],
) -> tuple[list[AttributionTarget], str | None]:
    explicit_targets = [target for target in record.attribution_targets if _valid_target(target)]
    if explicit_targets:
        return explicit_targets, None

    year_hints = sorted(
        set(
            _extract_years(
                f"{record.session_title or ''} {record.evidence_span or ''} {source_url} {page_text_hint}"
            )
            + [int(value) for value in page_year_hints if 1990 <= int(value) <= 2100]
        )
    )

    resolve_kwargs = {
        "record": record.model_dump(),
        "source_context": {
            "source_url": source_url,
            "page_url": page_url,
            "page_title": page_title,
            "page_text_hint": page_text_hint,
            "run_conference_name": run_conference_name,
            "record_session_title": record.session_title,
            "record_evidence_span": record.evidence_span,
            "known_targets": known_targets,
            "notes": "page_year_hints may include archive years; prioritize schedule-local evidence",
        },
        "known_targets": known_targets,
        "default_conference_name": default_conference_name,
        "page_year_hints": year_hints,
    }
    try:
        attribution = await resolve_attribution(settings, **resolve_kwargs)
    except TypeError:
        # Backward compatibility for monkeypatched tests with the legacy signature.
        attribution = await resolve_attribution(
            settings,
            record=resolve_kwargs["record"],
            source_context=resolve_kwargs["source_context"],
            known_targets=known_targets,
        )
    _merge_llm_debug(counters, attribution.debug)
    if not attribution.targets:
        relaxed_targets = _build_relaxed_attribution_targets(
            record=record,
            source_url=source_url,
            page_url=page_url,
            page_title=page_title,
            run_conference_name=run_conference_name,
            default_conference_name=default_conference_name,
            page_year_hints=year_hints,
            known_targets=known_targets,
        )
        if relaxed_targets:
            return relaxed_targets, "relaxed_context_fallback"
        return [], attribution.unresolved_reason

    return [target for target in attribution.targets if _valid_target(target)], attribution.unresolved_reason


async def execute_run(run_id: str) -> None:
    db = SessionLocal()
    settings = get_settings()
    run_debug: dict[str, Any] = {
        "run_id": run_id,
        "home_url": None,
        "status": "pending",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "log_version": 4,
        "counters": _new_debug_counters(),
        "metrics": _new_run_metrics(),
        "progress_state": _new_progress_state(),
        "urls": [],
        "years": [],
        "errors": [],
    }

    source_cache: dict[tuple[int, str], Source] = {}
    artifact_written_source_ids: set[int] = set()
    session_manager: PlaywrightDomainSessionManager | None = None

    try:
        run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
        if not run:
            run_debug["status"] = "not_found"
            return

        if run.status in {RunStatus.complete, RunStatus.partial, RunStatus.error}:
            run_debug["status"] = run.status.value
            return

        run_debug["home_url"] = run.home_url
        if run.conference_name:
            run_debug["conference_name"] = run.conference_name
        if run_manager.is_cancel_requested(run_id):
            run.status = RunStatus.partial
            run.error_message = "Cancelled by user"
            run.finished_at = datetime.now(timezone.utc)
            _event(db, run.id, "run_cancelled", "Run cancelled before start", level="warning")
            db.commit()
            run_debug["status"] = run.status.value
            return
        run.status = RunStatus.running
        run_conf_hint = _normalize_conference_name((run.conference_name or "").strip()) if run.conference_name else None
        _event(
            db,
            run.id,
            "run_start",
            f"Run started for seed URL {run.home_url}",
            data={"conference_name_hint": run_conf_hint} if run_conf_hint else None,
        )
        db.commit()

        preflight = await fetch_http(run.home_url, timeout_seconds=15.0)
        _event(
            db,
            run.id,
            "preflight",
            "Fetched seed URL",
            data={
                "status": preflight.http_status,
                "content_type": preflight.content_type,
                "blocked": preflight.blocked,
            },
            level="warning" if preflight.blocked else "info",
        )
        if preflight.blocked:
            run_debug["counters"]["blocked_pages"] += 1
        db.commit()

        year_hints = _extract_years(preflight.text)
        identity = await infer_conference_identity(
            settings,
            home_url=run.home_url,
            page_title=extract_page_title(preflight.text),
            html=preflight.text,
            top_headings=[],
            year_hints=sorted(set(year_hints)),
        )
        run_debug["counters"]["llm_attempts"] += int(identity.debug.llm_attempts or 0)
        run_debug["counters"]["llm_failures"] += int(identity.debug.llm_failures or 0)
        if identity.debug.used_fallback:
            run_debug["counters"]["heuristic_fallbacks"] += 1
        _event(
            db,
            run.id,
            "conference_identity_inferred",
            "Inferred conference identity from seed evidence",
            data={
                "organizer_name": identity.organizer_name,
                "event_series_name": identity.event_series_name,
                "display_name": identity.display_name,
                "canonical_name": identity.canonical_name,
                "confidence": identity.confidence,
                "used_fallback": identity.debug.used_fallback,
                "selected_model": identity.debug.selected_model,
                "selected_timeout_seconds": identity.debug.selected_timeout_seconds,
            },
        )
        db.commit()

        default_conference_name = _normalize_conference_name(
            run_conf_hint or identity.display_name or extract_page_title(preflight.text) or _title_from_domain(run.home_url)
        )
        conference_focus_tokens = _conference_focus_tokens(
            run.home_url,
            run_conf_hint or default_conference_name,
        )
        seed_year_hints = _extract_years(f"{run.home_url} {run_conf_hint or default_conference_name}")
        seed_year = max(seed_year_hints) if seed_year_hints else None
        pdf_enabled = bool(getattr(settings, "nav_pdf_enabled", False))
        strict_conference_focus = bool(getattr(settings, "nav_strict_conference_focus", True))
        known_targets: list[dict[str, Any]] = []
        max_depth = max(int(settings.nav_max_depth), 0)
        max_pages_per_domain = max(int(settings.nav_max_pages_per_domain), 1)
        max_pages_per_domain_hard = max(int(settings.nav_max_pages_per_domain_hard), max_pages_per_domain)
        budget_expand_step = max(1, int(settings.nav_budget_expand_step))
        budget_expand_window = max(1, int(settings.nav_budget_expand_window))
        budget_expand_candidate_threshold = max(1, int(settings.nav_budget_expand_candidate_threshold))
        budget_expand_appearance_threshold = max(1, int(settings.nav_budget_expand_appearance_threshold))
        global_page_cap = max(
            1,
            int(getattr(settings, "max_total_pages_per_run", settings.nav_max_total_pages) or settings.nav_max_total_pages),
        )
        max_run_duration_seconds = max(
            60,
            int(getattr(settings, "max_run_duration_minutes", 240) or 240) * 60,
        )
        novelty_window_size = max(1, int(getattr(settings, "novelty_window_size", 10) or 10))
        novelty_zero_window_limit = max(1, int(getattr(settings, "novelty_zero_window_limit", 3) or 3))
        year_metrics: dict[int, dict[str, Any]] = {}
        _event(
            db,
            run.id,
            "classify_seed",
            "Inferred seed conference identity; year attribution deferred to page evidence",
            data={
                "conference_name_hint": default_conference_name,
                "user_conference_name": run_conf_hint,
                "known_targets": known_targets,
            },
        )
        db.commit()

        domain_guards: dict[str, DomainGuardState] = {}
        domain_page_counts: dict[str, int] = {}
        domain_page_budgets: dict[str, int] = {}
        domain_recent_yields: dict[str, deque[tuple[int, int, int, int]]] = {}
        domain_budget_expanded_at: dict[str, int] = {}
        block_threshold = max(int(settings.domain_block_threshold), 1)
        no_progress_limit = max(int(settings.nav_no_progress_streak_limit), 1)
        zero_progress_window = max(1, int(settings.nav_consecutive_zero_window))
        watchdog_stall_seconds = max(int(getattr(settings, "watchdog_stall_seconds", 240) or 240), 1)
        max_stalls_per_run = max(int(getattr(settings, "watchdog_max_stalls_per_run", 2) or 2), 1)
        llm_request_timeout_default = max(float(getattr(settings, "llm_request_timeout_seconds", 120) or 120), 10.0)
        extraction_llm_model = select_llm_model(settings, stage="extraction")
        extraction_llm_timeout = select_llm_timeout(
            settings,
            stage="extraction",
            default_timeout_seconds=llm_request_timeout_default,
        )
        attribution_llm_model = select_llm_model(settings, stage="attribution")
        attribution_llm_timeout = select_llm_timeout(
            settings,
            stage="attribution",
            default_timeout_seconds=45.0,
        )
        llm_batch_timeout_buffer_seconds = max(int(getattr(settings, "llm_batch_timeout_buffer_seconds", 30) or 30), 0)
        reasoning_model_name = (getattr(settings, "deepseek_reasoning_model", "") or "").strip()
        extraction_uses_reasoning = bool(
            getattr(settings, "deepseek_reasoning_enabled", False)
            and reasoning_model_name
            and extraction_llm_model == reasoning_model_name
        )
        normalize_uses_default_impl = bool(
            getattr(normalize_candidates, "__name__", "") == "normalize_candidates"
            and getattr(normalize_candidates, "__module__", "").endswith("app.services.extract_llm")
        )
        normalize_batch_watchdog_seconds = (
            max(
                watchdog_stall_seconds,
                int(extraction_llm_timeout) + llm_batch_timeout_buffer_seconds,
            )
            if extraction_uses_reasoning and normalize_uses_default_impl
            else watchdog_stall_seconds
        )
        markdown_first_enabled = bool(getattr(settings, "markdown_first_enabled", True))
        link_memory_enabled = bool(getattr(settings, "link_memory_enabled", True))
        link_memory_decay_days = max(1, int(getattr(settings, "link_memory_decay_days", 30) or 30))
        link_memory_min_visits = max(1, int(getattr(settings, "link_memory_min_visits", 2) or 2))
        session_manager = PlaywrightDomainSessionManager()
        reported_bootstrap_domains: set[str] = set()
        frontier: list[tuple[float, int, FrontierNode]] = []
        frontier_seq = 0
        template_clusters: dict[str, set[str]] = {}
        branch_stats: dict[str, BranchStats] = {}
        queued_url_states: set[str] = {_canonical_url(run.home_url)}
        modal_breaker_attempted_states: set[str] = set()
        attempted_physician_enrichment_ids: set[int] = set()
        novelty_window: deque[tuple[int, int, int, int]] = deque(maxlen=novelty_window_size)
        novelty_zero_windows = 0
        started_monotonic = time.monotonic()
        seed_template = template_key_for_url(run.home_url)
        template_clusters.setdefault(seed_template, set()).add(_canonical_url(run.home_url))
        seed_branch = branch_id_for_url(run.home_url, hint=seed_template)
        seed_node = FrontierNode(
            priority=0.95,
            sequence=frontier_seq,
            url=run.home_url,
            canonical_url=_canonical_url(run.home_url),
            depth=0,
            branch_id=seed_branch,
            llm_priority=0.95,
            estimated_yield=0.7,
            novelty_score=0.95,
            enqueued_at=time.monotonic(),
        )
        _push_frontier(frontier, seed_node)
        visited_urls: set[str] = set()
        seen_url_states: set[str] = set()
        seen_page_fingerprints: set[str] = set()
        seen_candidate_hashes: set[str] = set()
        seen_normalized_speaker_keys: set[str] = set()
        linked_physician_ids: set[int] = set()
        normalize_cache: dict[str, list[ExtractedSpeaker]] = {}
        no_progress_streak = 0
        stall_count = 0
        terminate_due_stall = False
        cancelled_by_user = False
        prompt_version = "llm_normalize_v2"
        run_debug["metrics"]["pages_enqueued"] = len(frontier)
        run_debug["metrics"]["frontier_size"] = len(frontier)
        run_debug["metrics"]["adaptive_budget_current"] = max_pages_per_domain
        run_debug["metrics"]["adaptive_budget_max"] = max_pages_per_domain_hard
        run_debug["metrics"]["template_clusters_discovered"] = len(template_clusters)
        _emit_progress_heartbeat(
            db,
            run.id,
            run_debug,
            queue_estimate=len(frontier),
            no_progress_streak=no_progress_streak,
            last_stage="run_start",
        )
        db.commit()

        while frontier:
            if run_manager.is_cancel_requested(run_id):
                cancelled_by_user = True
                _event(
                    db,
                    run.id,
                    "run_cancelled",
                    "Run cancellation requested by user; stopping crawl loop",
                    level="warning",
                    data={"queue_remaining": len(frontier)},
                )
                db.commit()
                break
            current = _pop_frontier(frontier)
            url = current.url
            depth = current.depth
            current_branch_id = current.branch_id
            canonical_url = _canonical_url(url)
            queued_url_states.discard(canonical_url)
            if depth > max_depth:
                continue
            if canonical_url in seen_url_states:
                run_debug["metrics"]["repeated_state_skips"] += 1
                _event(
                    db,
                    run.id,
                    "state_repeat_skip",
                    "Skipping canonical URL state already processed",
                    data={"url": url, "canonical_url": canonical_url, "depth": depth},
                )
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="state_repeat_skip",
                )
                db.commit()
                continue
            if url in visited_urls:
                continue
            seen_url_states.add(canonical_url)
            run_debug["metrics"]["unique_url_states"] = len(seen_url_states)

            domain = _effective_domain(url)
            guard = domain_guards.setdefault(domain, DomainGuardState())
            domain_page_budgets.setdefault(domain, max_pages_per_domain)
            domain_recent_yields.setdefault(domain, deque(maxlen=budget_expand_window))
            if guard.stopped:
                _event(
                    db,
                    run.id,
                    "fetch_route",
                    "Skipping URL because domain is in blocked cooldown-stop state",
                    level="warning",
                    data={"url": url, "domain": domain},
                )
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="fetch_route",
                )
                db.commit()
                continue

            if run_debug["metrics"]["pages_visited"] >= global_page_cap:
                _event(
                    db,
                    run.id,
                    "no_progress_stop",
                    "Global crawl page cap reached",
                    level="warning",
                    data={"cap": global_page_cap},
                )
                db.commit()
                break

            elapsed_seconds = int(time.monotonic() - started_monotonic)
            if elapsed_seconds >= max_run_duration_seconds:
                _event(
                    db,
                    run.id,
                    "no_progress_stop",
                    "Run duration cap reached",
                    level="warning",
                    data={
                        "elapsed_seconds": elapsed_seconds,
                        "max_run_duration_seconds": max_run_duration_seconds,
                    },
                )
                db.commit()
                break

            current_domain_budget = int(domain_page_budgets.get(domain, max_pages_per_domain))
            recent_window = domain_recent_yields.get(domain, deque())
            if (
                len(recent_window) >= budget_expand_window
                and current_domain_budget < max_pages_per_domain_hard
            ):
                candidate_sum = sum(item[0] for item in recent_window)
                appearance_sum = sum(item[3] for item in recent_window)
                if candidate_sum >= budget_expand_candidate_threshold or appearance_sum >= budget_expand_appearance_threshold:
                    expanded_budget = min(max_pages_per_domain_hard, current_domain_budget + budget_expand_step)
                    if expanded_budget > current_domain_budget:
                        domain_page_budgets[domain] = expanded_budget
                        domain_budget_expanded_at[domain] = run_debug["metrics"]["pages_visited"]
                        run_debug["metrics"]["adaptive_budget_current"] = max(domain_page_budgets.values(), default=max_pages_per_domain)
                        _event(
                            db,
                            run.id,
                            "budget_expand",
                            "Expanded domain crawl budget after sustained yield",
                            data={
                                "domain": domain,
                                "previous_budget": current_domain_budget,
                                "new_budget": expanded_budget,
                                "candidate_sum_window": candidate_sum,
                                "appearance_sum_window": appearance_sum,
                            },
                        )
                        db.commit()
                        current_domain_budget = expanded_budget
            if domain_page_counts.get(domain, 0) >= current_domain_budget and not _is_pdf_like_url(url):
                run_debug["metrics"]["pages_skipped_budget"] += 1
                _event(
                    db,
                    run.id,
                    "budget_cap_reached",
                    "Domain page budget reached; skipping non-PDF URL",
                    level="warning",
                    data={
                        "url": url,
                        "domain": domain,
                        "limit": current_domain_budget,
                        "hard_limit": max_pages_per_domain_hard,
                    },
                )
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="fetch_route",
                )
                db.commit()
                continue

            await _apply_domain_throttle(settings, guard)
            run_debug["metrics"]["pages_visited"] += 1
            _event(
                db,
                run.id,
                "fetch_route",
                f"Fetching {url}",
                data={"depth": depth, "domain": domain, "request_count": guard.request_total, "canonical_url": canonical_url},
            )
            db.commit()

            try:
                page = await fetch_crawl_page(
                    settings,
                    url=url,
                    depth=depth,
                    seed_url=run.home_url,
                    session_manager=session_manager,
                )
            except Exception as exc:
                _event(
                    db,
                    run.id,
                    "fetch_route",
                    "Fetch raised exception; moving to next URL",
                    level="warning",
                    data={
                        "url": url,
                        "domain": domain,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
                visited_urls.add(url)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="fetch_route",
                )
                db.commit()
                continue

            if page.used_fallback:
                run_debug["counters"]["heuristic_fallbacks"] += 1
                _event(
                    db,
                    run.id,
                    "crawl_fetch_fallback",
                    "Crawl4AI fetch fell back to low-level fetch strategy",
                    data={
                        "url": url,
                        "fallback_reason": page.fallback_reason,
                        "fallback_method": page.fetch_method.value,
                    },
                    level="warning",
                )
                if page.fetch_method in {SourceMethod.playwright_dom, SourceMethod.playwright_network}:
                    bootstrap_status = session_manager.bootstrap_status_for_url(url)
                    if domain not in reported_bootstrap_domains and bootstrap_status.get("attempted"):
                        reported_bootstrap_domains.add(domain)
                        _event(
                            db,
                            run.id,
                            "fetch_route",
                            "Selenium bootstrap session result",
                            data={
                                "domain": domain,
                                "bootstrap_success": bool(bootstrap_status.get("success")),
                                "bootstrap_reason": bootstrap_status.get("reason"),
                                "bootstrap_cookies": int(bootstrap_status.get("cookies", 0)),
                            },
                            level="info" if bootstrap_status.get("success") else "warning",
                        )
                db.commit()

            if page.blocked:
                guard.blocked_count += 1
                run_debug["counters"]["blocked_pages"] += 1
                _event(
                    db,
                    run.id,
                    "fetch_route",
                    "Blocked by anti-bot protections",
                    level="warning",
                    data={
                        "url": url,
                        "method": page.fetch_method.value,
                        "domain": domain,
                        "blocked_count": guard.blocked_count,
                    },
                )
                db.commit()
                visited_urls.add(url)
                if guard.blocked_count >= block_threshold:
                    min_cd, max_cd = _cooldown_bounds(settings)
                    cooldown_seconds = random.randint(min_cd, max_cd) if max_cd > min_cd else min_cd
                    _event(
                        db,
                        run.id,
                        "fetch_route",
                        "Blocked threshold reached; cooling down and stopping further requests for this domain",
                        level="warning",
                        data={
                            "domain": domain,
                            "blocked_count": guard.blocked_count,
                            "cooldown_seconds": cooldown_seconds,
                        },
                    )
                    db.commit()
                    if cooldown_seconds > 0:
                        await asyncio.sleep(cooldown_seconds)
                    guard.stopped = True
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="fetch_route",
                )
                db.commit()
                continue

            if page.status != "fetched":
                _event(
                    db,
                    run.id,
                    "fetch_route",
                    "Fetch failed; moving to next URL",
                    level="warning",
                    data={"url": url, "method": page.fetch_method.value, "status": page.status},
                )
                visited_urls.add(url)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="fetch_route",
                )
                db.commit()
                continue

            visited_urls.add(url)
            domain_page_counts[domain] = domain_page_counts.get(domain, 0) + 1

            raw_summary = page.clean_text or extract_visible_text(page.html_snapshot or "", max_chars=2500)
            summary_text = sanitize_conference_context_text(
                raw_summary,
                max_chars=max(300, int(settings.nav_summary_text_chars)),
            )
            nav_links = _links_for_navigation(page)
            nav_pdf_links = _pdf_links_for_navigation(page)
            if not pdf_enabled:
                nav_links = [row for row in nav_links if not _is_pdf_like_url(str(row.get("url", "")))]
                nav_pdf_links = []
            current_page_intent = infer_page_intent(
                url=url,
                title=page.title,
                top_headings=page.top_headings,
                summary_text=summary_text,
                content_type=page.content_type,
            )
            page_year_hints = _extract_years(
                " ".join(
                    [
                        url,
                        page.title or "",
                        " ".join(page.top_headings[:8]),
                        summary_text[:1800],
                    ]
                )
            )
            page_fingerprint = _fingerprint_page(summary_text)
            fingerprint_seen_before = page_fingerprint in seen_page_fingerprints
            seen_page_fingerprints.add(page_fingerprint)

            _event(
                db,
                run.id,
                "nav_digest",
                "Built navigation digest for page",
                data={
                    "url": url,
                    "canonical_url": canonical_url,
                    "depth": depth,
                    "title": page.title,
                    "link_count": len(nav_links),
                    "pdf_link_count": len(nav_pdf_links),
                    "content_type": page.content_type,
                    "page_year_hints": page_year_hints,
                    "page_fingerprint": page_fingerprint[:16],
                    "fingerprint_seen_before": fingerprint_seen_before,
                },
            )
            db.commit()
            _event(
                db,
                run.id,
                "pathfinder_digest",
                "Prepared Pathfinder page digest",
                data={
                    "url": url,
                    "canonical_url": canonical_url,
                    "page_intent": current_page_intent,
                    "link_count": len(nav_links),
                    "pdf_link_count": len(nav_pdf_links),
                },
            )
            db.commit()

            if page.content_type == "pdf" and not pdf_enabled:
                _event(
                    db,
                    run.id,
                    "pdf_extract",
                    "PDF traversal/parsing disabled; skipping PDF URL",
                    level="warning",
                    data={"url": url},
                )
                run_debug["metrics"]["frontier_size"] = len(frontier)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="pdf_extract",
                )
                db.commit()
                continue

            embedded_candidates: list[dict[str, Any]] = []
            network_candidates: list[dict[str, Any]] = []
            dom_blocks: list[str] = []
            pdf_blocks: list[str] = []
            session_speaker_pairs: list[dict[str, Any]] = []
            pdf_text: str | None = None
            pdf_scanned = False
            network_payloads = list(page.raw_metadata.get("network_payloads") or [])
            html_snapshot = page.html_snapshot or ""
            html_for_extract = prioritize_event_content_html(html_snapshot) if html_snapshot else ""

            if page.content_type == "pdf":
                raw_pdf = page.raw_metadata.get("raw_pdf_latin1") or page.clean_text
                try:
                    pdf_text, scanned = extract_pdf_text_with_scan_flag(str(raw_pdf))
                    pdf_scanned = bool(scanned)
                except Exception:
                    pdf_text, scanned = "", False
                    pdf_scanned = False
                if scanned or not (pdf_text or "").strip():
                    _event(
                        db,
                        run.id,
                        "pdf_extract",
                        "PDF appears scanned/non-text; skipping OCR in v1",
                        level="warning",
                        data={"url": url},
                    )
                else:
                    pdf_blocks = extract_blocks_from_pdf_text(pdf_text)
                    _event(
                        db,
                        run.id,
                        "pdf_extract",
                        f"Extracted {len(pdf_blocks)} candidate blocks from PDF",
                        data={"url": url},
                    )
            else:
                if html_for_extract:
                    embedded_candidates = extract_embedded_candidates(html_for_extract)
                _event(
                    db,
                    run.id,
                    "embedded_extract",
                    f"Extracted {len(embedded_candidates)} embedded candidates",
                    data={"url": url},
                )

                network_candidates = extract_network_candidates(network_payloads)
                _event(
                    db,
                    run.id,
                    "network_extract",
                    f"Extracted {len(network_candidates)} network candidates",
                    data={"url": url},
                )

                if len(embedded_candidates) + len(network_candidates) < 3:
                    if html_for_extract:
                        dom_blocks = extract_blocks_from_html(html_for_extract)
                    if not dom_blocks and summary_text:
                        dom_blocks = [f"[BLOCK page_digest]\n{summary_text[:1200]}"]
                    _event(
                        db,
                        run.id,
                        "dom_extract",
                        f"Extracted {len(dom_blocks)} DOM candidate blocks",
                        data={"url": url},
                    )

                if nav_pdf_links:
                    _event(
                        db,
                        run.id,
                        "pdf_extract",
                        f"Discovered {len(nav_pdf_links)} PDF links in page digest",
                        data={"url": url},
                    )

                should_explore_interactions = (
                    settings.interaction_explorer_enabled
                    and depth < max_depth
                    and page.content_type == "html"
                    and (
                        len(nav_links) < max(1, int(settings.interaction_min_internal_links))
                        or (len(embedded_candidates) + len(network_candidates) + len(dom_blocks) == 0)
                        or no_progress_streak >= max(1, zero_progress_window // 2)
                    )
                )
                if should_explore_interactions:
                    _event(
                        db,
                        run.id,
                        "interaction_start",
                        "Starting generic interaction explorer",
                        data={
                            "url": url,
                            "depth": depth,
                            "link_count": len(nav_links),
                            "existing_candidates": len(embedded_candidates) + len(network_candidates) + len(dom_blocks),
                        },
                    )
                    db.commit()

                    interaction_result = await explore_interactions(
                        settings,
                        url=url,
                        seed_url=run.home_url,
                        session_manager=session_manager,
                        known_canonical_urls=seen_url_states | queued_url_states,
                        max_actions_per_page=max(1, int(settings.interaction_max_actions_per_page)),
                        no_novelty_limit=max(1, int(settings.interaction_no_novelty_limit)),
                    )
                    run_debug["metrics"]["interaction_actions_total"] += int(interaction_result.actions_total)

                    for action in interaction_result.actions[:20]:
                        _event(
                            db,
                            run.id,
                            "interaction_action",
                            "Interaction action attempted",
                            data={
                                "url": url,
                                "label": action.label,
                                "selector": action.selector,
                                "index": action.index,
                                "url_after": action.url_after,
                                "discovered_links": action.discovered_links,
                                "text_delta": action.text_delta,
                            },
                        )
                    db.commit()

                    interaction_new_links = 0
                    seen_nav_urls = {row["url"] for row in nav_links}
                    for row in interaction_result.discovered_links:
                        discovered_url = str(row.get("url", "")).strip()
                        if not discovered_url or discovered_url in seen_nav_urls:
                            continue
                        nav_links.append(
                            {
                                "url": discovered_url,
                                "text": str(row.get("text", ""))[:240],
                                "context": str(row.get("context", "interaction"))[:180],
                            }
                        )
                        seen_nav_urls.add(discovered_url)
                        interaction_new_links += 1

                    if interaction_result.network_payloads:
                        network_payloads.extend(interaction_result.network_payloads)
                        discovered_network_candidates = extract_network_candidates(interaction_result.network_payloads)
                        network_candidates.extend(discovered_network_candidates)

                    if interaction_result.interaction_blocks:
                        dom_blocks.extend(interaction_result.interaction_blocks)

                    _event(
                        db,
                        run.id,
                        "interaction_novelty",
                        "Interaction explorer discovered additional crawl/extraction signals",
                        data={
                            "url": url,
                            "actions_total": interaction_result.actions_total,
                            "actions_with_novelty": interaction_result.actions_with_novelty,
                            "discovered_links": interaction_new_links,
                            "interaction_blocks": len(interaction_result.interaction_blocks),
                            "network_payloads": len(interaction_result.network_payloads),
                        },
                    )
                    _event(
                        db,
                        run.id,
                        "interaction_stop",
                        "Interaction explorer finished",
                        data={"url": url, "stop_reason": interaction_result.stop_reason},
                    )
                    db.commit()
            db.commit()

            if page.content_type == "html" and html_for_extract:
                focused_context = extract_visible_text(
                    html_for_extract,
                    max_chars=max(5000, int(settings.llm_page_context_max_chars)),
                )
                raw_context = (page.clean_text or "")[: max(5000, int(settings.llm_page_context_max_chars))]
                # Keep context focused on event body first; add raw page text only
                # when focused extraction is too thin to preserve speaker coverage.
                if focused_context and len(focused_context.strip()) >= 2200:
                    page_context_text_raw = focused_context
                elif focused_context and raw_context:
                    page_context_text_raw = f"{focused_context}\n\n{raw_context[:8000]}"
                else:
                    page_context_text_raw = focused_context or raw_context
            else:
                page_context_text_raw = (pdf_text or page.clean_text) or extract_visible_text(
                    html_for_extract,
                    max_chars=max(5000, int(settings.llm_page_context_max_chars)),
                )
            session_speaker_pairs = extract_session_speaker_pairs(page_context_text_raw, url)
            page_context_text = sanitize_conference_context_text(
                page_context_text_raw,
                max_chars=max(5000, int(settings.llm_page_context_max_chars)),
            )
            if session_speaker_pairs:
                _event(
                    db,
                    run.id,
                    "dom_extract",
                    f"Extracted {len(session_speaker_pairs)} session-speaker pair candidates",
                    data={"url": url},
                )
                db.commit()

            prefilled_normalized_records: list[ExtractedSpeaker] = []
            reasoner_next_urls_override: list[NavigationCandidate] = []
            reasoner_stop_override = False
            reasoner_stop_reason: str | None = None
            reasoner_selected_model: str | None = None
            reasoner_selected_timeout: float | None = None
            use_legacy_extraction = not markdown_first_enabled
            markdown_candidates_from_reasoner: list[dict[str, Any]] = []

            if markdown_first_enabled:
                _event(
                    db,
                    run.id,
                    "markdown_extract_start",
                    "Starting markdown-first reasoner extraction",
                    data={"url": url, "canonical_url": canonical_url},
                )
                db.commit()
                markdown_result = await extract_and_decide(
                    settings,
                    seed_url=run.home_url,
                    page_url=url,
                    title=page.title,
                    top_headings=page.top_headings,
                    markdown_text=page_context_text,
                    internal_links=nav_links,
                    pdf_links=nav_pdf_links,
                    conference_context={
                        "conference_name_hint": default_conference_name,
                        "known_targets": known_targets[:40],
                        "page_year_hints": page_year_hints[:20],
                    },
                    visited_urls=seen_url_states,
                    max_next_urls=max(1, int(getattr(settings, "pathfinder_max_next_urls", settings.nav_max_next_urls))),
                )
                run_debug["counters"]["llm_attempts"] += int(markdown_result.debug.llm_attempts or 0)
                run_debug["counters"]["llm_failures"] += int(markdown_result.debug.llm_failures or 0)
                if markdown_result.debug.used_fallback:
                    run_debug["counters"]["heuristic_fallbacks"] += 1
                run_debug["metrics"]["markdown_pages_processed"] += 1
                run_debug["metrics"]["markdown_chars_processed"] += int(markdown_result.debug.markdown_chars or 0)
                run_debug["metrics"]["markdown_segments_used"] += int(markdown_result.debug.segments_used or 0)
                reasoner_selected_model = markdown_result.debug.selected_model
                reasoner_selected_timeout = markdown_result.debug.selected_timeout_seconds

                for segment_row in markdown_result.segment_debug:
                    _event(
                        db,
                        run.id,
                        "markdown_segment_start",
                        f"Processing markdown segment {segment_row.segment_index}/{segment_row.segment_total}",
                        data={
                            "url": url,
                            "segment_index": segment_row.segment_index,
                            "segment_total": segment_row.segment_total,
                            "chars": segment_row.chars,
                        },
                    )
                    _event(
                        db,
                        run.id,
                        "markdown_segment_end",
                        f"Processed markdown segment {segment_row.segment_index}/{segment_row.segment_total}",
                        data={
                            "url": url,
                            "segment_index": segment_row.segment_index,
                            "segment_total": segment_row.segment_total,
                            "chars": segment_row.chars,
                            "duration_ms": segment_row.duration_ms,
                            "success": segment_row.success,
                            "speaker_count": segment_row.speaker_count,
                            "next_link_count": segment_row.next_link_count,
                            "error": segment_row.error,
                        },
                        level="warning" if not segment_row.success else "info",
                    )
                db.commit()

                if markdown_result.debug.success:
                    prefilled_normalized_records = markdown_result.speakers
                    reasoner_next_urls_override = markdown_result.next_links
                    reasoner_stop_override = markdown_result.stop
                    reasoner_stop_reason = markdown_result.stop_reason
                    markdown_candidates_from_reasoner = markdown_result.markdown_candidates
                    use_legacy_extraction = False
                else:
                    use_legacy_extraction = False

                _event(
                    db,
                    run.id,
                    "markdown_extract_end",
                    "Completed markdown-first reasoner extraction",
                    data={
                        "url": url,
                        "speaker_count": len(markdown_result.speakers),
                        "next_link_count": len(markdown_result.next_links),
                        "stop": markdown_result.stop,
                        "used_fallback": markdown_result.debug.used_fallback,
                        "fallback_reason": markdown_result.debug.fallback_reason,
                        "selected_model": markdown_result.debug.selected_model,
                        "selected_timeout_seconds": markdown_result.debug.selected_timeout_seconds,
                    },
                    level="warning" if markdown_result.debug.used_fallback else "info",
                )
                db.commit()

            if use_legacy_extraction:
                candidates_for_llm = _build_llm_candidates(
                    settings=settings,
                    source_url=url,
                    embedded_candidates=embedded_candidates,
                    network_candidates=network_candidates,
                    dom_blocks=dom_blocks,
                    pdf_blocks=pdf_blocks,
                    session_speaker_pairs=session_speaker_pairs,
                    page_context_text=page_context_text,
                )
                if page.content_type == "pdf" and pdf_scanned:
                    candidates_for_llm = []
            else:
                candidates_for_llm = markdown_candidates_from_reasoner
            candidate_hashes = [_candidate_hash(row) for row in candidates_for_llm]
            new_candidates_for_llm: list[dict[str, Any]] = []
            new_candidate_hashes: list[str] = []
            for row, row_hash in zip(candidates_for_llm, candidate_hashes, strict=False):
                if row_hash in seen_candidate_hashes:
                    continue
                seen_candidate_hashes.add(row_hash)
                new_candidates_for_llm.append(row)
                new_candidate_hashes.append(row_hash)

            if use_legacy_extraction:
                modal_decision = should_attempt_modal_breaker(
                    settings,
                    page_intent=current_page_intent,
                    candidate_count=len(new_candidates_for_llm),
                    normalized_count=0,
                    already_attempted=canonical_url in modal_breaker_attempted_states,
                    html_snapshot=html_snapshot,
                    title=page.title,
                    summary_text=summary_text,
                    url=url,
                )
            else:
                modal_decision = should_attempt_modal_breaker(
                    settings,
                    page_intent=current_page_intent,
                    candidate_count=0,
                    normalized_count=len(prefilled_normalized_records),
                    already_attempted=True,
                    html_snapshot=html_snapshot,
                    title=page.title,
                    summary_text=summary_text,
                    url=url,
                )
                modal_decision.should_attempt = False
                modal_decision.reason = "markdown_first_primary_path"

            if modal_decision.dynamic_signal:
                run_debug["metrics"]["dynamic_pages_detected"] += 1

            if modal_decision.should_attempt and use_legacy_extraction:
                modal_breaker_attempted_states.add(canonical_url)
                run_debug["metrics"]["modal_breaker_attempts"] += 1
                _event(
                    db,
                    run.id,
                    "modal_breaker_start",
                    "Attempting modal-breaker render pass",
                    data={
                        "url": url,
                        "page_intent": current_page_intent,
                        "reason": modal_decision.reason,
                    },
                )
                db.commit()

                modal_fetch_error: str | None = None
                try:
                    try:
                        modal_page = await fetch_crawl_page(
                            settings,
                            url=url,
                            depth=depth,
                            seed_url=run.home_url,
                            session_manager=session_manager,
                            modal_breaker=True,
                            wait_for_selectors=str(settings.modal_breaker_wait_for_selectors),
                            js_code=expand_all_js_script(),
                            magic_mode=bool(settings.modal_breaker_magic_mode),
                        )
                    except TypeError:
                        modal_page = await fetch_crawl_page(
                            settings,
                            url=url,
                            depth=depth,
                            seed_url=run.home_url,
                            session_manager=session_manager,
                        )
                except Exception as exc:
                    modal_fetch_error = f"{type(exc).__name__}: {exc}"
                    modal_page = None
                modal_added = 0
                modal_added_links = 0
                if modal_page and modal_page.status == "fetched" and not modal_page.blocked:
                    modal_summary_text = (
                        modal_page.clean_text
                        or extract_visible_text(modal_page.html_snapshot or "", max_chars=2500)
                    )[: max(300, int(settings.nav_summary_text_chars))]
                    modal_nav_links = _links_for_navigation(modal_page)
                    modal_nav_pdfs = _pdf_links_for_navigation(modal_page)
                    seen_nav_urls = {row["url"] for row in nav_links}
                    for row in modal_nav_links:
                        candidate_url = str(row.get("url", "")).strip()
                        if not candidate_url or candidate_url in seen_nav_urls:
                            continue
                        nav_links.append(row)
                        seen_nav_urls.add(candidate_url)
                        modal_added_links += 1
                    if pdf_enabled:
                        seen_pdf_urls = {row["url"] for row in nav_pdf_links}
                        for row in modal_nav_pdfs:
                            candidate_url = str(row.get("url", "")).strip()
                            if not candidate_url or candidate_url in seen_pdf_urls:
                                continue
                            nav_pdf_links.append(row)
                            seen_pdf_urls.add(candidate_url)
                            modal_added_links += 1

                    modal_network_payloads = list(modal_page.raw_metadata.get("network_payloads") or [])
                    if modal_network_payloads:
                        network_payloads.extend(modal_network_payloads)
                    modal_html_for_extract = prioritize_event_content_html(modal_page.html_snapshot or "")
                    modal_embedded = extract_embedded_candidates(modal_html_for_extract) if modal_html_for_extract else []
                    modal_network = extract_network_candidates(modal_network_payloads)
                    modal_dom_blocks: list[str] = []
                    if len(modal_embedded) + len(modal_network) < 3:
                        if modal_html_for_extract:
                            modal_dom_blocks = extract_blocks_from_html(modal_html_for_extract)
                        if not modal_dom_blocks and modal_summary_text:
                            modal_dom_blocks = [f"[BLOCK page_digest]\n{modal_summary_text[:1200]}"]
                    if modal_page.content_type == "html":
                        modal_context = (
                            extract_visible_text(
                                modal_html_for_extract,
                                max_chars=max(5000, int(settings.llm_page_context_max_chars)),
                            )
                            if modal_html_for_extract
                            else modal_summary_text
                        )
                    else:
                        modal_context = modal_summary_text
                    modal_session_pairs = extract_session_speaker_pairs(modal_context, url)
                    modal_candidates = _build_llm_candidates(
                        settings=settings,
                        source_url=url,
                        embedded_candidates=modal_embedded,
                        network_candidates=modal_network,
                        dom_blocks=modal_dom_blocks,
                        pdf_blocks=[],
                        session_speaker_pairs=modal_session_pairs,
                        page_context_text=modal_context,
                    )
                    run_debug["metrics"]["speaker_candidates_found"] += len(modal_candidates)
                    for row in modal_candidates:
                        row_hash = _candidate_hash(row)
                        if row_hash in seen_candidate_hashes:
                            continue
                        seen_candidate_hashes.add(row_hash)
                        new_candidates_for_llm.append(row)
                        new_candidate_hashes.append(row_hash)
                        modal_added += 1
                    if modal_added > 0:
                        run_debug["metrics"]["modal_breaker_successes"] += 1
                _event(
                    db,
                    run.id,
                    "modal_breaker_end",
                    "Modal-breaker render pass finished",
                    data={
                        "url": url,
                        "reason": modal_decision.reason,
                        "added_candidates": modal_added,
                        "added_links": modal_added_links,
                        "status": modal_page.status if modal_page else "error",
                        "blocked": modal_page.blocked if modal_page else None,
                        "fetch_error": modal_fetch_error,
                    },
                    level="info" if modal_page and modal_page.status == "fetched" else "warning",
                )
                db.commit()
            else:
                _event(
                    db,
                    run.id,
                    "modal_breaker_skip",
                    "Skipped modal-breaker render pass",
                    data={
                        "url": url,
                        "page_intent": current_page_intent,
                        "reason": modal_decision.reason,
                    },
                )
                db.commit()

            run_debug["metrics"]["speaker_candidates_found"] += len(candidates_for_llm)
            run_debug["metrics"]["speaker_candidates_new"] += len(new_candidates_for_llm)
            _event(
                db,
                run.id,
                "candidate_checkpoint",
                "Candidate extraction checkpoint",
                data={
                    "url": url,
                    "canonical_url": canonical_url,
                    "depth": depth,
                    "total_candidates": len(candidates_for_llm),
                    "new_candidates": len(new_candidates_for_llm),
                    "session_speaker_pairs": len(session_speaker_pairs),
                    "fingerprint_seen_before": fingerprint_seen_before,
                    "pdf_scanned": pdf_scanned,
                },
            )
            db.commit()

            linked_before_url = run_debug["counters"]["linked_appearances"]
            url_debug: dict[str, Any] = {
                "url": url,
                "canonical_url": canonical_url,
                "depth": depth,
                "method": page.fetch_method.value,
                "http_status": page.http_status,
                "content_type": page.content_type,
                "embedded_candidates": len(embedded_candidates),
                "network_candidates": len(network_candidates),
                "dom_blocks": len(dom_blocks),
                "pdf_blocks": len(pdf_blocks),
                "session_speaker_pairs": len(session_speaker_pairs),
                "pdf_scanned": pdf_scanned,
                "llm_candidates": len(new_candidates_for_llm),
                "normalized_records": 0,
                "candidate_preview": [str(item.get("text", ""))[:220] for item in new_candidates_for_llm[:3]],
            }

            artifact_source: Source | None = None
            artifact_target = _artifact_target_for_page(known_targets=known_targets, url=url)
            if artifact_target is not None:
                artifact_conference_name, artifact_year = artifact_target
                artifact_conference_year = _ensure_conference_year(db, artifact_conference_name, artifact_year)
                _link_run_conference_year(db, run.id, artifact_conference_year.id)
                artifact_source = _get_or_create_source(
                    db,
                    source_cache,
                    conference_year_id=artifact_conference_year.id,
                    url=url,
                    category=_source_category_for_url(url),
                    method=_source_method_for_page(page),
                    fetch_status=page.status,
                    http_status=page.http_status,
                    content_type=page.content_type,
                )

            conference_year_hints_for_page = _conference_year_hints_for_page(
                known_targets=known_targets,
                default_conference_name=default_conference_name,
                page_year_hints=page_year_hints,
            )
            url_debug["conference_year_hints"] = conference_year_hints_for_page

            normalized_records: list[ExtractedSpeaker] = list(prefilled_normalized_records)
            if not normalized_records and new_candidates_for_llm:
                normalize_cache_key = _stable_hash(prompt_version + "::" + "||".join(sorted(new_candidate_hashes)))
                cached_records = normalize_cache.get(normalize_cache_key)
                if cached_records is not None:
                    normalized_records = cached_records
                    run_debug["counters"]["llm_calls_saved"] += 1
                else:
                    normalize_batch_size = max(1, int(settings.llm_normalize_batch_size))
                    expected_batches = (len(new_candidates_for_llm) + normalize_batch_size - 1) // normalize_batch_size
                    batch_outputs: list[ExtractedSpeaker] = []
                    had_timeout = False
                    normalization_cancelled = False
                    for batch_index, start in enumerate(range(0, len(new_candidates_for_llm), normalize_batch_size), start=1):
                        if run_manager.is_cancel_requested(run_id):
                            cancelled_by_user = True
                            normalization_cancelled = True
                            frontier.clear()
                            _event(
                                db,
                                run.id,
                                "run_cancelled",
                                "Run cancellation requested by user; stopping normalization batches",
                                level="warning",
                                data={
                                    "url": url,
                                    "batch_index": batch_index,
                                    "batch_total": expected_batches,
                                    "queue_remaining": len(frontier),
                                },
                            )
                            db.commit()
                            break
                        batch = new_candidates_for_llm[start : start + normalize_batch_size]
                        _event(
                            db,
                            run.id,
                            "llm_normalize_start",
                            f"Starting normalization batch {batch_index}/{expected_batches}",
                            data={
                                "url": url,
                                "batch_index": batch_index,
                                "batch_total": expected_batches,
                                "candidate_count": len(batch),
                                "conference_year_hints": conference_year_hints_for_page,
                                "selected_model": extraction_llm_model,
                                "selected_timeout_seconds": extraction_llm_timeout,
                                "watchdog_timeout_seconds": normalize_batch_watchdog_seconds,
                            },
                        )
                        db.commit()
                        started_at = time.monotonic()
                        try:
                            normalized = await asyncio.wait_for(
                                normalize_candidates(
                                    settings,
                                    batch,
                                    conference_year_hints_for_page,
                                    batch_size=len(batch),
                                ),
                                timeout=normalize_batch_watchdog_seconds,
                            )
                            _merge_llm_debug(run_debug["counters"], normalized.debug)
                            batch_outputs.extend(normalized.records)
                            duration_ms = int((time.monotonic() - started_at) * 1000)
                            _event(
                                db,
                                run.id,
                                "llm_normalize_end",
                                f"Completed normalization batch {batch_index}/{expected_batches}",
                                data={
                                    "url": url,
                                    "batch_index": batch_index,
                                    "batch_total": expected_batches,
                                    "record_count": len(normalized.records),
                                    "duration_ms": duration_ms,
                                    "used_fallback": normalized.debug.used_fallback,
                                },
                            )
                            db.commit()
                        except asyncio.TimeoutError:
                            had_timeout = True
                            stall_count += 1
                            run_debug["counters"]["stalls_recovered"] += 1
                            run_debug["counters"]["llm_batches_started"] += 1
                            run_debug["counters"]["llm_batches_timed_out"] += 1
                            _event(
                                db,
                                run.id,
                                "stage_stall_detected",
                                "Normalization batch stalled; falling back to deterministic extraction",
                                level="warning",
                                data={
                                    "url": url,
                                    "batch_index": batch_index,
                                    "batch_total": expected_batches,
                                    "watchdog_seconds": normalize_batch_watchdog_seconds,
                                    "selected_model": extraction_llm_model,
                                    "selected_timeout_seconds": extraction_llm_timeout,
                                },
                            )
                            _event(
                                db,
                                run.id,
                                "llm_batch_timeout",
                                f"Normalization batch {batch_index}/{expected_batches} timed out",
                                level="warning",
                                data={
                                    "url": url,
                                    "batch_index": batch_index,
                                    "batch_total": expected_batches,
                                    "candidate_count": len(batch),
                                    "selected_model": extraction_llm_model,
                                    "selected_timeout_seconds": extraction_llm_timeout,
                                },
                            )
                            fallback_records = heuristic_normalize_candidates(batch)
                            batch_outputs.extend(fallback_records)
                            _event(
                                db,
                                run.id,
                                "llm_normalize_end",
                                f"Completed normalization batch {batch_index}/{expected_batches} via fallback",
                                level="warning",
                                data={
                                    "url": url,
                                    "batch_index": batch_index,
                                    "batch_total": expected_batches,
                                    "record_count": len(fallback_records),
                                    "duration_ms": int((time.monotonic() - started_at) * 1000),
                                    "used_fallback": True,
                                },
                            )
                            db.commit()
                            if stall_count >= max_stalls_per_run:
                                run_debug["counters"]["stalls_terminal"] += 1
                                _event(
                                    db,
                                    run.id,
                                    "no_progress_stop",
                                    "Stopping run after repeated stall recoveries",
                                    level="warning",
                                    data={"url": url, "stall_count": stall_count},
                                )
                                db.commit()
                                terminate_due_stall = True
                                frontier.clear()
                                break
                    normalized_records = batch_outputs
                    normalize_cache[normalize_cache_key] = normalized_records
                    if not normalization_cancelled:
                        _event(
                            db,
                            run.id,
                            "llm_normalize",
                            f"Normalized {len(normalized_records)} records",
                            data={
                                "url": url,
                                "batch_size": normalize_batch_size,
                                "batch_count": expected_batches,
                                "had_timeout": had_timeout,
                                "selected_model": extraction_llm_model,
                                "selected_timeout_seconds": extraction_llm_timeout,
                            },
                        )
                        db.commit()
            elif normalized_records:
                _event(
                    db,
                    run.id,
                    "llm_normalize",
                    f"Normalized {len(normalized_records)} records via markdown-first reasoner",
                    data={
                        "url": url,
                        "batch_size": 1,
                        "batch_count": 1,
                        "had_timeout": False,
                        "selected_model": reasoner_selected_model,
                        "selected_timeout_seconds": reasoner_selected_timeout,
                    },
                )
                db.commit()
            else:
                run_debug["counters"]["llm_calls_saved"] += 1

            if cancelled_by_user:
                run_debug["metrics"]["frontier_size"] = len(frontier)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="run_cancelled",
                )
                db.commit()
                break

            url_debug["normalized_records"] = len(normalized_records)
            url_debug["normalized_preview"] = [record.full_name for record in normalized_records[:8]]
            run_debug["metrics"]["normalized_speakers"] += len(normalized_records)
            new_physicians_from_page = 0
            completion_records: list[ExtractedSpeaker] = []
            unresolved_for_reconcile: list[dict[str, Any]] = []

            for record in normalized_records:
                if not record.full_name.strip():
                    continue

                clean_name, designation_out, canonical_aliases, valid_name = _clean_name_and_designation(
                    record.full_name,
                    record.designation,
                    role=record.role,
                    evidence=record.evidence_span,
                )
                if not valid_name or not clean_name:
                    continue
                if _is_non_person_record(clean_name, record.evidence_span):
                    continue

                normalized_key = _speaker_record_key(clean_name, record.session_title, record.role)
                if normalized_key in seen_normalized_speaker_keys:
                    continue
                seen_normalized_speaker_keys.add(normalized_key)

                if not (
                    record.is_physician_candidate
                    or is_physician_like(
                        clean_name,
                        designation_out,
                        record.affiliation,
                        record.role,
                        session_title=record.session_title,
                        evidence_span=record.evidence_span,
                    )
                ):
                    continue

                targets, unresolved_reason = await _resolve_targets_for_record(
                    settings,
                    record=record,
                    source_url=url,
                    page_url=canonical_url,
                    page_title=page.title,
                    run_conference_name=run_conf_hint,
                    page_text_hint=summary_text,
                    page_year_hints=page_year_hints,
                    known_targets=known_targets,
                    default_conference_name=default_conference_name,
                    counters=run_debug["counters"],
                )

                if not targets:
                    unresolved_for_reconcile.append(
                        {
                            "record": record,
                            "clean_name": clean_name,
                            "designation_out": designation_out,
                            "canonical_aliases": canonical_aliases,
                            "initial_unresolved_reason": unresolved_reason,
                        }
                    )
                    continue

                run_debug["counters"]["attribution_resolved_count"] += 1
                for target in targets:
                    resolved_conference_name = run_conf_hint or target.conference_name
                    conference_year = _ensure_conference_year(db, resolved_conference_name, target.year)
                    _link_run_conference_year(db, run.id, conference_year.id)
                    if not any(
                        item["conference_name"].lower() == resolved_conference_name.lower() and int(item["year"]) == target.year
                        for item in known_targets
                    ):
                        known_targets.append({"conference_name": resolved_conference_name, "year": target.year})

                    year_metric = year_metrics.setdefault(
                        conference_year.id,
                        {
                            "conference_name": resolved_conference_name,
                            "year": target.year,
                            "linked": 0,
                            "duplicates": 0,
                            "notes": None,
                        },
                    )
                    if conference_year.status == ConferenceYearStatus.pending:
                        conference_year.status = ConferenceYearStatus.running

                    source = _get_or_create_source(
                        db,
                        source_cache,
                        conference_year_id=conference_year.id,
                        url=url,
                        category=_source_category_for_url(url),
                        method=_source_method_for_page(page),
                        fetch_status=page.status,
                        http_status=page.http_status,
                        content_type=page.content_type,
                    )

                    if source.id not in artifact_written_source_ids:
                        _store_source_artifacts(
                            db,
                            source_id=source.id,
                            raw_text=html_for_extract or page.clean_text,
                            pdf_text=pdf_text,
                            network_payloads=network_payloads,
                            candidates_for_llm=new_candidates_for_llm,
                            llm_records=[r.model_dump() for r in normalized_records],
                        )
                        artifact_written_source_ids.add(source.id)

                    physician = get_or_create_physician(
                        db=db,
                        full_name=clean_name,
                        designation=designation_out,
                        affiliation=record.affiliation,
                        location=record.location,
                        aliases=list(record.aliases) + canonical_aliases,
                    )
                    await _maybe_enrich_physician(
                        db,
                        settings,
                        run_id=run.id,
                        physician=physician,
                        full_name=clean_name,
                        conference_name=resolved_conference_name,
                        year=target.year,
                        session_title=record.session_title,
                        designation_hint=designation_out,
                        affiliation_hint=record.affiliation,
                        location_hint=record.location,
                        source_url=url,
                        evidence_span=record.evidence_span,
                        run_counters=run_debug["counters"],
                        attempted_physician_ids=attempted_physician_enrichment_ids,
                    )

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
                        year_metric["duplicates"] += 1
                        run_debug["counters"]["duplicate_links_skipped"] += 1
                        _event(
                            db,
                            run.id,
                            "link_duplicate_skipped",
                            f"Skipped duplicate link for {clean_name}",
                            conference_year_id=conference_year.id,
                            data={"session_title": record.session_title, "url": url},
                        )
                        db.commit()
                        continue

                    generated_brief = None
                    if not record.talk_brief_extracted and record.session_title:
                        generated_brief = await generate_talk_brief(
                            settings,
                            session_title=record.session_title,
                            raw_context=record.evidence_span or summary_text,
                        )
                        run_debug["counters"]["llm_attempts"] += 1 if settings.deepseek_api_key else 0
                        if settings.deepseek_api_key and generated_brief is None:
                            run_debug["counters"]["llm_failures"] += 1
                        if not settings.deepseek_api_key:
                            run_debug["counters"]["heuristic_fallbacks"] += 1

                    target_confidence = float(target.confidence) if target.confidence is not None else float(record.confidence or 0.7)
                    confidence = min(float(record.confidence or target_confidence), target_confidence)
                    db.add(
                        Appearance(
                            physician_id=physician.id,
                            conference_year_id=conference_year.id,
                            role=record.role,
                            session_title=record.session_title,
                            talk_brief_extracted=record.talk_brief_extracted,
                            talk_brief_generated=generated_brief,
                            confidence=confidence,
                            source_url=url,
                        )
                    )
                    if physician.id not in linked_physician_ids:
                        linked_physician_ids.add(physician.id)
                        new_physicians_from_page += 1
                    year_metric["linked"] += 1
                    run_debug["counters"]["linked_appearances"] += 1
                    _event(
                        db,
                        run.id,
                        "link_created",
                        f"Linked {clean_name} to {resolved_conference_name} {target.year}",
                        conference_year_id=conference_year.id,
                        data={"session_title": record.session_title, "confidence": confidence, "url": url},
                    )
                    db.commit()

            if unresolved_for_reconcile:
                _event(
                    db,
                    run.id,
                    "attribution_reconcile_start",
                    "Starting attribution reconciliation for unresolved speakers",
                    data={
                        "url": url,
                        "record_count": len(unresolved_for_reconcile),
                        "selected_model": attribution_llm_model,
                        "selected_timeout_seconds": attribution_llm_timeout,
                    },
                )
                db.commit()

                reconcile_payload = [entry["record"].model_dump() for entry in unresolved_for_reconcile]
                batch_result = await resolve_attribution_batch(
                    settings,
                    records=reconcile_payload,
                    source_context={
                        "source_url": url,
                        "page_url": canonical_url,
                        "page_title": page.title,
                        "page_text_hint": summary_text,
                        "run_conference_name": run_conf_hint,
                        "notes": "page_year_hints may include archive years; prioritize schedule-local evidence",
                    },
                    known_targets=known_targets,
                    default_conference_name=default_conference_name,
                    page_year_hints=page_year_hints,
                )
                _merge_llm_debug(run_debug["counters"], batch_result.debug)

                by_index = {int(item.index): item for item in batch_result.results}
                reconcile_resolved = 0
                reconcile_unresolved = 0

                for idx, entry in enumerate(unresolved_for_reconcile):
                    item = by_index.get(idx)
                    targets = [target for target in (item.targets if item else []) if _valid_target(target)]
                    record = entry["record"]
                    clean_name = str(entry["clean_name"])
                    designation_out = entry["designation_out"]
                    canonical_aliases = entry["canonical_aliases"]

                    if not targets:
                        reconcile_unresolved += 1
                        unresolved_reason = (
                            (item.unresolved_reason if item else None)
                            or str(entry.get("initial_unresolved_reason") or "")
                            or "attribution_reconcile_no_targets"
                        )
                        run_debug["counters"]["unresolved_attributions"] += 1
                        run_debug["counters"]["attribution_final_unresolved_count"] += 1
                        _event(
                            db,
                            run.id,
                            "attribution_reconcile_unresolved",
                            f"Attribution unresolved after reconciliation for {clean_name}",
                            level="warning",
                            data={
                                "url": url,
                                "reason": unresolved_reason,
                                "selected_model": attribution_llm_model,
                                "selected_timeout_seconds": attribution_llm_timeout,
                            },
                        )
                        _event(
                            db,
                            run.id,
                            "attribution_unresolved",
                            f"Attribution unresolved for {clean_name}",
                            level="warning",
                            data={
                                "url": url,
                                "reason": unresolved_reason,
                                "selected_model": attribution_llm_model,
                                "selected_timeout_seconds": attribution_llm_timeout,
                            },
                        )
                        db.commit()
                        continue

                    reconcile_resolved += 1
                    run_debug["counters"]["attribution_reconcile_resolved_count"] += 1
                    for target in targets:
                        resolved_conference_name = run_conf_hint or target.conference_name
                        conference_year = _ensure_conference_year(db, resolved_conference_name, target.year)
                        _link_run_conference_year(db, run.id, conference_year.id)
                        if not any(
                            item["conference_name"].lower() == resolved_conference_name.lower() and int(item["year"]) == target.year
                            for item in known_targets
                        ):
                            known_targets.append({"conference_name": resolved_conference_name, "year": target.year})

                        year_metric = year_metrics.setdefault(
                            conference_year.id,
                            {
                                "conference_name": resolved_conference_name,
                                "year": target.year,
                                "linked": 0,
                                "duplicates": 0,
                                "notes": None,
                            },
                        )
                        if conference_year.status == ConferenceYearStatus.pending:
                            conference_year.status = ConferenceYearStatus.running

                        source = _get_or_create_source(
                            db,
                            source_cache,
                            conference_year_id=conference_year.id,
                            url=url,
                            category=_source_category_for_url(url),
                            method=_source_method_for_page(page),
                            fetch_status=page.status,
                            http_status=page.http_status,
                            content_type=page.content_type,
                        )

                        if source.id not in artifact_written_source_ids:
                            _store_source_artifacts(
                                db,
                                source_id=source.id,
                                raw_text=html_for_extract or page.clean_text,
                                pdf_text=pdf_text,
                                network_payloads=network_payloads,
                                candidates_for_llm=new_candidates_for_llm,
                                llm_records=[r.model_dump() for r in normalized_records],
                            )
                            artifact_written_source_ids.add(source.id)

                        physician = get_or_create_physician(
                            db=db,
                            full_name=clean_name,
                            designation=designation_out,
                            affiliation=record.affiliation,
                            location=record.location,
                            aliases=list(record.aliases) + canonical_aliases,
                        )
                        await _maybe_enrich_physician(
                            db,
                            settings,
                            run_id=run.id,
                            physician=physician,
                            full_name=clean_name,
                            conference_name=resolved_conference_name,
                            year=target.year,
                            session_title=record.session_title,
                            designation_hint=designation_out,
                            affiliation_hint=record.affiliation,
                            location_hint=record.location,
                            source_url=url,
                            evidence_span=record.evidence_span,
                            run_counters=run_debug["counters"],
                            attempted_physician_ids=attempted_physician_enrichment_ids,
                        )

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
                            year_metric["duplicates"] += 1
                            run_debug["counters"]["duplicate_links_skipped"] += 1
                            _event(
                                db,
                                run.id,
                                "link_duplicate_skipped",
                                f"Skipped duplicate link for {clean_name}",
                                conference_year_id=conference_year.id,
                                data={"session_title": record.session_title, "url": url},
                            )
                            db.commit()
                            continue

                        generated_brief = None
                        if not record.talk_brief_extracted and record.session_title:
                            generated_brief = await generate_talk_brief(
                                settings,
                                session_title=record.session_title,
                                raw_context=record.evidence_span or summary_text,
                            )
                            run_debug["counters"]["llm_attempts"] += 1 if settings.deepseek_api_key else 0
                            if settings.deepseek_api_key and generated_brief is None:
                                run_debug["counters"]["llm_failures"] += 1
                            if not settings.deepseek_api_key:
                                run_debug["counters"]["heuristic_fallbacks"] += 1

                        target_confidence = float(target.confidence) if target.confidence is not None else float(record.confidence or 0.7)
                        confidence = min(float(record.confidence or target_confidence), target_confidence)
                        db.add(
                            Appearance(
                                physician_id=physician.id,
                                conference_year_id=conference_year.id,
                                role=record.role,
                                session_title=record.session_title,
                                talk_brief_extracted=record.talk_brief_extracted,
                                talk_brief_generated=generated_brief,
                                confidence=confidence,
                                source_url=url,
                            )
                        )
                        if physician.id not in linked_physician_ids:
                            linked_physician_ids.add(physician.id)
                            new_physicians_from_page += 1
                        year_metric["linked"] += 1
                        run_debug["counters"]["linked_appearances"] += 1
                        _event(
                            db,
                            run.id,
                            "link_created",
                            f"Linked {clean_name} to {resolved_conference_name} {target.year}",
                            conference_year_id=conference_year.id,
                            data={"session_title": record.session_title, "confidence": confidence, "url": url},
                        )
                        db.commit()

                _event(
                    db,
                    run.id,
                    "attribution_reconcile_end",
                    "Finished attribution reconciliation",
                    data={
                        "url": url,
                        "record_count": len(unresolved_for_reconcile),
                        "resolved_count": reconcile_resolved,
                        "unresolved_count": reconcile_unresolved,
                        "selected_model": attribution_llm_model,
                        "selected_timeout_seconds": attribution_llm_timeout,
                    },
                    level="warning" if reconcile_unresolved > 0 else "info",
                )
                db.commit()

            normalized_session_keys = {
                (
                    (record.full_name or "").strip().lower(),
                    (record.session_title or "").strip().lower(),
                )
                for record in normalized_records
                if (record.full_name or "").strip() and (record.session_title or "").strip()
            }
            completion_candidates = [
                row
                for row in session_speaker_pairs
                if str(row.get("speaker_name_raw") or "").strip()
                and str(row.get("session_title") or "").strip()
                and (
                    str(row.get("speaker_name_raw") or "").strip().lower(),
                    str(row.get("session_title") or "").strip().lower(),
                )
                not in normalized_session_keys
            ]
            if completion_candidates:
                _event(
                    db,
                    run.id,
                    "link_completion_start",
                    "Starting completion pass for missing session-speaker pairs",
                    data={"url": url, "candidate_count": len(completion_candidates)},
                )
                db.commit()

                completion_result = await normalize_candidates(
                    settings,
                    completion_candidates,
                    conference_year_hints_for_page,
                    batch_size=max(1, min(8, len(completion_candidates))),
                )
                _merge_llm_debug(run_debug["counters"], completion_result.debug)
                completion_records = completion_result.records
                run_debug["metrics"]["normalized_speakers"] += len(completion_records)

                for record in completion_records:
                    if not (record.full_name or "").strip():
                        _event(
                            db,
                            run.id,
                            "link_completion_skipped",
                            "Skipped completion record with empty speaker name",
                            data={"url": url, "session_title": record.session_title},
                        )
                        continue

                    clean_name, designation_out, canonical_aliases, valid_name = _clean_name_and_designation(
                        record.full_name,
                        record.designation,
                        role=record.role,
                        evidence=record.evidence_span,
                    )
                    if not valid_name or not clean_name or _is_non_person_record(clean_name, record.evidence_span):
                        _event(
                            db,
                            run.id,
                            "link_completion_skipped",
                            "Skipped completion record that failed person validation",
                            data={"url": url, "full_name": record.full_name, "session_title": record.session_title},
                        )
                        continue

                    normalized_key = _speaker_record_key(clean_name, record.session_title, record.role)
                    if normalized_key in seen_normalized_speaker_keys:
                        _event(
                            db,
                            run.id,
                            "link_completion_skipped",
                            "Skipped completion record already linked/seen",
                            data={"url": url, "full_name": clean_name, "session_title": record.session_title},
                        )
                        continue
                    seen_normalized_speaker_keys.add(normalized_key)

                    if not (
                        record.is_physician_candidate
                        or is_physician_like(
                            clean_name,
                            designation_out,
                            record.affiliation,
                            record.role,
                            session_title=record.session_title,
                            evidence_span=record.evidence_span,
                        )
                    ):
                        _event(
                            db,
                            run.id,
                            "link_completion_skipped",
                            "Skipped completion record not physician-like",
                            data={"url": url, "full_name": clean_name, "session_title": record.session_title},
                        )
                        continue

                    targets, unresolved_reason = await _resolve_targets_for_record(
                        settings,
                        record=record,
                        source_url=url,
                        page_url=canonical_url,
                        page_title=page.title,
                        run_conference_name=run_conf_hint,
                        page_text_hint=summary_text,
                        page_year_hints=page_year_hints,
                        known_targets=known_targets,
                        default_conference_name=default_conference_name,
                        counters=run_debug["counters"],
                    )
                    if not targets:
                        run_debug["counters"]["unresolved_attributions"] += 1
                        run_debug["counters"]["attribution_final_unresolved_count"] += 1
                        _event(
                            db,
                            run.id,
                            "link_completion_skipped",
                            "Skipped completion record due to unresolved attribution",
                            level="warning",
                            data={
                                "url": url,
                                "full_name": clean_name,
                                "session_title": record.session_title,
                                "reason": unresolved_reason,
                                "selected_model": attribution_llm_model,
                                "selected_timeout_seconds": attribution_llm_timeout,
                            },
                        )
                        db.commit()
                        continue

                    run_debug["counters"]["attribution_resolved_count"] += 1
                    for target in targets:
                        resolved_conference_name = run_conf_hint or target.conference_name
                        conference_year = _ensure_conference_year(db, resolved_conference_name, target.year)
                        _link_run_conference_year(db, run.id, conference_year.id)
                        if not any(
                            item["conference_name"].lower() == resolved_conference_name.lower()
                            and int(item["year"]) == target.year
                            for item in known_targets
                        ):
                            known_targets.append({"conference_name": resolved_conference_name, "year": target.year})

                        year_metric = year_metrics.setdefault(
                            conference_year.id,
                            {
                                "conference_name": resolved_conference_name,
                                "year": target.year,
                                "linked": 0,
                                "duplicates": 0,
                                "notes": None,
                            },
                        )
                        if conference_year.status == ConferenceYearStatus.pending:
                            conference_year.status = ConferenceYearStatus.running

                        source = _get_or_create_source(
                            db,
                            source_cache,
                            conference_year_id=conference_year.id,
                            url=url,
                            category=_source_category_for_url(url),
                            method=_source_method_for_page(page),
                            fetch_status=page.status,
                            http_status=page.http_status,
                            content_type=page.content_type,
                        )

                        if source.id not in artifact_written_source_ids:
                            _store_source_artifacts(
                                db,
                                source_id=source.id,
                                raw_text=html_for_extract or page.clean_text,
                                pdf_text=pdf_text,
                                network_payloads=network_payloads,
                                candidates_for_llm=new_candidates_for_llm,
                                llm_records=[r.model_dump() for r in normalized_records + completion_records],
                            )
                            artifact_written_source_ids.add(source.id)

                        physician = get_or_create_physician(
                            db=db,
                            full_name=clean_name,
                            designation=designation_out,
                            affiliation=record.affiliation,
                            location=record.location,
                            aliases=list(record.aliases) + canonical_aliases,
                        )
                        await _maybe_enrich_physician(
                            db,
                            settings,
                            run_id=run.id,
                            physician=physician,
                            full_name=clean_name,
                            conference_name=resolved_conference_name,
                            year=target.year,
                            session_title=record.session_title,
                            designation_hint=designation_out,
                            affiliation_hint=record.affiliation,
                            location_hint=record.location,
                            source_url=url,
                            evidence_span=record.evidence_span,
                            run_counters=run_debug["counters"],
                            attempted_physician_ids=attempted_physician_enrichment_ids,
                        )

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
                            year_metric["duplicates"] += 1
                            run_debug["counters"]["duplicate_links_skipped"] += 1
                            _event(
                                db,
                                run.id,
                                "link_completion_skipped",
                                "Skipped completion record due to duplicate appearance",
                                data={
                                    "url": url,
                                    "full_name": clean_name,
                                    "session_title": record.session_title,
                                    "year": target.year,
                                },
                            )
                            db.commit()
                            continue

                        generated_brief = None
                        if not record.talk_brief_extracted and record.session_title:
                            generated_brief = await generate_talk_brief(
                                settings,
                                session_title=record.session_title,
                                raw_context=record.evidence_span or summary_text,
                            )
                            run_debug["counters"]["llm_attempts"] += 1 if settings.deepseek_api_key else 0
                            if settings.deepseek_api_key and generated_brief is None:
                                run_debug["counters"]["llm_failures"] += 1
                            if not settings.deepseek_api_key:
                                run_debug["counters"]["heuristic_fallbacks"] += 1

                        target_confidence = float(target.confidence) if target.confidence is not None else float(record.confidence or 0.7)
                        confidence = min(float(record.confidence or target_confidence), target_confidence)
                        db.add(
                            Appearance(
                                physician_id=physician.id,
                                conference_year_id=conference_year.id,
                                role=record.role,
                                session_title=record.session_title,
                                talk_brief_extracted=record.talk_brief_extracted,
                                talk_brief_generated=generated_brief,
                                confidence=confidence,
                                source_url=url,
                            )
                        )
                        if physician.id not in linked_physician_ids:
                            linked_physician_ids.add(physician.id)
                            new_physicians_from_page += 1
                        year_metric["linked"] += 1
                        run_debug["counters"]["linked_appearances"] += 1
                        _event(
                            db,
                            run.id,
                            "link_completion_created",
                            f"Completion pass linked {clean_name} to {resolved_conference_name} {target.year}",
                            conference_year_id=conference_year.id,
                            data={
                                "session_title": record.session_title,
                                "confidence": confidence,
                                "url": url,
                            },
                        )
                        db.commit()

            if artifact_source is not None and artifact_source.id not in artifact_written_source_ids:
                _store_source_artifacts(
                    db,
                    source_id=artifact_source.id,
                    raw_text=html_for_extract or page.clean_text,
                    pdf_text=pdf_text,
                    network_payloads=network_payloads,
                    candidates_for_llm=new_candidates_for_llm,
                    llm_records=[r.model_dump() for r in normalized_records + completion_records],
                )
                artifact_written_source_ids.add(artifact_source.id)
                db.commit()

            run_debug["metrics"]["physicians_linked"] = len(linked_physician_ids)
            linked_from_url = run_debug["counters"]["linked_appearances"] - linked_before_url
            page_new_normalized = len(normalized_records) + len(completion_records)
            page_has_resolution_progress = bool(new_candidates_for_llm) or linked_from_url > 0

            current_branch_stats = branch_stats.setdefault(current_branch_id, BranchStats())
            current_branch_stats.pages_seen += 1
            current_branch_stats.new_candidates += len(new_candidates_for_llm)
            current_branch_stats.new_normalized += page_new_normalized
            current_branch_stats.new_physicians += new_physicians_from_page
            current_branch_stats.linked_appearances += linked_from_url
            domain_recent_yields[domain].append((len(new_candidates_for_llm), page_new_normalized, 0, linked_from_url))

            if link_memory_enabled:
                memory_row = update_template_memory(
                    db,
                    domain=domain,
                    template_key=template_key_for_url(url),
                    intent=current_page_intent,
                    speaker_hit=page_new_normalized > 0,
                    appearance_hit=linked_from_url > 0,
                )
                if (page_new_normalized > 0 or linked_from_url > 0) and int(memory_row.visits or 0) <= max(1, link_memory_min_visits):
                    run_debug["metrics"]["memory_templates_promoted"] += 1
                _event(
                    db,
                    run.id,
                    "memory_update",
                    "Updated navigation template memory",
                    data={
                        "url": url,
                        "domain": domain,
                        "template_key": template_key_for_url(url),
                        "intent": current_page_intent,
                        "visits": memory_row.visits,
                        "speaker_hits": memory_row.speaker_hits,
                        "appearance_hits": memory_row.appearance_hits,
                        "zero_yield_streak": memory_row.zero_yield_streak,
                    },
                )
                db.commit()

            if strict_conference_focus:
                before_links = len(nav_links)
                nav_links, dropped_links = _filter_navigation_links(
                    seed_url=run.home_url,
                    page_url=url,
                    nav_links=nav_links,
                    focus_tokens=conference_focus_tokens,
                    allow_pdf=pdf_enabled,
                )
                if pdf_enabled:
                    before_pdf = len(nav_pdf_links)
                    nav_pdf_links, dropped_pdf = _filter_navigation_links(
                        seed_url=run.home_url,
                        page_url=url,
                        nav_links=nav_pdf_links,
                        focus_tokens=conference_focus_tokens,
                        allow_pdf=True,
                    )
                else:
                    before_pdf = len(nav_pdf_links)
                    dropped_pdf = before_pdf
                    nav_pdf_links = []
                if before_links != len(nav_links) or before_pdf != len(nav_pdf_links):
                    _event(
                        db,
                        run.id,
                        "nav_decide",
                        "Applied conference-focused navigation filter",
                        data={
                            "url": url,
                            "before_links": before_links,
                            "kept_links": len(nav_links),
                            "dropped_links": dropped_links,
                            "before_pdf_links": before_pdf,
                            "kept_pdf_links": len(nav_pdf_links),
                            "dropped_pdf_links": dropped_pdf,
                            "focus_tokens": sorted(list(conference_focus_tokens))[:16],
                        },
                    )
                    db.commit()

            template_new_from_page = 0
            for row in nav_links:
                nav_url = str(row.get("url", "")).strip()
                if not nav_url:
                    continue
                nav_template = template_key_for_url(nav_url)
                nav_cluster = template_clusters.setdefault(nav_template, set())
                before_size = len(nav_cluster)
                nav_cluster.add(_canonical_url(nav_url))
                if before_size == 0 and len(nav_cluster) == 1:
                    template_new_from_page += 1

            if template_new_from_page > 0:
                _event(
                    db,
                    run.id,
                    "template_cluster_expand",
                    "Discovered new URL template clusters from page links",
                    data={
                        "url": url,
                        "new_templates": template_new_from_page,
                        "template_clusters_total": len(template_clusters),
                    },
                )
                db.commit()

            run_debug["metrics"]["branch_count"] = len(branch_stats)
            run_debug["metrics"]["template_clusters_discovered"] = len(template_clusters)
            run_debug["metrics"]["adaptive_budget_current"] = max(domain_page_budgets.values(), default=max_pages_per_domain)
            run_debug["metrics"]["high_yield_branches"] = sum(
                1
                for stats in branch_stats.values()
                if stats.new_physicians > 0 or stats.linked_appearances > 0
            )
            run_debug["metrics"]["frontier_size"] = len(frontier)

            _event(
                db,
                run.id,
                "resolution_checkpoint",
                "Resolution checkpoint",
                data={
                    "url": url,
                    "canonical_url": canonical_url,
                    "new_normalized": page_new_normalized,
                    "new_physicians": new_physicians_from_page,
                    "new_appearances": linked_from_url,
                    "branch_id": current_branch_id,
                    "no_progress_streak": no_progress_streak,
                },
            )
            db.commit()

            run_debug["urls"].append(
                {
                    **url_debug,
                    "nav_stop": False,
                    "nav_next_urls": [],
                }
            )

            if terminate_due_stall:
                _event(
                    db,
                    run.id,
                    "no_progress_stop",
                    "Run terminated due to repeated normalization stalls",
                    level="warning",
                    data={"url": url, "stall_count": stall_count},
                )
                run_debug["metrics"]["frontier_size"] = len(frontier)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="no_progress_stop",
                )
                db.commit()
                break

            if fingerprint_seen_before and not page_has_resolution_progress:
                no_progress_streak += 1
                run_debug["metrics"]["repeated_state_skips"] += 1
                _event(
                    db,
                    run.id,
                    "state_repeat_skip",
                    "Repeated page state with no new candidates; skipping navigation branch",
                    data={"url": url, "canonical_url": canonical_url, "page_fingerprint": page_fingerprint[:16]},
                )
                run_debug["metrics"]["frontier_size"] = len(frontier)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="state_repeat_skip",
                )
                db.commit()
                if no_progress_streak >= no_progress_limit:
                    _event(
                        db,
                        run.id,
                        "no_progress_stop",
                        "Stopping run after repeated no-progress pages",
                        level="warning",
                        data={"url": url, "streak": no_progress_streak},
                    )
                    run_debug["metrics"]["frontier_size"] = len(frontier)
                    _emit_progress_heartbeat(
                        db,
                        run.id,
                        run_debug,
                        queue_estimate=len(frontier),
                        no_progress_streak=no_progress_streak,
                        last_stage="no_progress_stop",
                    )
                    db.commit()
                    break
                continue

            decision_kwargs: dict[str, Any] = {
                "seed_url": run.home_url,
                "page_url": url,
                "title": page.title,
                "top_headings": page.top_headings,
                "summary_text": summary_text,
                "links": nav_links,
                "pdf_links": nav_pdf_links,
                "current_physician_like_count": max(0, linked_from_url),
                "remaining_page_budget": max(
                    0,
                    int(domain_page_budgets.get(domain, max_pages_per_domain)) - domain_page_counts.get(domain, 0),
                ),
                "remaining_depth": max(0, max_depth - depth),
                "visited_urls": visited_urls,
                "frontier_context": {
                    "frontier_size": len(frontier),
                    "branch_count": len(branch_stats),
                    "domain_page_count": domain_page_counts.get(domain, 0),
                    "domain_budget": int(domain_page_budgets.get(domain, max_pages_per_domain)),
                    "domain_hard_budget": max_pages_per_domain_hard,
                    "seed_year": seed_year,
                    "series_focus_tokens": sorted(list(conference_focus_tokens))[:20],
                },
                "branch_stats": {
                    key: {
                        "pages_seen": value.pages_seen,
                        "new_candidates": value.new_candidates,
                        "new_normalized": value.new_normalized,
                        "new_links": value.new_links,
                        "new_physicians": value.new_physicians,
                        "linked_appearances": value.linked_appearances,
                    }
                    for key, value in list(branch_stats.items())[:200]
                },
                "tried_templates": list(template_clusters.keys())[:200],
                "page_novelty": {
                    "fingerprint_seen_before": fingerprint_seen_before,
                    "new_candidates": len(new_candidates_for_llm),
                    "new_normalized": page_new_normalized,
                    "new_appearances": linked_from_url,
                    "new_physicians": new_physicians_from_page,
                },
                "content_type": page.content_type,
                "decide_next_fn": decide_next,
            }
            pathfinder_max_next_urls = max(
                1,
                int(getattr(settings, "pathfinder_max_next_urls", settings.nav_max_next_urls)),
            )
            nav_source = "reasoner" if reasoner_next_urls_override else "pathfinder"
            if reasoner_next_urls_override:
                gatekeeper_links: list[NavigationCandidate] = []
                explore_links: list[NavigationCandidate] = []
                for candidate in reasoner_next_urls_override:
                    page_type = (candidate.page_type or "").lower()
                    if any(token in page_type for token in ("gatekeeper", "session", "archive", "speaker", "program")):
                        gatekeeper_links.append(candidate)
                    else:
                        explore_links.append(candidate)
                pathfinder_decision = PathfinderDecision(
                    gatekeeper_links=gatekeeper_links[:pathfinder_max_next_urls],
                    explore_links=explore_links[:pathfinder_max_next_urls],
                    stop=reasoner_stop_override,
                    stop_reason=reasoner_stop_reason,
                    page_intent=current_page_intent,
                    debug=NavigationDecisionDebug(
                        stage="nav_decide",
                        used_llm=True,
                        success=True,
                        used_fallback=False,
                        fallback_reason=None,
                        llm_attempts=0,
                        llm_failures=0,
                        llm_http_failures=0,
                        llm_parse_failures=0,
                        selected_model=reasoner_selected_model,
                        selected_timeout_seconds=reasoner_selected_timeout,
                    ),
                )
            elif markdown_first_enabled:
                pathfinder_decision = PathfinderDecision(
                    gatekeeper_links=[],
                    explore_links=[],
                    stop=False,
                    stop_reason=None,
                    page_intent=current_page_intent,
                    debug=NavigationDecisionDebug(
                        stage="nav_decide",
                        used_llm=False,
                        success=True,
                        used_fallback=False,
                        fallback_reason=None,
                        llm_attempts=0,
                        llm_failures=0,
                        llm_http_failures=0,
                        llm_parse_failures=0,
                        selected_model=reasoner_selected_model,
                        selected_timeout_seconds=reasoner_selected_timeout,
                    ),
                )
            else:
                pathfinder_decision = await decide_pathfinder(settings, **decision_kwargs)
                _merge_pathfinder_debug(run_debug["counters"], pathfinder_decision.debug)

            decision_next_urls, scope_dropped = _filter_navigation_candidates(
                seed_url=run.home_url,
                page_url=url,
                candidates=list(pathfinder_decision.next_urls),
                focus_tokens=conference_focus_tokens,
                allow_pdf=pdf_enabled,
            )

            if markdown_first_enabled and not decision_next_urls:
                run_debug["metrics"]["nav_reask_attempts"] += 1
                _event(
                    db,
                    run.id,
                    "nav_reask_start",
                    "Reasoner returned no scoped links; running one navigation re-ask",
                    data={
                        "url": url,
                        "reasoner_next_link_count": len(reasoner_next_urls_override),
                        "scope_dropped": scope_dropped,
                    },
                )
                db.commit()

                reask_decision = await decide_pathfinder(settings, **decision_kwargs)
                _merge_pathfinder_debug(run_debug["counters"], reask_decision.debug)
                reask_next_urls, reask_scope_dropped = _filter_navigation_candidates(
                    seed_url=run.home_url,
                    page_url=url,
                    candidates=list(reask_decision.next_urls),
                    focus_tokens=conference_focus_tokens,
                    allow_pdf=pdf_enabled,
                )
                scope_dropped += reask_scope_dropped
                _event(
                    db,
                    run.id,
                    "nav_reask_end",
                    "Navigation re-ask completed",
                    data={
                        "url": url,
                        "next_url_count": len(reask_next_urls),
                        "scope_dropped": reask_scope_dropped,
                        "used_fallback": reask_decision.debug.used_fallback,
                        "selected_model": reask_decision.debug.selected_model,
                        "selected_timeout_seconds": reask_decision.debug.selected_timeout_seconds,
                    },
                )
                db.commit()

                if reask_next_urls:
                    run_debug["metrics"]["nav_reask_successes"] += 1
                    pathfinder_decision = reask_decision
                    decision_next_urls = reask_next_urls
                    nav_source = "nav_reask"
                else:
                    _event(
                        db,
                        run.id,
                        "nav_reask_no_links",
                        "Navigation re-ask returned no conference-scoped links",
                        data={"url": url},
                    )
                    run_debug["metrics"]["branches_closed_no_links"] += 1
                    _event(
                        db,
                        run.id,
                        "branch_closed_no_links",
                        "Branch closed: no conference-scoped links after one nav re-ask",
                        data={"url": url, "branch_id": current_branch_id},
                    )
                    run_debug["urls"][-1]["nav_stop"] = True
                    run_debug["urls"][-1]["nav_next_urls"] = []
                    if page_has_resolution_progress:
                        no_progress_streak = 0
                    else:
                        no_progress_streak += 1
                    run_debug["metrics"]["frontier_size"] = len(frontier)
                    _emit_progress_heartbeat(
                        db,
                        run.id,
                        run_debug,
                        queue_estimate=len(frontier),
                        no_progress_streak=no_progress_streak,
                        last_stage="branch_closed_no_links",
                    )
                    db.commit()
                    continue

            if scope_dropped > 0:
                _event(
                    db,
                    run.id,
                    "nav_decide",
                    "Applied conference-scope verifier to next links",
                    data={
                        "url": url,
                        "dropped_links": scope_dropped,
                        "kept_links": len(decision_next_urls),
                    },
                )
                db.commit()

            gatekeeper_urls = [
                row
                for row in decision_next_urls
                if any(
                    token in (row.page_type or "").lower()
                    for token in ("gatekeeper", "session", "archive", "speaker", "program")
                )
            ]
            gatekeeper_count = len(gatekeeper_urls)
            run_debug["metrics"]["gatekeeper_links_found"] += gatekeeper_count
            if gatekeeper_count > 0:
                _event(
                    db,
                    run.id,
                    "gatekeeper_found",
                    f"Pathfinder identified {gatekeeper_count} gatekeeper links",
                    data={
                        "url": url,
                        "gatekeeper_urls": [row.url for row in gatekeeper_urls[:12]],
                        "page_intent": pathfinder_decision.page_intent,
                    },
                )
                db.commit()
            _event(
                db,
                run.id,
                "pathfinder_decide",
                "Pathfinder decision generated",
                data={
                    "url": url,
                    "canonical_url": canonical_url,
                    "page_intent": pathfinder_decision.page_intent,
                    "stop": pathfinder_decision.stop,
                    "gatekeeper_count": gatekeeper_count,
                    "explore_count": max(0, len(decision_next_urls) - gatekeeper_count),
                    "next_url_count": len(decision_next_urls),
                    "nav_source": nav_source,
                    "used_fallback": pathfinder_decision.debug.used_fallback,
                    "stop_reason": pathfinder_decision.stop_reason,
                    "selected_model": pathfinder_decision.debug.selected_model,
                    "selected_timeout_seconds": pathfinder_decision.debug.selected_timeout_seconds,
                },
            )
            db.commit()

            _event(
                db,
                run.id,
                "nav_decide",
                "Navigation decision generated",
                data={
                    "url": url,
                    "canonical_url": canonical_url,
                    "stop": pathfinder_decision.stop,
                    "next_url_count": len(decision_next_urls),
                    "nav_source": nav_source,
                    "used_fallback": pathfinder_decision.debug.used_fallback,
                    "stop_reason": pathfinder_decision.stop_reason,
                    "selected_model": pathfinder_decision.debug.selected_model,
                    "selected_timeout_seconds": pathfinder_decision.debug.selected_timeout_seconds,
                },
            )
            db.commit()

            run_debug["urls"][-1]["nav_stop"] = pathfinder_decision.stop
            run_debug["urls"][-1]["nav_next_urls"] = [row.url for row in decision_next_urls]
            if page_new_normalized == 0 and decision_next_urls:
                run_debug["metrics"]["pages_with_zero_speakers_nonzero_links"] += 1

            if pathfinder_decision.stop:
                _event(
                    db,
                    run.id,
                    "nav_stop",
                    "Navigation stop requested by LLM",
                    data={"url": url, "reason": pathfinder_decision.stop_reason},
                )
                if not decision_next_urls and len(frontier) == 0:
                    run_debug["metrics"]["frontier_size"] = 0
                    _emit_progress_heartbeat(
                        db,
                        run.id,
                        run_debug,
                        queue_estimate=0,
                        no_progress_streak=no_progress_streak,
                        last_stage="nav_stop",
                    )
                    db.commit()
                    break
                _event(
                    db,
                    run.id,
                    "nav_decide",
                    "Continuing crawl despite LLM stop because frontier still has candidates",
                    level="warning",
                    data={
                        "url": url,
                        "frontier_size": len(frontier),
                        "next_url_count": len(decision_next_urls),
                    },
                )
                db.commit()

            novelty_window.append((gatekeeper_count, len(new_candidates_for_llm), page_new_normalized, linked_from_url))
            if len(novelty_window) >= novelty_window_size:
                gatekeeper_sum = sum(item[0] for item in novelty_window)
                candidate_sum = sum(item[1] for item in novelty_window)
                normalized_sum = sum(item[2] for item in novelty_window)
                linked_sum = sum(item[3] for item in novelty_window)
                if gatekeeper_sum == 0 and candidate_sum == 0 and normalized_sum == 0 and linked_sum == 0:
                    novelty_zero_windows += 1
                else:
                    novelty_zero_windows = 0
                run_debug["metrics"]["novelty_windows_without_progress"] = novelty_zero_windows
                _event(
                    db,
                    run.id,
                    "novelty_window_checkpoint",
                    "Evaluated novelty window progress",
                    data={
                        "url": url,
                        "window_size": novelty_window_size,
                        "gatekeeper_sum": gatekeeper_sum,
                        "candidate_sum": candidate_sum,
                        "normalized_sum": normalized_sum,
                        "linked_sum": linked_sum,
                        "zero_window_streak": novelty_zero_windows,
                    },
                )
                db.commit()
                if novelty_zero_windows >= novelty_zero_window_limit:
                    _event(
                        db,
                        run.id,
                        "novelty_stop",
                        "Stopping run due to repeated zero-novelty windows",
                        level="warning",
                        data={
                            "url": url,
                            "zero_window_streak": novelty_zero_windows,
                            "window_size": novelty_window_size,
                        },
                    )
                    run_debug["metrics"]["frontier_size"] = len(frontier)
                    _emit_progress_heartbeat(
                        db,
                        run.id,
                        run_debug,
                        queue_estimate=len(frontier),
                        no_progress_streak=no_progress_streak,
                        last_stage="novelty_stop",
                    )
                    db.commit()
                    break

            if depth >= max_depth:
                if page_has_resolution_progress:
                    no_progress_streak = 0
                else:
                    no_progress_streak += 1
                if no_progress_streak >= no_progress_limit:
                    _event(
                        db,
                        run.id,
                        "no_progress_stop",
                        "Stopping run after consecutive pages with no new candidates and no new links",
                        level="warning",
                        data={"url": url, "streak": no_progress_streak},
                    )
                    run_debug["metrics"]["frontier_size"] = len(frontier)
                    _emit_progress_heartbeat(
                        db,
                        run.id,
                        run_debug,
                        queue_estimate=len(frontier),
                        no_progress_streak=no_progress_streak,
                        last_stage="no_progress_stop",
                    )
                    db.commit()
                    break
                run_debug["metrics"]["frontier_size"] = len(frontier)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="nav_decide",
                )
                db.commit()
                continue

            enqueued = 0
            enqueued_urls: list[str] = []
            enqueued_url_states: list[str] = []
            scored_enqueue_rows: list[dict[str, Any]] = []
            template_new_from_enqueue = 0
            memory_scores: dict[str, float] = {}

            if link_memory_enabled and decision_next_urls:
                enqueue_templates = [template_key_for_url(item.url) for item in decision_next_urls]
                memory_scores = get_template_memory_scores(
                    db,
                    domain=domain,
                    template_keys=enqueue_templates,
                    decay_days=link_memory_decay_days,
                    min_visits=link_memory_min_visits,
                )
                run_debug["metrics"]["memory_templates_hit"] += len(memory_scores)
                if memory_scores:
                    _event(
                        db,
                        run.id,
                        "memory_bias_applied",
                        "Applied memory bias to enqueue scoring",
                        data={
                            "url": url,
                            "domain": domain,
                            "template_scores": {
                                key: round(value, 4)
                                for key, value in sorted(memory_scores.items(), key=lambda row: row[1], reverse=True)[:20]
                            },
                        },
                    )
                    db.commit()

            for item in decision_next_urls:
                next_url = item.url
                next_canonical = _canonical_url(next_url)
                if next_canonical in seen_url_states or next_canonical in queued_url_states:
                    continue
                if not pdf_enabled and _is_pdf_like_url(next_url):
                    continue
                next_domain = _effective_domain(next_url)
                next_domain_budget = int(domain_page_budgets.get(next_domain, max_pages_per_domain))
                if domain_page_counts.get(next_domain, 0) >= next_domain_budget and not _is_pdf_like_url(next_url):
                    run_debug["metrics"]["pages_skipped_budget"] += 1
                    continue

                next_template = template_key_for_url(next_url)
                next_cluster = template_clusters.setdefault(next_template, set())
                before_cluster = len(next_cluster)
                next_cluster.add(next_canonical)
                if before_cluster == 0 and len(next_cluster) == 1:
                    template_new_from_enqueue += 1

                next_branch_hint = (item.branch_hint or next_template)[:180]
                next_branch_id = branch_id_for_url(next_url, hint=next_branch_hint)
                next_novelty_score = _novelty_score_for_url(
                    canonical_url=next_canonical,
                    template_key=next_template,
                    seen_url_states=seen_url_states,
                    template_clusters=template_clusters,
                )
                template_memory_score = float(memory_scores.get(next_template, 0.0))
                branch_score = branch_yield_score(branch_stats.get(next_branch_id))
                depth_penalty = 0.05 * float(depth + 1)
                next_priority = (
                    (0.45 * float(item.priority))
                    + (0.30 * template_memory_score)
                    + (0.15 * next_novelty_score)
                    + (0.10 * branch_score)
                    - depth_penalty
                )
                next_priority = max(0.01, min(1.0, next_priority))
                next_priority = max(0.01, min(1.0, next_priority + _year_priority_delta(next_url, seed_year)))
                frontier_seq += 1
                _push_frontier(
                    frontier,
                    FrontierNode(
                        priority=next_priority,
                        sequence=frontier_seq,
                        url=next_url,
                        canonical_url=next_canonical,
                        depth=depth + 1,
                        branch_id=next_branch_id,
                        llm_priority=float(item.priority),
                        estimated_yield=float(item.expected_yield if item.expected_yield is not None else item.priority),
                        novelty_score=next_novelty_score,
                        enqueued_at=time.monotonic(),
                    ),
                )
                queued_url_states.add(next_canonical)
                enqueued += 1
                enqueued_urls.append(next_url)
                enqueued_url_states.append(next_canonical)
                run_debug["metrics"]["pages_enqueued"] += 1
                scored_enqueue_rows.append(
                    {
                        "url": next_url,
                        "canonical_url": next_canonical,
                        "branch_id": next_branch_id,
                        "priority": round(next_priority, 4),
                        "llm_priority": round(float(item.priority), 4),
                        "memory_score": round(template_memory_score, 4),
                        "novelty_score": round(next_novelty_score, 4),
                        "template_key": next_template,
                    }
                )

            current_branch_stats.new_links += enqueued
            if domain_recent_yields[domain]:
                last_candidates, last_normalized, _, last_appearances = domain_recent_yields[domain].pop()
                domain_recent_yields[domain].append((last_candidates, last_normalized, enqueued, last_appearances))

            if template_new_from_enqueue > 0:
                _event(
                    db,
                    run.id,
                    "template_cluster_expand",
                    "Discovered new template clusters during enqueue scoring",
                    data={
                        "url": url,
                        "new_templates": template_new_from_enqueue,
                        "template_clusters_total": len(template_clusters),
                    },
                )
                db.commit()

            if page_has_resolution_progress or enqueued > 0:
                no_progress_streak = 0
            else:
                no_progress_streak += 1

            if decision_next_urls and enqueued == 0 and len(frontier) == 0:
                _event(
                    db,
                    run.id,
                    "no_progress_stop",
                    "Stopping run because all next URLs resolve to previously seen states",
                    level="warning",
                    data={"url": url, "next_url_count": len(decision_next_urls)},
                )
                run_debug["metrics"]["frontier_size"] = len(frontier)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="no_progress_stop",
                )
                db.commit()
                break

            if no_progress_streak >= no_progress_limit:
                _event(
                    db,
                    run.id,
                    "no_progress_stop",
                    "Stopping run after consecutive pages with no new candidates and no new links",
                    level="warning",
                    data={"url": url, "streak": no_progress_streak},
                )
                run_debug["metrics"]["frontier_size"] = len(frontier)
                _emit_progress_heartbeat(
                    db,
                    run.id,
                    run_debug,
                    queue_estimate=len(frontier),
                    no_progress_streak=no_progress_streak,
                    last_stage="no_progress_stop",
                )
                db.commit()
                break

            if enqueued > 0:
                _event(
                    db,
                    run.id,
                    "nav_enqueue",
                    f"Enqueued {enqueued} next URL(s)",
                    data={
                        "from_url": url,
                        "urls": enqueued_urls,
                        "canonical_urls": enqueued_url_states,
                        "next_depth": depth + 1,
                    },
                )
                _event(
                    db,
                    run.id,
                    "frontier_score",
                    "Scored and prioritized frontier candidates",
                    data={"from_url": url, "count": enqueued, "candidates": scored_enqueue_rows[:40]},
                )

            run_debug["metrics"]["branch_count"] = len(branch_stats)
            run_debug["metrics"]["template_clusters_discovered"] = len(template_clusters)
            run_debug["metrics"]["adaptive_budget_current"] = max(domain_page_budgets.values(), default=max_pages_per_domain)
            run_debug["metrics"]["high_yield_branches"] = sum(
                1
                for stats in branch_stats.values()
                if stats.new_physicians > 0 or stats.linked_appearances > 0
            )
            run_debug["metrics"]["frontier_size"] = len(frontier)
            _emit_progress_heartbeat(
                db,
                run.id,
                run_debug,
                queue_estimate=len(frontier),
                no_progress_streak=no_progress_streak,
                last_stage="nav_enqueue" if enqueued > 0 else "nav_decide",
            )
            db.commit()

        if not frontier and not terminate_due_stall:
            _event(
                db,
                run.id,
                "nav_noop_stop",
                "Navigation queue exhausted after de-duplication/novelty guards",
                data={"visited_url_count": len(visited_urls), "unique_url_states": len(seen_url_states)},
            )
            _emit_progress_heartbeat(
                db,
                run.id,
                run_debug,
                queue_estimate=0,
                no_progress_streak=no_progress_streak,
                last_stage="nav_noop_stop",
            )
            db.commit()

        if linked_physician_ids:
            merge_stats = merge_close_physicians(db, physician_ids=set(linked_physician_ids))
            if merge_stats.merged_physicians > 0:
                _event(
                    db,
                    run.id,
                    "physician_merge_applied",
                    f"Merged {merge_stats.merged_physicians} duplicate physician records",
                    data={
                        "merged_physicians": merge_stats.merged_physicians,
                        "moved_aliases": merge_stats.moved_aliases,
                        "moved_appearances": merge_stats.moved_appearances,
                        "duplicate_appearances_skipped": merge_stats.duplicate_appearances_skipped,
                    },
                )
                db.commit()

        discovered_rows = db.execute(
            select(RunConferenceYear, ConferenceYear, Conference)
            .join(ConferenceYear, ConferenceYear.id == RunConferenceYear.conference_year_id)
            .join(Conference, Conference.id == ConferenceYear.conference_id)
            .where(RunConferenceYear.run_id == run.id)
            .order_by(Conference.name.asc(), ConferenceYear.year.asc())
        ).all()

        any_complete = False
        any_partial = False
        for _, conference_year, conference in discovered_rows:
            metrics = year_metrics.get(
                conference_year.id,
                {
                    "conference_name": conference.name,
                    "year": conference_year.year,
                    "linked": 0,
                    "duplicates": 0,
                    "notes": None,
                },
            )
            if metrics["linked"] > 0:
                conference_year.status = ConferenceYearStatus.complete
                conference_year.notes = f"Linked {metrics['linked']} physician appearance(s)"
                any_complete = True
            elif metrics["duplicates"] > 0:
                conference_year.status = ConferenceYearStatus.partial
                conference_year.notes = "Only duplicate appearance records found"
                any_partial = True
            else:
                conference_year.status = ConferenceYearStatus.partial
                conference_year.notes = "Conference-year discovered but no attributable physician links"
                any_partial = True

            run_debug["years"].append(
                {
                    "conference_year_id": conference_year.id,
                    "conference_name": conference.name,
                    "year": conference_year.year,
                    "status": conference_year.status.value,
                    "notes": conference_year.notes,
                    "linked": metrics["linked"],
                    "duplicates": metrics["duplicates"],
                }
            )

        if cancelled_by_user:
            run.status = RunStatus.partial
            run.error_message = "Cancelled by user"
        elif terminate_due_stall:
            run.status = RunStatus.partial
            run.error_message = "Run terminated after repeated normalization stalls"
        elif not discovered_rows:
            if run_debug["counters"]["blocked_pages"] > 0:
                run.status = RunStatus.partial
                run.error_message = "No attributable conference-years found; sources were blocked or inconclusive"
            else:
                run.status = RunStatus.error
                run.error_message = "No attributable conference-years discovered"
        elif run_debug["counters"]["linked_appearances"] > 0 and not any_partial:
            run.status = RunStatus.complete
        elif any_complete or any_partial:
            run.status = RunStatus.partial
        else:
            run.status = RunStatus.error

        run.finished_at = datetime.now(timezone.utc)
        run_debug["status"] = run.status.value
        _refresh_metrics_and_progress(
            run_debug,
            queue_estimate=0,
            no_progress_streak=0,
            last_stage="run_complete",
        )
        _event(
            db,
            run.id,
            "run_complete",
            f"Run completed with status {run.status.value}",
            data={
                "counters": run_debug["counters"],
                "metrics": run_debug["metrics"],
                "progress_state": run_debug["progress_state"],
            },
        )
        db.commit()
    except asyncio.CancelledError:
        # Cancellation can arrive while we're blocked in a long await (LLM/fetch).
        # Treat cancellation as a partial (terminal) run and persist it.
        db.rollback()
        run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
        if run:
            run.status = RunStatus.partial
            run.error_message = "Cancelled by user"
            run.finished_at = datetime.now(timezone.utc)
            db.add(
                RunEvent(
                    run_id=run.id,
                    conference_year_id=None,
                    stage="run_cancelled",
                    level="warning",
                    message="Run cancelled by user",
                    data_json='{"source":"task_cancel"}',
                )
            )
            db.commit()
        run_debug["status"] = "partial"
        run_debug["errors"].append({"error": "Cancelled by user"})
        return
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run_id)).scalar_one_or_none()
        if run:
            run.status = RunStatus.error
            run.error_message = str(exc)
            run.finished_at = datetime.now(timezone.utc)
            db.add(
                RunEvent(
                    run_id=run.id,
                    conference_year_id=None,
                    stage="run_error",
                    level="error",
                    message=str(exc),
                )
            )
            db.commit()
        run_debug["status"] = "error"
        run_debug["errors"].append({"error": str(exc)})
    finally:
        run_manager.clear_cancel_request(run_id)
        if session_manager is not None:
            try:
                await session_manager.close()
            except Exception:
                pass
        run_debug["finished_at"] = datetime.now(timezone.utc).isoformat()
        try:
            path = _write_run_debug_log(run_id, run_debug)
            print(f"[run-debug] wrote {path}")
        except Exception as exc:  # noqa: BLE001
            print(f"[run-debug] failed to write log for run {run_id}: {exc}")
        db.close()

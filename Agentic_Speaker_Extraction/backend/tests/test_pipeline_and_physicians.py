from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Appearance, ConferenceYear, ConferenceYearStatus, Physician, RunConferenceYear, RunStatus, ScrapeRun
from app.services import runs
from app.services.attribution_llm import AttributionResult, AttributionTarget
from app.services.crawl_fetch import CrawlDigestLink, CrawlPageResult
from app.services.extract_llm import AttributionTargetHint, ExtractedSpeaker, NormalizeResult
from app.services.fetchers import FetchResult
from app.services.navigation_llm import NavigationCandidate, NavigationDecisionResult, NavigationDecisionDebug


@pytest.mark.asyncio
async def test_execute_run_home_url_dedupes_physicians_across_discovered_years(monkeypatch) -> None:
    db = SessionLocal()
    run = ScrapeRun(
        id=str(uuid4()),
        home_url="https://seed.example.org",
        status=RunStatus.pending,
    )
    db.add(run)
    db.commit()
    db.close()

    async def fake_fetch_http(url: str, timeout_seconds: float = 15.0):
        return FetchResult(
            url=url,
            method=runs.SourceMethod.http_static,
            fetch_status="fetched",
            http_status=200,
            content_type="html",
            text="<html><body><a href='/p1'>P1</a><a href='/p2'>P2</a></body></html>",
            blocked=False,
            network_payloads=[],
        )

    async def fake_fetch_crawl_page(settings, *, url: str, depth: int, seed_url: str, session_manager=None):  # noqa: ANN001
        if url.endswith("/p1"):
            body = (
                "<html><body>"
                "<section class='speaker-card'>Dr. Jane Smith, MD Speaker Retina Update</section>"
                "</body></html>"
            )
        elif url.endswith("/p2"):
            body = (
                "<html><body>"
                "<section class='speaker-card'>Jane Smith MD Panelist Glaucoma Panel</section>"
                "</body></html>"
            )
        else:
            body = "<html><body><a href='/p1'>Program</a><a href='/p2'>Speakers</a></body></html>"

        return CrawlPageResult(
            url=url,
            content_type="html",
            status="fetched",
            clean_text=body,
            title="Continuum",
            top_headings=["Continuum"],
            internal_links=[
                CrawlDigestLink(url="https://seed.example.org/p1", text="Program", context="menu"),
                CrawlDigestLink(url="https://seed.example.org/p2", text="Speakers", context="menu"),
            ]
            if url == "https://seed.example.org"
            else [],
            pdf_links=[],
            html_snapshot=body,
            raw_metadata={"network_payloads": []},
            http_status=200,
            blocked=False,
            fetch_method=runs.SourceMethod.http_static,
            used_fallback=False,
            fallback_reason=None,
        )

    async def fake_decide_next(  # noqa: ANN001
        settings,
        *,
        seed_url: str,
        page_url: str,
        title: str,
        top_headings: list[str],
        summary_text: str,
        links: list[dict[str, str]],
        pdf_links: list[dict[str, str]],
        current_physician_like_count: int,
        remaining_page_budget: int,
        remaining_depth: int,
        visited_urls: set[str],
    ):
        if page_url.endswith("/p2"):
            return NavigationDecisionResult(
                next_urls=[],
                stop=True,
                stop_reason="done",
                extraction_hint="complete",
                debug=NavigationDecisionDebug(used_llm=False, success=True, used_fallback=True, fallback_reason="test"),
            )
        if page_url.endswith("/p1"):
            return NavigationDecisionResult(
                next_urls=[
                    NavigationCandidate(
                        url="https://seed.example.org/p2",
                        reason="next page",
                        priority=0.9,
                        page_type="speakers",
                    )
                ],
                stop=False,
                stop_reason=None,
                extraction_hint="continue",
                debug=NavigationDecisionDebug(used_llm=False, success=True, used_fallback=True, fallback_reason="test"),
            )
        return NavigationDecisionResult(
            next_urls=[
                NavigationCandidate(
                    url="https://seed.example.org/p1",
                    reason="program",
                    priority=0.95,
                    page_type="program",
                ),
                NavigationCandidate(
                    url="https://seed.example.org/p2",
                    reason="speakers",
                    priority=0.94,
                    page_type="speakers",
                ),
            ],
            stop=False,
            stop_reason=None,
            extraction_hint="continue",
            debug=NavigationDecisionDebug(used_llm=False, success=True, used_fallback=True, fallback_reason="test"),
        )

    async def fake_normalize(settings, candidates, conference_year_hints, batch_size: int = 40):  # noqa: ANN001
        url = candidates[0]["source_url"]
        if url.endswith("/p1"):
            records = [
                ExtractedSpeaker(
                    full_name="Dr. Jane Smith, MD",
                    designation="MD",
                    affiliation="University Eye Institute",
                    location="Boston",
                    role="Speaker",
                    session_title="Retina Update",
                    talk_brief_extracted=None,
                    aliases=["Jane Smith, MD"],
                    is_physician_candidate=True,
                    confidence=0.95,
                    evidence_span="Jane Smith Retina Update",
                    attribution_targets=[
                        AttributionTargetHint(
                            conference_name="Continuum",
                            year=2024,
                            confidence=0.93,
                            reason="page_header",
                        )
                    ],
                )
            ]
        else:
            records = [
                ExtractedSpeaker(
                    full_name="Jane Smith MD",
                    designation="MD",
                    affiliation="University Eye Institute",
                    location="Boston",
                    role="Panelist",
                    session_title="Glaucoma Panel",
                    talk_brief_extracted="Panel on clinical cases",
                    aliases=["Dr Jane Smith"],
                    is_physician_candidate=True,
                    confidence=0.91,
                    evidence_span="Glaucoma panel details",
                    attribution_targets=[
                        AttributionTargetHint(
                            conference_name="Continuum",
                            year=2025,
                            confidence=0.92,
                            reason="session_label",
                        )
                    ],
                )
            ]

        return NormalizeResult(records=records)

    async def fake_resolve_attribution(settings, record, source_context, known_targets):  # noqa: ANN001
        return AttributionResult(
            targets=[
                AttributionTarget(
                    conference_name="Continuum",
                    year=2024,
                    confidence=0.80,
                    reason="fallback",
                )
            ]
        )

    brief_calls = {"count": 0}

    async def fake_generate_brief(settings, session_title: str, raw_context: str):  # noqa: ANN001
        brief_calls["count"] += 1
        return f"Generated summary for {session_title}"

    monkeypatch.setattr(runs, "fetch_http", fake_fetch_http)
    monkeypatch.setattr(runs, "fetch_crawl_page", fake_fetch_crawl_page)
    monkeypatch.setattr(runs, "decide_next", fake_decide_next)
    monkeypatch.setattr(runs, "normalize_candidates", fake_normalize)
    monkeypatch.setattr(runs, "resolve_attribution", fake_resolve_attribution)
    monkeypatch.setattr(runs, "generate_talk_brief", fake_generate_brief)

    await runs.execute_run(run.id)

    db = SessionLocal()
    physicians = db.execute(select(Physician)).scalars().all()
    appearances = db.execute(select(Appearance)).scalars().all()
    run_links = db.execute(select(RunConferenceYear)).scalars().all()
    years = db.execute(select(ConferenceYear).order_by(ConferenceYear.year.asc())).scalars().all()
    events = db.execute(select(runs.RunEvent).where(runs.RunEvent.run_id == run.id)).scalars().all()
    refreshed_run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run.id)).scalar_one()
    db.close()

    assert len(physicians) == 1
    assert len(appearances) == 2
    assert {a.session_title for a in appearances} == {"Retina Update", "Glaucoma Panel"}
    assert len(run_links) == 2
    assert {y.year for y in years} == {2024, 2025}
    assert all(y.status == ConferenceYearStatus.complete for y in years)
    assert refreshed_run.status in {RunStatus.complete, RunStatus.partial}
    assert brief_calls["count"] == 1
    assert not any(evt.stage in {"discover_links", "rerank_links"} for evt in events)


@pytest.mark.asyncio
async def test_execute_run_survives_blocked_primary_when_alternative_succeeds(monkeypatch) -> None:
    db = SessionLocal()
    run = ScrapeRun(
        id=str(uuid4()),
        home_url="https://seed.example.org",
        status=RunStatus.pending,
    )
    db.add(run)
    db.commit()
    db.close()

    async def fake_fetch_http(url: str, timeout_seconds: float = 15.0):
        return FetchResult(
            url=url,
            method=runs.SourceMethod.http_static,
            fetch_status="fetched",
            http_status=200,
            content_type="html",
            text="<html><body><a href='/blocked'>Blocked</a><a href='/ok'>OK</a></body></html>",
            blocked=False,
            network_payloads=[],
        )

    async def fake_fetch_crawl_page(settings, *, url: str, depth: int, seed_url: str, session_manager=None):  # noqa: ANN001
        if url.endswith("/blocked"):
            return CrawlPageResult(
                url=url,
                content_type="html",
                status="error",
                clean_text="Cloudflare challenge",
                title="Blocked",
                top_headings=[],
                internal_links=[],
                pdf_links=[],
                html_snapshot="Cloudflare challenge",
                raw_metadata={},
                http_status=403,
                blocked=True,
                fetch_method=runs.SourceMethod.playwright_network,
                used_fallback=True,
                fallback_reason="blocked",
            )

        body = (
            "<html><body>"
            "<div class='speaker-profile'>Alicia Rao, MD Speaker Retina Triage</div>"
            "speaker page"
            "</body></html>"
        )
        return CrawlPageResult(
            url=url,
            content_type="html",
            status="fetched",
            clean_text=body,
            title="Continuum",
            top_headings=["Continuum"],
            internal_links=[],
            pdf_links=[],
            html_snapshot=body,
            raw_metadata={"network_payloads": []},
            http_status=200,
            blocked=False,
            fetch_method=runs.SourceMethod.http_static,
            used_fallback=False,
            fallback_reason=None,
        )

    async def fake_decide_next(  # noqa: ANN001
        settings,
        *,
        seed_url: str,
        page_url: str,
        title: str,
        top_headings: list[str],
        summary_text: str,
        links: list[dict[str, str]],
        pdf_links: list[dict[str, str]],
        current_physician_like_count: int,
        remaining_page_budget: int,
        remaining_depth: int,
        visited_urls: set[str],
    ):
        if page_url.endswith("/ok"):
            return NavigationDecisionResult(
                next_urls=[],
                stop=True,
                stop_reason="done",
                extraction_hint="complete",
                debug=NavigationDecisionDebug(used_llm=False, success=True, used_fallback=True, fallback_reason="test"),
            )
        return NavigationDecisionResult(
            next_urls=[
                NavigationCandidate(
                    url="https://seed.example.org/blocked",
                    reason="candidate",
                    priority=0.9,
                    page_type="unknown",
                ),
                NavigationCandidate(
                    url="https://seed.example.org/ok",
                    reason="candidate",
                    priority=0.9,
                    page_type="unknown",
                ),
            ],
            stop=False,
            stop_reason=None,
            extraction_hint="continue",
            debug=NavigationDecisionDebug(used_llm=False, success=True, used_fallback=True, fallback_reason="test"),
        )

    async def fake_normalize(settings, candidates, conference_year_hints, batch_size: int = 40):  # noqa: ANN001
        return NormalizeResult(
            records=[
                ExtractedSpeaker(
                    full_name="Alicia Rao, MD",
                    designation="MD",
                    affiliation=None,
                    location=None,
                    role="Speaker",
                    session_title="Retina Triage",
                    talk_brief_extracted="Clinical decision pathways",
                    aliases=["Dr Alicia Rao"],
                    is_physician_candidate=True,
                    confidence=0.91,
                    evidence_span="Alicia Rao Retina Triage",
                    attribution_targets=[
                        AttributionTargetHint(
                            conference_name="Continuum",
                            year=2026,
                            confidence=0.91,
                            reason="header",
                        )
                    ],
                )
            ]
        )

    monkeypatch.setattr(runs, "fetch_http", fake_fetch_http)
    monkeypatch.setattr(runs, "fetch_crawl_page", fake_fetch_crawl_page)
    monkeypatch.setattr(runs, "decide_next", fake_decide_next)
    monkeypatch.setattr(runs, "normalize_candidates", fake_normalize)

    await runs.execute_run(run.id)

    db = SessionLocal()
    refreshed_run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run.id)).scalar_one()
    appearances = db.execute(select(Appearance)).scalars().all()
    events = db.execute(select(runs.RunEvent).where(runs.RunEvent.run_id == run.id)).scalars().all()
    db.close()

    assert refreshed_run.status in {RunStatus.complete, RunStatus.partial}
    assert len(appearances) == 1
    assert any(evt.stage == "fetch_route" and "Blocked" in evt.message for evt in events)


def test_physicians_endpoint_shows_all_conference_year_links(client) -> None:
    db = SessionLocal()

    from app.models import Conference

    conference = Conference(name="Link Test", canonical_name="link test")
    db.add(conference)
    db.flush()

    year_a = ConferenceYear(conference_id=conference.id, year=2023, status=ConferenceYearStatus.complete)
    year_b = ConferenceYear(conference_id=conference.id, year=2024, status=ConferenceYearStatus.complete)
    db.add_all([year_a, year_b])
    db.flush()

    physician = Physician(full_name="Jose Alvarez", name_key="jose alvarez", primary_designation="MD")
    db.add(physician)
    db.flush()

    db.add_all(
        [
            Appearance(physician_id=physician.id, conference_year_id=year_a.id, session_title="Session A", confidence=0.8),
            Appearance(physician_id=physician.id, conference_year_id=year_b.id, session_title="Session B", confidence=0.9),
        ]
    )
    db.commit()
    db.close()

    response = client.get("/api/v1/physicians")
    assert response.status_code == 200
    body = response.json()

    assert len(body) == 1
    assert body[0]["full_name"] == "Jose Alvarez"
    assert len(body[0]["appearances"]) == 2


@pytest.mark.asyncio
async def test_execute_run_recovers_from_normalize_stall(monkeypatch) -> None:
    db = SessionLocal()
    run = ScrapeRun(
        id=str(uuid4()),
        home_url="https://seed.example.org",
        status=RunStatus.pending,
    )
    db.add(run)
    db.commit()
    db.close()

    async def fake_fetch_http(url: str, timeout_seconds: float = 15.0):
        return FetchResult(
            url=url,
            method=runs.SourceMethod.http_static,
            fetch_status="fetched",
            http_status=200,
            content_type="html",
            text="<html><head><title>ACTHIV 2026</title></head><body>ACTHIV 2026</body></html>",
            blocked=False,
            network_payloads=[],
        )

    async def fake_fetch_crawl_page(settings, *, url: str, depth: int, seed_url: str, session_manager=None):  # noqa: ANN001
        body = (
            "<html><body>"
            "<div class='speaker-profile'>Dr. Alicia Rao, MD Speaker Retina Triage</div>"
            "</body></html>"
        )
        return CrawlPageResult(
            url=url,
            content_type="html",
            status="fetched",
            clean_text=body,
            title="ACTHIV 2026",
            top_headings=["ACTHIV 2026"],
            internal_links=[],
            pdf_links=[],
            html_snapshot=body,
            raw_metadata={"network_payloads": []},
            http_status=200,
            blocked=False,
            fetch_method=runs.SourceMethod.http_static,
            used_fallback=False,
            fallback_reason=None,
        )

    async def fake_decide_next(  # noqa: ANN001
        settings,
        *,
        seed_url: str,
        page_url: str,
        title: str,
        top_headings: list[str],
        summary_text: str,
        links: list[dict[str, str]],
        pdf_links: list[dict[str, str]],
        current_physician_like_count: int,
        remaining_page_budget: int,
        remaining_depth: int,
        visited_urls: set[str],
    ):
        return NavigationDecisionResult(
            next_urls=[],
            stop=True,
            stop_reason="done",
            extraction_hint="complete",
            debug=NavigationDecisionDebug(used_llm=False, success=True, used_fallback=True, fallback_reason="test"),
        )

    async def fake_normalize(settings, candidates, conference_year_hints, batch_size: int = 40):  # noqa: ANN001
        await asyncio.sleep(1.5)
        return NormalizeResult(records=[])

    monkeypatch.setattr(runs, "fetch_http", fake_fetch_http)
    monkeypatch.setattr(runs, "fetch_crawl_page", fake_fetch_crawl_page)
    monkeypatch.setattr(runs, "decide_next", fake_decide_next)
    monkeypatch.setattr(runs, "normalize_candidates", fake_normalize)
    original_settings = runs.get_settings()
    monkeypatch.setattr(
        runs,
        "get_settings",
        lambda: original_settings.model_copy(update={"watchdog_stall_seconds": 1, "watchdog_max_stalls_per_run": 1}),
    )

    await runs.execute_run(run.id)

    db = SessionLocal()
    refreshed_run = db.execute(select(ScrapeRun).where(ScrapeRun.id == run.id)).scalar_one()
    events = db.execute(select(runs.RunEvent).where(runs.RunEvent.run_id == run.id)).scalars().all()
    db.close()

    assert refreshed_run.status in {RunStatus.partial, RunStatus.complete}
    assert refreshed_run.status != RunStatus.running
    assert any(evt.stage == "stage_stall_detected" for evt in events)
    assert any(evt.stage == "llm_batch_timeout" for evt in events)


def test_infer_seed_targets_uses_page_year_hints_not_url_lock() -> None:
    targets = runs._infer_seed_targets(
        home_url="https://www.iapac.org/conferences/continuum-2025/",
        page_title="Continuum 2025 | IAPAC",
        year_hints=[2024, 2025, 2026],
    )
    years = sorted(item["year"] for item in targets)
    assert years == [2025, 2026]


@pytest.mark.asyncio
async def test_execute_run_link_completion_creates_missing_appearance(monkeypatch) -> None:
    db = SessionLocal()
    run = ScrapeRun(
        id=str(uuid4()),
        home_url="https://seed.example.org/continuum-2025/",
        status=RunStatus.pending,
    )
    db.add(run)
    db.commit()
    db.close()

    async def fake_fetch_http(url: str, timeout_seconds: float = 15.0):
        return FetchResult(
            url=url,
            method=runs.SourceMethod.http_static,
            fetch_status="fetched",
            http_status=200,
            content_type="html",
            text="<html><head><title>Continuum 2025</title></head><body>Continuum 2025</body></html>",
            blocked=False,
            network_payloads=[],
        )

    async def fake_fetch_crawl_page(settings, *, url: str, depth: int, seed_url: str, session_manager=None):  # noqa: ANN001
        body = """
        <html><body>
        **PLENARY SESSION 2: HIV TREATMENT**
        * Person-Centered HIV Care: Implementing Patient Choice in ART Rupa Patel
        </body></html>
        """
        return CrawlPageResult(
            url=url,
            content_type="html",
            status="fetched",
            clean_text=body,
            title="Continuum 2025",
            top_headings=["Continuum 2025"],
            internal_links=[],
            pdf_links=[],
            html_snapshot=body,
            raw_metadata={"network_payloads": []},
            http_status=200,
            blocked=False,
            fetch_method=runs.SourceMethod.http_static,
            used_fallback=False,
            fallback_reason=None,
        )

    async def fake_decide_next(  # noqa: ANN001
        settings,
        *,
        seed_url: str,
        page_url: str,
        title: str,
        top_headings: list[str],
        summary_text: str,
        links: list[dict[str, str]],
        pdf_links: list[dict[str, str]],
        current_physician_like_count: int,
        remaining_page_budget: int,
        remaining_depth: int,
        visited_urls: set[str],
    ):
        return NavigationDecisionResult(
            next_urls=[],
            stop=True,
            stop_reason="done",
            extraction_hint="complete",
            debug=NavigationDecisionDebug(used_llm=False, success=True, used_fallback=True, fallback_reason="test"),
        )

    call_state = {"count": 0}

    async def fake_normalize(settings, candidates, conference_year_hints, batch_size: int = 40):  # noqa: ANN001
        call_state["count"] += 1
        if call_state["count"] == 1:
            return NormalizeResult(records=[])
        return NormalizeResult(
            records=[
                ExtractedSpeaker(
                    full_name="Rupa Patel",
                    designation=None,
                    affiliation=None,
                    location=None,
                    role="Speaker",
                    session_title="Person-Centered HIV Care: Implementing Patient Choice in ART",
                    talk_brief_extracted=None,
                    aliases=[],
                    is_physician_candidate=False,
                    confidence=0.85,
                    evidence_span="Session: Person-Centered HIV Care: Implementing Patient Choice in ART. Speaker: Rupa Patel",
                    attribution_targets=[
                        AttributionTargetHint(
                            conference_name="Continuum",
                            year=2025,
                            confidence=0.9,
                            reason="session_pair",
                        )
                    ],
                )
            ]
        )

    monkeypatch.setattr(runs, "fetch_http", fake_fetch_http)
    monkeypatch.setattr(runs, "fetch_crawl_page", fake_fetch_crawl_page)
    monkeypatch.setattr(runs, "decide_next", fake_decide_next)
    monkeypatch.setattr(runs, "normalize_candidates", fake_normalize)

    await runs.execute_run(run.id)

    db = SessionLocal()
    appearances = db.execute(select(Appearance)).scalars().all()
    events = db.execute(select(runs.RunEvent).where(runs.RunEvent.run_id == run.id)).scalars().all()
    db.close()

    assert len(appearances) == 1
    assert appearances[0].session_title == "Person-Centered HIV Care: Implementing Patient Choice in ART"
    assert any(evt.stage == "link_completion_start" for evt in events)
    assert any(evt.stage == "link_completion_created" for evt in events)

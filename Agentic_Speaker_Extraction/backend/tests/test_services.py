from __future__ import annotations

import json

import httpx
import pytest

from app.config import Settings
from app.services import link_discovery
from app.services.conference_identity import infer_conference_identity
from app.services.crawl_fetch import fetch_crawl_page
from app.services.extract_candidates import (
    extract_blocks_from_html,
    extract_blocks_from_network_json,
    extract_blocks_from_pdf_text,
    extract_embedded_candidates,
    extract_internal_links,
    extract_network_candidates,
    extract_session_speaker_pairs,
)
from app.services.fetchers import choose_fetch_method, detect_blocked
from app.services.frontier import BranchStats, branch_id_for_url, frontier_priority, template_key_for_url
from app.services.interaction_explorer import explore_interactions
from app.services.link_discovery import discover_internal_links
from app.services.navigation_llm import decide_next
from app.services.pathfinder_agent import decide_pathfinder, infer_page_intent
from app.services.preflight_classifier import build_seed_summary_packet, classify_seed_page
from app.services.extractor_agent import expand_all_js_script, should_attempt_modal_breaker
from app.services.search import SearchResult
from app.services.triage_llm import triage_urls


@pytest.mark.asyncio
async def test_triage_retries_on_invalid_json(monkeypatch) -> None:
    calls = {"count": 0}

    class FakeResponse:
        def __init__(self, content: str):
            self.status_code = 200
            self._content = content

        def json(self) -> dict:
            return {"choices": [{"message": {"content": self._content}}]}

    async def fake_post(self, *args, **kwargs):  # noqa: ANN001, ANN002
        calls["count"] += 1
        if calls["count"] == 1:
            return FakeResponse("{not-valid-json")

        valid_payload = {
            "selected": [
                {
                    "url": "https://example.org/program",
                    "category": "official_program",
                    "method": "http_static",
                    "score": 0.88,
                    "reason": "Official program page",
                }
            ],
            "discarded": [],
            "platform_guess": {"name": "none", "evidence": []},
        }
        return FakeResponse(json.dumps(valid_payload))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        deepseek_api_key="x",
        deepseek_model="deepseek-chat",
        deepseek_base_url="https://api.deepseek.com",
    )

    results = [SearchResult(url="https://example.org/program", title="Program", snippet="Agenda")]
    triage = await triage_urls(settings, results)

    assert calls["count"] == 2
    assert triage.selected[0].url == "https://example.org/program"


@pytest.mark.asyncio
async def test_preflight_classification_fallback_without_api_key(fixture_text) -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        deepseek_api_key="",
        deepseek_model="deepseek-chat",
        deepseek_base_url="https://api.deepseek.com",
    )

    packet = build_seed_summary_packet(
        home_url="https://conference.example.org",
        html=fixture_text["html"],
        content_type="html",
        http_status=200,
        blocked=False,
    )
    result = await classify_seed_page(settings, packet)

    assert result.debug.used_fallback is True
    assert result.suggested_paths
    assert result.page_type in {"homepage", "speakers", "program"}


def test_fetch_router_method_selection() -> None:
    assert choose_fetch_method("https://site.org/program.pdf").value == "pdf_text"
    assert choose_fetch_method("https://events.cvent.com/event/abc").value == "playwright_network"
    assert choose_fetch_method("https://site.org/speakers").value == "http_static"


@pytest.mark.asyncio
async def test_internal_link_discovery_same_domain_and_subdomains(monkeypatch) -> None:
    seed_html = """
    <html><body>
      <a href="/program">Program</a>
      <a href="https://sub.example.org/speakers">Speakers</a>
      <a href="https://outside.net/agenda">Outside</a>
    </body></html>
    """

    async def fake_discover_from_sitemap(seed_url: str):
        return ["https://example.org/sitemap-program", "https://sub.example.org/faculty"]

    async def fake_fetch_text(url: str, timeout_seconds: float = 15.0):
        return "<a href='/agenda'>Agenda</a>"

    monkeypatch.setattr(link_discovery, "_discover_from_sitemap", fake_discover_from_sitemap)
    monkeypatch.setattr(link_discovery, "_fetch_text", fake_fetch_text)

    from app.services.preflight_classifier import SeedClassificationResult

    classification = SeedClassificationResult(
        page_type="homepage",
        suggested_paths=["/speakers"],
        priority_links=["https://example.org/high-priority"],
        stop_rules=[],
    )

    links = await discover_internal_links("https://example.org", seed_html, classification, max_candidates=50)
    urls = {row.url for row in links}

    assert "https://example.org/program" in urls
    assert "https://sub.example.org/speakers" in urls
    assert "https://outside.net/agenda" not in urls


def test_block_detection_and_candidate_extractors(fixture_text) -> None:
    assert detect_blocked(403, "") is False
    assert detect_blocked(200, "Please verify you are human") is False
    assert detect_blocked(200, "Normal conference page") is False

    links = extract_internal_links(fixture_text["html"], "https://example.org")
    assert isinstance(links, list)

    html_blocks = extract_blocks_from_html(fixture_text["html"])
    pdf_blocks = extract_blocks_from_pdf_text(fixture_text["pdf_text"])
    network_blocks = extract_blocks_from_network_json(fixture_text["network_json"])

    embedded = extract_embedded_candidates(
        """
        <script type='application/ld+json'>
        {"@type":"Person","name":"Jane Doe","title":"MD","affiliation":"Clinic"}
        </script>
        """
    )
    network_candidates = extract_network_candidates(
        [{"data": {"sessions": [{"title": "Session 1", "speakers": [{"name": "Dr. A"}]}]}}]
    )
    session_pairs = extract_session_speaker_pairs(
        """
        **PLENARY SESSION 2: HIV TREATMENT**
        * Person-Centered HIV Care: Implementing Patient Choice in ART Rupa Patel
        * Long-Acting ART: Transforming the HIV Treatment Landscape Monica Gandhi
        """,
        "https://example.org/program",
    )
    html_session_pairs = extract_session_speaker_pairs(
        """
        <p><strong>PLENARY SESSION 2: HIV TREATMENT</strong></p>
        <ul>
          <li><a href="https://example.org/rupa.pdf"><em>Person-Centered HIV Care: Implementing Patient Choice in ART</em></a><br>Rupa Patel</li>
          <li><a href="https://example.org/monica.pdf"><em>Long-Acting ART: Transforming the HIV Treatment Landscape</em></a><br>Monica Gandhi</li>
        </ul>
        """,
        "https://example.org/program",
    )
    nav_noise_pairs = extract_session_speaker_pairs(
        """
        [ Join The AAN ](https://www.aan.com/conferences-community/member-engagement/) Log In
        [ Education ](https://www.aan.com/education) Education Education
        [ Research ](https://www.aan.com/research) Research Research
        """,
        "https://example.org/program",
    )

    assert any("Jane Smith" in block for block in html_blocks)
    assert any("Maria Gomez" in block for block in pdf_blocks)
    assert any("Priya Nair" in block for block in network_blocks)
    assert embedded
    assert network_candidates
    assert any(item.get("speaker_name_raw") == "Rupa Patel" for item in session_pairs)
    assert any(item.get("speaker_name_raw") == "Rupa Patel" for item in html_session_pairs)
    assert nav_noise_pairs == []


@pytest.mark.asyncio
async def test_navigation_llm_fallback_filters_domain_and_visited() -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        deepseek_api_key="",
        nav_max_next_urls=3,
    )
    result = await decide_next(
        settings,
        seed_url="https://example.org",
        page_url="https://example.org/home",
        title="Home",
        top_headings=[],
        summary_text="conference home",
        links=[
            {"url": "https://example.org/program", "text": "Program", "context": "menu"},
            {"url": "https://sub.example.org/speakers", "text": "Speakers", "context": "menu"},
            {"url": "https://outside.net/agenda", "text": "Agenda", "context": "menu"},
        ],
        pdf_links=[
            {"url": "https://example.org/program.pdf", "text": "Program PDF", "context": "hero"},
        ],
        current_physician_like_count=0,
        remaining_page_budget=4,
        remaining_depth=2,
        visited_urls={"https://example.org/program"},
    )

    urls = [row.url for row in result.next_urls]
    assert result.debug.used_fallback is True
    assert "https://example.org/program" not in urls
    assert "https://outside.net/agenda" not in urls
    assert "https://example.org/program.pdf" in urls
    assert len(urls) <= 3


@pytest.mark.asyncio
async def test_navigation_llm_fallback_stays_in_event_context() -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        deepseek_api_key="",
        nav_max_next_urls=8,
    )
    result = await decide_next(
        settings,
        seed_url="https://www.aan.com/events/annual-meeting",
        page_url="https://www.aan.com/events/annual-meeting",
        title="Annual Meeting",
        top_headings=[],
        summary_text="Annual meeting event page",
        links=[
            {"url": "https://www.aan.com/education/leadership-programs", "text": "Leadership Programs", "context": "menu"},
            {"url": "https://www.aan.com/research/aan-research-program", "text": "Research Program", "context": "menu"},
            {"url": "https://www.aan.com/events/annual-meeting/programming", "text": "Programming", "context": "subnav"},
            {"url": "https://www.aan.com/events/annual-meeting/abstracts", "text": "Abstracts", "context": "subnav"},
            {"url": "https://www.aan.com/msa/Public/Events/Index/", "text": "Events Index", "context": "events"},
        ],
        pdf_links=[],
        current_physician_like_count=0,
        remaining_page_budget=8,
        remaining_depth=2,
        visited_urls=set(),
    )

    urls = [row.url for row in result.next_urls]
    assert result.debug.used_fallback is True
    assert "https://www.aan.com/events/annual-meeting/programming" in urls
    assert "https://www.aan.com/events/annual-meeting/abstracts" in urls
    assert "https://www.aan.com/msa/Public/Events/Index/" in urls
    assert "https://www.aan.com/education/leadership-programs" not in urls
    assert "https://www.aan.com/research/aan-research-program" not in urls


@pytest.mark.asyncio
async def test_crawl_fetch_fallback_when_crawl4ai_disabled(monkeypatch) -> None:
    async def fake_fetch_source(url: str, method, session_manager=None):  # noqa: ANN001
        from app.services.fetchers import FetchResult

        return FetchResult(
            url=url,
            method=method,
            fetch_status="fetched",
            http_status=200,
            content_type="html",
            text="<html><body><h1>Program</h1><a href='/speakers'>Speakers</a></body></html>",
            blocked=False,
            network_payloads=[],
        )

    monkeypatch.setattr("app.services.crawl_fetch.fetch_source", fake_fetch_source)

    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        crawl4ai_enabled=False,
    )

    page = await fetch_crawl_page(
        settings,
        url="https://example.org",
        depth=0,
        seed_url="https://example.org",
        session_manager=None,
    )

    assert page.status == "fetched"
    assert page.used_fallback is True
    assert page.fallback_reason == "crawl4ai_disabled"
    assert any(link.url.endswith("/speakers") for link in page.internal_links)


@pytest.mark.asyncio
async def test_crawl_fetch_rejects_out_of_domain() -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        crawl4ai_enabled=False,
    )
    page = await fetch_crawl_page(
        settings,
        url="https://outside.net/page",
        depth=1,
        seed_url="https://example.org",
        session_manager=None,
    )
    assert page.status == "error"
    assert page.fallback_reason == "out_of_domain"


def test_frontier_template_and_priority_scoring() -> None:
    template_a = template_key_for_url("https://www.aan.com/msa/Public/Events/Details/22140")
    template_b = template_key_for_url("https://www.aan.com/msa/public/events/details/22177")
    assert template_a == template_b

    branch = branch_id_for_url("https://www.aan.com/msa/Public/Events/Details/22140", hint=template_a)
    assert "aan.com" in branch

    high_stats = BranchStats(
        pages_seen=4,
        new_candidates=40,
        new_normalized=12,
        new_links=20,
        new_physicians=5,
        linked_appearances=8,
    )
    low_stats = BranchStats(
        pages_seen=4,
        new_candidates=1,
        new_normalized=0,
        new_links=0,
        new_physicians=0,
        linked_appearances=0,
    )
    high_priority = frontier_priority(llm_priority=0.55, branch_stats=high_stats, novelty_score=0.5, depth=1)
    low_priority = frontier_priority(llm_priority=0.55, branch_stats=low_stats, novelty_score=0.5, depth=1)
    assert high_priority > low_priority


@pytest.mark.asyncio
async def test_conference_identity_fallback_organizer_plus_event() -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        deepseek_api_key="",
    )
    html = """
    <html>
      <head>
        <meta property="og:site_name" content="American Academy of Neurology" />
      </head>
      <body>
        <h1>Annual Meeting</h1>
      </body>
    </html>
    """
    result = await infer_conference_identity(
        settings,
        home_url="https://www.aan.com/events/annual-meeting",
        page_title="AAN Annual Meeting 2026 | American Academy of Neurology",
        html=html,
        top_headings=["Annual Meeting", "Programming"],
        year_hints=[2026],
    )
    assert result.debug.used_fallback is True
    assert result.organizer_name
    assert result.event_series_name
    assert "American Academy of Neurology" in result.display_name
    assert "Annual Meeting" in result.display_name


@pytest.mark.asyncio
async def test_interaction_explorer_short_circuit_in_test_env() -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        app_env="test",
        interaction_explorer_enabled=True,
    )
    result = await explore_interactions(
        settings,
        url="https://example.org",
        seed_url="https://example.org",
        session_manager=None,  # type: ignore[arg-type]
        known_canonical_urls=set(),
    )
    assert result.stop_reason == "test_env_disabled"
    assert result.actions_total == 0


def test_pathfinder_page_intent_inference() -> None:
    assert (
        infer_page_intent(
            url="https://example.org/program",
            title="Scientific Program",
            top_headings=["Plenary Sessions"],
            summary_text="Agenda and faculty",
            content_type="html",
        )
        == "session_detail"
    )
    assert (
        infer_page_intent(
            url="https://example.org/archive/2024",
            title="Past Meetings",
            top_headings=["Archive"],
            summary_text="Previous years",
            content_type="html",
        )
        == "archive"
    )
    assert (
        infer_page_intent(
            url="https://example.org/files/program.pdf",
            title="Program PDF",
            top_headings=[],
            summary_text="",
            content_type="pdf",
        )
        == "pdf"
    )


@pytest.mark.asyncio
async def test_pathfinder_decision_splits_gatekeeper_and_explore() -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        deepseek_api_key="",
        pathfinder_enabled=True,
        pathfinder_max_next_urls=6,
    )
    decision = await decide_pathfinder(
        settings,
        seed_url="https://example.org/events/annual",
        page_url="https://example.org/events/annual",
        title="Annual Meeting",
        top_headings=["Scientific Program"],
        summary_text="Browse abstracts and faculty",
        links=[
            {"url": "https://example.org/events/annual/program", "text": "Program", "context": "menu"},
            {"url": "https://example.org/events/annual/speakers", "text": "Speakers", "context": "menu"},
            {"url": "https://example.org/events/annual/travel", "text": "Travel", "context": "menu"},
        ],
        pdf_links=[],
        current_physician_like_count=0,
        remaining_page_budget=10,
        remaining_depth=2,
        visited_urls=set(),
    )

    gatekeeper_urls = {item.url for item in decision.gatekeeper_links}
    assert "https://example.org/events/annual/program" in gatekeeper_urls
    assert any("speaker" in url for url in gatekeeper_urls)
    assert all("travel" not in item.url for item in decision.gatekeeper_links)
    assert decision.page_intent in {"gatekeeper", "listing", "session_detail"}


def test_modal_breaker_decision_rules() -> None:
    settings = Settings(
        postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper",
        modal_breaker_enabled=True,
        modal_breaker_min_candidates=3,
    )
    decision = should_attempt_modal_breaker(
        settings,
        page_intent="session_detail",
        candidate_count=1,
        normalized_count=0,
        already_attempted=False,
        html_snapshot="<button aria-expanded='false'>View Session</button>",
        title="Program Details",
        summary_text="View session and faculty",
        url="https://example.org/program",
    )
    assert decision.should_attempt is True
    assert decision.dynamic_signal is True
    assert "low_yield" in decision.reason

    skipped = should_attempt_modal_breaker(
        settings,
        page_intent="non_content",
        candidate_count=0,
        normalized_count=0,
        already_attempted=False,
        html_snapshot="<div>Travel info</div>",
        title="Travel",
        summary_text="Hotel and travel",
        url="https://example.org/travel",
    )
    assert skipped.should_attempt is False


def test_expand_all_js_script_contains_session_and_speaker_markers() -> None:
    script = expand_all_js_script().lower()
    assert "session" in script
    assert "speaker" in script

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.config import Settings
from app.services.navigation_llm import NavigationCandidate, NavigationDecisionDebug, decide_next


PAGE_INTENT_VALUES = {"gatekeeper", "listing", "session_detail", "archive", "non_content", "pdf"}
GATEKEEPER_HINTS = {
    "program",
    "agenda",
    "speaker",
    "speakers",
    "faculty",
    "presenter",
    "presenters",
    "session",
    "sessions",
    "cme",
    "abstract",
    "abstracts",
    "plenary",
    "scientific",
}
UTILITY_HINTS = {
    "travel",
    "hotel",
    "housing",
    "sponsor",
    "sponsorship",
    "exhibits",
    "registration",
    "faq",
    "about",
    "contact",
    "policy",
    "privacy",
}
ARCHIVE_HINTS = {"archive", "archives", "past", "previous", "history"}


@dataclass(slots=True)
class PathfinderDecision:
    gatekeeper_links: list[NavigationCandidate] = field(default_factory=list)
    explore_links: list[NavigationCandidate] = field(default_factory=list)
    stop: bool = False
    stop_reason: str | None = None
    page_intent: str = "listing"
    debug: NavigationDecisionDebug = field(default_factory=NavigationDecisionDebug)

    @property
    def next_urls(self) -> list[NavigationCandidate]:
        return [*self.gatekeeper_links, *self.explore_links]


def _text_tokens(*values: str | None) -> set[str]:
    blob = " ".join((value or "").lower() for value in values)
    parts: set[str] = set()
    for token in blob.replace("/", " ").replace("_", " ").replace("-", " ").split():
        token = token.strip(".,:;()[]{}!?\"'`")
        if len(token) < 2:
            continue
        parts.add(token)
    return parts


def infer_page_intent(
    *,
    url: str,
    title: str,
    top_headings: list[str],
    summary_text: str,
    content_type: str,
) -> str:
    if (content_type or "").lower() == "pdf" or url.lower().endswith(".pdf"):
        return "pdf"
    tokens = _text_tokens(url, title, summary_text, " ".join(top_headings[:10]))
    if tokens.intersection(ARCHIVE_HINTS):
        return "archive"
    if tokens.intersection(GATEKEEPER_HINTS):
        if "session" in tokens or "plenary" in tokens:
            return "session_detail"
        return "gatekeeper"
    if tokens.intersection(UTILITY_HINTS):
        return "non_content"
    return "listing"


def _candidate_confidence(candidate: NavigationCandidate) -> float:
    if candidate.expected_yield is not None:
        return float(candidate.expected_yield)
    return float(candidate.priority)


def _is_gatekeeper_candidate(candidate: NavigationCandidate, *, min_confidence: float) -> bool:
    confidence = _candidate_confidence(candidate)
    if confidence < min_confidence:
        return False

    tokens = _text_tokens(candidate.url, candidate.reason, candidate.page_type, candidate.branch_hint)
    if tokens.intersection(UTILITY_HINTS) and not tokens.intersection(GATEKEEPER_HINTS):
        return False
    return bool(tokens.intersection(GATEKEEPER_HINTS))


async def decide_pathfinder(
    settings: Settings,
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
    frontier_context: dict[str, Any] | None = None,
    branch_stats: dict[str, Any] | None = None,
    tried_templates: list[str] | None = None,
    page_novelty: dict[str, Any] | None = None,
    content_type: str = "html",
    decide_next_fn=None,
) -> PathfinderDecision:
    page_intent = infer_page_intent(
        url=page_url,
        title=title,
        top_headings=top_headings,
        summary_text=summary_text,
        content_type=content_type,
    )
    if page_intent not in PAGE_INTENT_VALUES:
        page_intent = "listing"

    nav_fn = decide_next_fn or decide_next

    if not settings.pathfinder_enabled:
        fallback_kwargs = {
            "seed_url": seed_url,
            "page_url": page_url,
            "title": title,
            "top_headings": top_headings,
            "summary_text": summary_text,
            "links": links,
            "pdf_links": pdf_links,
            "current_physician_like_count": current_physician_like_count,
            "remaining_page_budget": remaining_page_budget,
            "remaining_depth": remaining_depth,
            "visited_urls": visited_urls,
            "frontier_context": frontier_context,
            "branch_stats": branch_stats,
            "tried_templates": tried_templates,
            "page_novelty": page_novelty,
        }
        try:
            fallback = await nav_fn(settings, **fallback_kwargs)
        except TypeError:
            for key in ("frontier_context", "branch_stats", "tried_templates", "page_novelty"):
                fallback_kwargs.pop(key, None)
            fallback = await nav_fn(settings, **fallback_kwargs)
        return PathfinderDecision(
            gatekeeper_links=fallback.next_urls[:1],
            explore_links=fallback.next_urls[1:],
            stop=fallback.stop,
            stop_reason=fallback.stop_reason,
            page_intent=page_intent,
            debug=fallback.debug,
        )

    nav_kwargs = {
        "seed_url": seed_url,
        "page_url": page_url,
        "title": title,
        "top_headings": top_headings,
        "summary_text": summary_text,
        "links": links,
        "pdf_links": pdf_links,
        "current_physician_like_count": current_physician_like_count,
        "remaining_page_budget": remaining_page_budget,
        "remaining_depth": remaining_depth,
        "visited_urls": visited_urls,
        "frontier_context": frontier_context,
        "branch_stats": branch_stats,
        "tried_templates": tried_templates,
        "page_novelty": page_novelty,
        "max_next_urls_override": int(settings.pathfinder_max_next_urls),
        "llm_retry_override": int(settings.pathfinder_llm_retry_count),
    }
    try:
        nav = await nav_fn(settings, **nav_kwargs)
    except TypeError:
        for key in (
            "max_next_urls_override",
            "llm_retry_override",
            "frontier_context",
            "branch_stats",
            "tried_templates",
            "page_novelty",
        ):
            nav_kwargs.pop(key, None)
        nav = await nav_fn(settings, **nav_kwargs)

    min_conf = max(0.0, min(1.0, float(settings.pathfinder_gatekeeper_min_conf)))
    gatekeeper_links: list[NavigationCandidate] = []
    explore_links: list[NavigationCandidate] = []
    for candidate in nav.next_urls:
        if _is_gatekeeper_candidate(candidate, min_confidence=min_conf):
            gatekeeper_links.append(candidate)
        else:
            explore_links.append(candidate)

    # Ensure we always keep crawl momentum even when gatekeeper classification is strict.
    if not gatekeeper_links and explore_links:
        gatekeeper_links = [explore_links.pop(0)]

    return PathfinderDecision(
        gatekeeper_links=gatekeeper_links,
        explore_links=explore_links,
        stop=nav.stop,
        stop_reason=nav.stop_reason,
        page_intent=page_intent,
        debug=nav.debug,
    )

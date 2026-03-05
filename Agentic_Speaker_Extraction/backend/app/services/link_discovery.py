from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.services.extract_candidates import extract_internal_links
from app.services.preflight_classifier import SeedClassificationResult


KEYWORDS = {
    "speakers": 12,
    "speaker": 12,
    "faculty": 12,
    "presenters": 11,
    "presenter": 11,
    "agenda": 10,
    "program": 10,
    "schedule": 9,
    "sessions": 9,
    "session": 9,
    "abstract": 8,
    "symposium": 7,
    "workshop": 7,
    "scientific-program": 8,
    "brochure": 6,
    "pdf": 5,
}


@dataclass(slots=True)
class DiscoveredLink:
    url: str
    anchor: str
    score: float
    reason: str
    depth: int


def _registrable_domain(host: str) -> str:
    parts = [p for p in host.lower().split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _same_site(seed_url: str, candidate_url: str) -> bool:
    seed = urlparse(seed_url)
    candidate = urlparse(candidate_url)
    return _registrable_domain(seed.netloc) == _registrable_domain(candidate.netloc)


def _score_link(url: str, anchor: str) -> tuple[float, str]:
    blob = f"{url} {anchor}".lower()
    score = 0.0
    reasons: list[str] = []
    for key, weight in KEYWORDS.items():
        if key in blob:
            score += float(weight)
            reasons.append(key)

    if url.lower().endswith(".pdf"):
        score += 4.0
        reasons.append("pdf")

    return score, ",".join(reasons[:5]) or "keyword_score"


async def _fetch_text(url: str, timeout_seconds: float = 15.0) -> str:
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            resp = await client.get(url)
    except httpx.HTTPError:
        return ""

    if resp.status_code >= 400:
        return ""

    return resp.text


async def _discover_from_sitemap(seed_url: str) -> list[str]:
    root = f"{urlparse(seed_url).scheme}://{urlparse(seed_url).netloc}"
    sitemap_urls = [f"{root}/sitemap.xml"]
    robots_url = f"{root}/robots.txt"

    robots_text = await _fetch_text(robots_url)
    for line in robots_text.splitlines():
        lowered = line.lower().strip()
        if not lowered.startswith("sitemap:"):
            continue
        _, value = line.split(":", 1)
        candidate = value.strip()
        if candidate:
            sitemap_urls.append(candidate)

    discovered: list[str] = []
    for sitemap_url in sitemap_urls[:5]:
        xml = await _fetch_text(sitemap_url)
        if not xml.strip():
            continue
        soup = BeautifulSoup(xml, "xml")
        for loc in soup.select("url > loc"):
            url = (loc.get_text() or "").strip()
            if not url:
                continue
            if _same_site(seed_url, url):
                discovered.append(url)

    return discovered


async def discover_internal_links(
    seed_url: str,
    seed_html: str,
    classification: SeedClassificationResult,
    *,
    max_candidates: int = 50,
) -> list[DiscoveredLink]:
    discovered: dict[str, DiscoveredLink] = {}

    def add_link(url: str, anchor: str, depth: int) -> None:
        if not url:
            return
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return
        if not _same_site(seed_url, url):
            return
        score, reason = _score_link(url, anchor)
        existing = discovered.get(url)
        candidate = DiscoveredLink(url=url, anchor=anchor[:200], score=score, reason=reason, depth=depth)
        if existing is None or candidate.score > existing.score:
            discovered[url] = candidate

    for link in extract_internal_links(seed_html, seed_url, max_links=150):
        add_link(link["url"], link.get("anchor", ""), 0)

    for url in classification.priority_links:
        add_link(urljoin(seed_url, url), "priority", 0)

    for path in classification.suggested_paths:
        add_link(urljoin(seed_url, path), "suggested_path", 0)

    sitemap_urls = await _discover_from_sitemap(seed_url)
    for url in sitemap_urls[:120]:
        add_link(url, "sitemap", 0)

    # Depth-1 expansion on top candidates.
    top_seed = sorted(discovered.values(), key=lambda item: item.score, reverse=True)[:12]
    for item in top_seed:
        html = await _fetch_text(item.url)
        if not html.strip():
            continue
        for link in extract_internal_links(html, item.url, max_links=60):
            add_link(link["url"], link.get("anchor", ""), 1)

    ranked = sorted(discovered.values(), key=lambda item: (item.score, -item.depth), reverse=True)
    return ranked[:max_candidates]

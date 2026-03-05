from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

from app.config import Settings
from app.models.enums import SourceMethod
from app.services.extract_candidates import (
    extract_event_focused_text,
    extract_internal_links,
    extract_page_title,
    extract_visible_text,
    sanitize_conference_context_text,
)
from app.services.fetchers import PlaywrightDomainSessionManager, choose_fetch_method, fetch_source

try:
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
except Exception:  # pragma: no cover
    AsyncWebCrawler = None  # type: ignore[assignment]
    CrawlerRunConfig = None  # type: ignore[assignment]


PDF_HINTS = ("pdf", "program", "agenda", "brochure", "abstract")


@dataclass(slots=True)
class CrawlDigestLink:
    url: str
    text: str
    context: str


@dataclass(slots=True)
class CrawlPageResult:
    url: str
    content_type: str
    status: str
    clean_text: str
    title: str
    top_headings: list[str]
    internal_links: list[CrawlDigestLink]
    pdf_links: list[str]
    html_snapshot: str | None
    raw_metadata: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None
    blocked: bool = False
    fetch_method: SourceMethod = SourceMethod.http_static
    used_fallback: bool = False
    fallback_reason: str | None = None


def _registrable_domain(host: str) -> str:
    parts = [p for p in host.lower().split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _same_site(seed_url: str, candidate_url: str) -> bool:
    seed = urlparse(seed_url)
    candidate = urlparse(candidate_url)
    return _registrable_domain(seed.netloc) == _registrable_domain(candidate.netloc)


def _coerce_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    for attr in ("raw_markdown", "markdown", "text"):
        nested = getattr(value, attr, None)
        if isinstance(nested, str) and nested.strip():
            return nested
    return str(value)


def _normalize_content_type(url: str, content_type_raw: str | None) -> str:
    lowered = (content_type_raw or "").lower()
    if "pdf" in lowered or url.lower().endswith(".pdf"):
        return "pdf"
    if "json" in lowered:
        return "json"
    if "html" in lowered or not lowered:
        return "html"
    return "unknown"


def _extract_top_headings(html_snapshot: str | None) -> list[str]:
    if not html_snapshot:
        return []

    # Lightweight heading extraction without adding another parser dependency path here.
    headings: list[str] = []
    for marker in ("<h1", "<h2", "<h3"):
        cursor = 0
        while len(headings) < 12:
            idx = html_snapshot.lower().find(marker, cursor)
            if idx == -1:
                break
            start = html_snapshot.find(">", idx)
            end = html_snapshot.find("</", start + 1) if start != -1 else -1
            if start == -1 or end == -1:
                break
            raw = html_snapshot[start + 1 : end]
            text = " ".join(raw.replace("\n", " ").split()).strip()
            if text:
                headings.append(text[:180])
            cursor = end + 2
    return headings[:12]


def _extract_pdf_links(
    *,
    base_url: str,
    internal_links: list[CrawlDigestLink],
    html_snapshot: str | None,
    seed_url: str,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for link in internal_links:
        lowered = f"{link.url} {link.text}".lower()
        if not any(h in lowered for h in PDF_HINTS):
            continue
        if link.url in seen or not _same_site(seed_url, link.url):
            continue
        seen.add(link.url)
        out.append(link.url)

    if html_snapshot:
        for row in extract_internal_links(html_snapshot, base_url, max_links=200):
            lowered = f"{row['url']} {row['anchor']}".lower()
            if not any(h in lowered for h in PDF_HINTS):
                continue
            if row["url"] in seen or not _same_site(seed_url, row["url"]):
                continue
            seen.add(row["url"])
            out.append(row["url"])

    return out[:40]


def _links_from_html(html_snapshot: str, page_url: str, seed_url: str) -> list[CrawlDigestLink]:
    out: list[CrawlDigestLink] = []
    seen: set[str] = set()
    for row in extract_internal_links(html_snapshot, page_url, max_links=320):
        url = row["url"]
        if url in seen or not _same_site(seed_url, url):
            continue
        seen.add(url)
        out.append(CrawlDigestLink(url=url, text=row.get("anchor", ""), context="html_anchor"))
    return out


def _merge_internal_links(
    *,
    crawl_links: list[CrawlDigestLink],
    html_links: list[CrawlDigestLink],
    seed_url: str,
) -> list[CrawlDigestLink]:
    merged: dict[str, CrawlDigestLink] = {}

    for row in [*crawl_links, *html_links]:
        if not _same_site(seed_url, row.url):
            continue
        existing = merged.get(row.url)
        if existing is None:
            merged[row.url] = row
            continue
        # Prefer richer link labels/context when both sources return the same URL.
        if len((row.text or "").strip()) > len((existing.text or "").strip()):
            merged[row.url] = row
            continue
        if (existing.context or "") == "crawl4ai" and (row.context or "") == "html_anchor":
            merged[row.url] = row

    return list(merged.values())


def _normalize_crawl_links(raw_links: Any, page_url: str, seed_url: str) -> list[CrawlDigestLink]:
    out: list[CrawlDigestLink] = []
    seen: set[str] = set()

    candidates: list[Any]
    if isinstance(raw_links, dict):
        candidates = []
        for key in ("internal", "links", "items"):
            value = raw_links.get(key)
            if isinstance(value, list):
                candidates.extend(value)
    elif isinstance(raw_links, list):
        candidates = raw_links
    else:
        candidates = []

    for item in candidates:
        if isinstance(item, dict):
            href = str(
                item.get("href")
                or item.get("url")
                or item.get("link")
                or ""
            ).strip()
            text = str(item.get("text") or item.get("title") or item.get("anchor") or "").strip()
            context = str(item.get("context") or item.get("source") or "crawl4ai").strip()
        else:
            href = str(item).strip()
            text = ""
            context = "crawl4ai"

        if not href:
            continue
        url = urljoin(page_url, href)
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not _same_site(seed_url, url):
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(CrawlDigestLink(url=url, text=text[:220], context=context[:120]))
        if len(out) >= 120:
            break

    return out


def _is_thin_result(result: CrawlPageResult) -> bool:
    if result.content_type == "pdf":
        return len(result.clean_text.strip()) < 40
    return len(result.clean_text.strip()) < 300 and not result.internal_links and not result.pdf_links


def _sanitize_links(result: CrawlPageResult, seed_url: str) -> None:
    dedup_internal: dict[str, CrawlDigestLink] = {}
    for link in result.internal_links:
        if not _same_site(seed_url, link.url):
            continue
        dedup_internal[link.url] = link
    result.internal_links = list(dedup_internal.values())[:120]

    dedup_pdf: dict[str, None] = {}
    for url in result.pdf_links:
        if _same_site(seed_url, url):
            dedup_pdf[url] = None
    result.pdf_links = list(dedup_pdf.keys())[:40]


def _build_crawler_config(
    *,
    timeout_seconds: float,
    modal_breaker: bool,
    wait_for_selectors: str | None,
    js_code: str | None,
    magic_mode: bool,
):
    if CrawlerRunConfig is None:
        return None

    base_kwargs: dict[str, Any] = {
        "page_timeout": int(timeout_seconds * 1000),
        "scan_full_page": True,
        "verbose": False,
    }
    if modal_breaker:
        if wait_for_selectors:
            base_kwargs["wait_for"] = f"css:{wait_for_selectors.split(',')[0].strip()}"
        if js_code:
            base_kwargs["js_code"] = js_code
        if magic_mode:
            base_kwargs["magic_mode"] = True

    optional_keys = ["magic_mode", "js_code", "wait_for"]
    try_kwargs = dict(base_kwargs)
    while True:
        try:
            return CrawlerRunConfig(**try_kwargs)
        except TypeError:
            removable = [key for key in optional_keys if key in try_kwargs]
            if not removable:
                return CrawlerRunConfig(
                    page_timeout=int(timeout_seconds * 1000),
                    scan_full_page=True,
                    verbose=False,
                )
            try_kwargs.pop(removable[-1], None)


async def _crawl4ai_fetch(
    url: str,
    timeout_seconds: float,
    seed_url: str,
    *,
    modal_breaker: bool = False,
    wait_for_selectors: str | None = None,
    js_code: str | None = None,
    magic_mode: bool = False,
) -> tuple[CrawlPageResult | None, str | None]:
    if AsyncWebCrawler is None:
        return None, "crawl4ai_not_installed"

    try:
        async with AsyncWebCrawler() as crawler:
            if CrawlerRunConfig is not None:
                config = _build_crawler_config(
                    timeout_seconds=timeout_seconds,
                    modal_breaker=modal_breaker,
                    wait_for_selectors=wait_for_selectors,
                    js_code=js_code,
                    magic_mode=magic_mode,
                )
                result = await asyncio.wait_for(crawler.arun(url=url, config=config), timeout=timeout_seconds + 5)
            else:
                result = await asyncio.wait_for(crawler.arun(url=url), timeout=timeout_seconds + 5)
    except asyncio.TimeoutError:
        return None, "crawl4ai_timeout"
    except Exception as exc:  # noqa: BLE001
        return None, f"crawl4ai_error:{type(exc).__name__}"

    success = bool(getattr(result, "success", True))
    if not success:
        return None, "crawl4ai_unsuccessful"

    metadata = getattr(result, "metadata", None)
    metadata_dict = metadata if isinstance(metadata, dict) else {}
    content_type_raw = str(
        metadata_dict.get("content_type")
        or getattr(result, "content_type", "")
        or ""
    )
    content_type = _normalize_content_type(url, content_type_raw)

    html_snapshot = _coerce_text(
        getattr(result, "html", None)
        or getattr(result, "cleaned_html", None)
        or getattr(result, "raw_html", None)
    )
    markdown_v2 = getattr(result, "markdown_v2", None)
    clean_text = _coerce_text(
        getattr(result, "markdown", None)
        or getattr(result, "cleaned_markdown", None)
        or getattr(result, "fit_markdown", None)
        or getattr(markdown_v2, "raw_markdown", None)
        or getattr(result, "text", None)
    )
    if html_snapshot:
        focused_text = extract_event_focused_text(html_snapshot, max_chars=40000)
        if focused_text:
            clean_text = focused_text
    if not clean_text and html_snapshot:
        clean_text = extract_visible_text(html_snapshot, max_chars=20000)
    clean_text = sanitize_conference_context_text(clean_text or "", max_chars=300000)

    title = str(
        metadata_dict.get("title")
        or getattr(result, "title", "")
        or (extract_page_title(html_snapshot) if html_snapshot else "")
    ).strip()

    crawl_links = _normalize_crawl_links(getattr(result, "links", None), url, seed_url)
    html_links = _links_from_html(html_snapshot, url, seed_url) if html_snapshot else []
    internal_links = _merge_internal_links(
        crawl_links=crawl_links,
        html_links=html_links,
        seed_url=seed_url,
    )

    pdf_links = _extract_pdf_links(
        base_url=url,
        internal_links=internal_links,
        html_snapshot=html_snapshot,
        seed_url=seed_url,
    )

    page = CrawlPageResult(
        url=url,
        content_type=content_type,
        status="fetched",
        clean_text=clean_text[:300000],
        title=title[:300],
        top_headings=_extract_top_headings(html_snapshot),
        internal_links=internal_links,
        pdf_links=pdf_links,
        html_snapshot=html_snapshot[:300000] if html_snapshot else None,
        raw_metadata={
            "crawl4ai_used": True,
            "metadata": metadata_dict,
            "modal_breaker": modal_breaker,
        },
        http_status=getattr(result, "status_code", None),
        blocked=False,
        fetch_method=SourceMethod.http_static,
        used_fallback=False,
        fallback_reason=None,
    )
    _sanitize_links(page, seed_url)
    return page, None


async def _fallback_fetch(
    *,
    url: str,
    seed_url: str,
    session_manager: PlaywrightDomainSessionManager | None,
    reason: str,
) -> CrawlPageResult:
    method = choose_fetch_method(url)
    fetch = await fetch_source(url, method, session_manager=session_manager)

    html_snapshot: str | None = fetch.text if fetch.content_type == "html" else None
    clean_text = (
        extract_event_focused_text(fetch.text, max_chars=40000)
        if fetch.content_type == "html"
        else fetch.text
    )
    clean_text = sanitize_conference_context_text(clean_text or "", max_chars=300000)
    title = extract_page_title(html_snapshot) if html_snapshot else ""
    internal_links = _links_from_html(html_snapshot, url, seed_url) if html_snapshot else []
    pdf_links = _extract_pdf_links(
        base_url=url,
        internal_links=internal_links,
        html_snapshot=html_snapshot,
        seed_url=seed_url,
    )

    page = CrawlPageResult(
        url=url,
        content_type=fetch.content_type,
        status="fetched" if fetch.fetch_status == "fetched" else "error",
        clean_text=clean_text[:300000],
        title=title[:300],
        top_headings=_extract_top_headings(html_snapshot),
        internal_links=internal_links,
        pdf_links=pdf_links,
        html_snapshot=html_snapshot[:300000] if html_snapshot else None,
        raw_metadata={
            "crawl4ai_used": False,
            "fallback_method": method.value,
            "fetch_status": fetch.fetch_status,
            "network_payloads": fetch.network_payloads,
            "fallback_reason": reason,
        },
        http_status=fetch.http_status,
        blocked=fetch.blocked or fetch.fetch_status == "blocked",
        fetch_method=method,
        used_fallback=True,
        fallback_reason=reason,
    )
    _sanitize_links(page, seed_url)
    return page


async def fetch_crawl_page(
    settings: Settings,
    *,
    url: str,
    depth: int,
    seed_url: str,
    session_manager: PlaywrightDomainSessionManager | None = None,
    modal_breaker: bool = False,
    wait_for_selectors: str | None = None,
    js_code: str | None = None,
    magic_mode: bool | None = None,
) -> CrawlPageResult:
    _ = depth
    if not _same_site(seed_url, url):
        return CrawlPageResult(
            url=url,
            content_type="unknown",
            status="error",
            clean_text="",
            title="",
            top_headings=[],
            internal_links=[],
            pdf_links=[],
            html_snapshot=None,
            raw_metadata={"crawl4ai_used": False, "reason": "out_of_domain"},
            http_status=None,
            blocked=False,
            fetch_method=SourceMethod.http_static,
            used_fallback=False,
            fallback_reason="out_of_domain",
        )

    if not settings.crawl4ai_enabled:
        return await _fallback_fetch(
            url=url,
            seed_url=seed_url,
            session_manager=session_manager,
            reason="crawl4ai_disabled",
        )

    if magic_mode is None:
        magic_mode = bool(settings.modal_breaker_magic_mode)
    crawled, error_reason = await _crawl4ai_fetch(
        url,
        float(settings.crawl4ai_timeout_seconds),
        seed_url,
        modal_breaker=modal_breaker,
        wait_for_selectors=wait_for_selectors,
        js_code=js_code,
        magic_mode=bool(magic_mode),
    )
    if crawled is not None and not _is_thin_result(crawled):
        return crawled

    reason = "crawl4ai_thin_content" if crawled is not None else (error_reason or "crawl4ai_failed")
    return await _fallback_fetch(
        url=url,
        seed_url=seed_url,
        session_manager=session_manager,
        reason=reason,
    )

from __future__ import annotations

import json
import re
from typing import Any, Literal
from urllib.parse import urljoin, urlparse

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
from app.services.llm_response import extract_json_object, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout


PATH_BREAK_RE = re.compile(r"[^a-z0-9]+")
COMMON_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "this",
    "that",
    "your",
    "about",
    "home",
    "menu",
    "more",
    "page",
    "view",
    "read",
    "click",
    "here",
    "link",
    "site",
    "annual",
    "meeting",
    "conference",
    "conferences",
}
CONTENT_SIGNAL_TOKENS = {
    "program",
    "agenda",
    "session",
    "speaker",
    "faculty",
    "presenter",
    "plenary",
    "abstract",
    "schedule",
    "workshop",
    "symposium",
}


def _extract_json_obj(content: str) -> dict[str, Any] | None:
    return extract_json_object(content)


class NavigationCandidate(BaseModel):
    url: str
    reason: str = "llm_selected"
    priority: float = Field(default=0.7, ge=0.0, le=1.0)
    page_type: str = "unknown"
    branch_hint: str | None = None
    expected_yield: float | None = Field(default=None, ge=0.0, le=1.0)


class NavigationDecisionDebug(BaseModel):
    stage: Literal["nav_decide"] = "nav_decide"
    used_llm: bool = False
    success: bool = True
    used_fallback: bool = False
    fallback_reason: str | None = None
    llm_attempts: int = 0
    llm_failures: int = 0
    llm_http_failures: int = 0
    llm_parse_failures: int = 0
    selected_model: str | None = None
    selected_timeout_seconds: float | None = None


class NavigationDecisionResult(BaseModel):
    next_urls: list[NavigationCandidate] = Field(default_factory=list)
    stop: bool = False
    stop_reason: str | None = None
    extraction_hint: str | None = None
    debug: NavigationDecisionDebug = Field(default_factory=NavigationDecisionDebug)


def _registrable_domain(host: str) -> str:
    parts = [p for p in host.lower().split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _same_site(seed_url: str, candidate_url: str) -> bool:
    a = urlparse(seed_url)
    b = urlparse(candidate_url)
    return _registrable_domain(a.netloc) == _registrable_domain(b.netloc)


def _normalize_url(base_url: str, candidate: str) -> str | None:
    value = (candidate or "").strip()
    if not value:
        return None
    url = urljoin(base_url, value)
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None
    return url


def _tokenize(text: str) -> list[str]:
    def _stem(token: str) -> str:
        if len(token) > 6 and token.endswith("ing"):
            stemmed = token[:-3]
            if stemmed.endswith("mm"):
                stemmed = stemmed[:-1]
            return stemmed
        if len(token) > 5 and token.endswith("ers"):
            return token[:-3]
        if len(token) > 4 and token.endswith("es"):
            return token[:-2]
        if len(token) > 4 and token.endswith("s"):
            return token[:-1]
        if len(token) > 5 and token.endswith("ed"):
            return token[:-2]
        return token

    cleaned = re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
    out: list[str] = []
    for raw_token in cleaned.split():
        token = _stem(raw_token)
        variants = [raw_token]
        if token != raw_token:
            variants.append(token)
        for variant in variants:
            if len(variant) < 3:
                continue
            if variant in COMMON_STOPWORDS:
                continue
            if variant.isdigit():
                continue
            out.append(variant)
    return out


def _path_tokens(url: str) -> set[str]:
    path = urlparse(url).path.lower()
    tokens: set[str] = set()
    for raw in path.split("/"):
        if not raw:
            continue
        for raw_token in PATH_BREAK_RE.split(raw):
            token = raw_token.strip()
            if not token:
                continue
            variants = [token]
            stemmed = token
            if stemmed.endswith("ing") and len(stemmed) > 6:
                stemmed = stemmed[:-3]
                if stemmed.endswith("mm"):
                    stemmed = stemmed[:-1]
            elif stemmed.endswith("s") and len(stemmed) > 4:
                stemmed = stemmed[:-1]
            if stemmed != token:
                variants.append(stemmed)
            for variant in variants:
                if len(variant) < 3:
                    continue
                if variant in COMMON_STOPWORDS:
                    continue
                tokens.add(variant)
    return tokens


def _path_segments(url: str) -> list[str]:
    return [segment for segment in urlparse(url).path.lower().split("/") if segment]


def _template_hint(url: str) -> str:
    segments: list[str] = []
    for part in _path_segments(url):
        if part.isdigit():
            segments.append("{num}")
        elif re.match(r"^[0-9a-f-]{8,}$", part):
            segments.append("{id}")
        else:
            segments.append(part)
        if len(segments) >= 5:
            break
    if not segments:
        return "/"
    return "/" + "/".join(segments)


def _focus_tokens(
    *,
    seed_url: str,
    page_url: str,
    title: str,
    top_headings: list[str],
    summary_text: str,
) -> set[str]:
    tokens: list[str] = []
    tokens.extend(_path_tokens(seed_url))
    tokens.extend(_path_tokens(page_url))
    tokens.extend(_tokenize(title))
    for heading in top_headings[:12]:
        tokens.extend(_tokenize(heading))
    tokens.extend(_tokenize(summary_text[:1800]))

    counts: dict[str, int] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda row: row[1], reverse=True)
    return {token for token, _ in ranked[:50]}


def _context_similarity(
    *,
    seed_url: str,
    page_url: str,
    candidate_url: str,
    text: str,
    context: str,
    focus_tokens: set[str],
    is_pdf: bool,
) -> float:
    candidate_tokens = _path_tokens(candidate_url)
    candidate_tokens.update(_tokenize(text))
    candidate_tokens.update(_tokenize(context))
    if not candidate_tokens:
        candidate_tokens.update(_path_tokens(candidate_url))

    shared_focus = len(candidate_tokens.intersection(focus_tokens))
    text_focus = len(set(_tokenize(text)).intersection(focus_tokens))
    context_focus = len(set(_tokenize(context)).intersection(focus_tokens))
    seed_overlap = len(_path_tokens(candidate_url).intersection(_path_tokens(seed_url)))
    page_overlap = len(_path_tokens(candidate_url).intersection(_path_tokens(page_url)))
    segment_depth = len(_path_segments(candidate_url))

    score = 0.0
    score += float(shared_focus) * 2.2
    score += float(text_focus) * 1.6
    score += float(context_focus) * 0.8
    score += float(seed_overlap) * 0.8
    score += float(page_overlap) * 0.55
    score -= min(2.0, float(segment_depth) * 0.2)
    if is_pdf:
        score += 1.0
    if urlparse(candidate_url).query:
        score -= 0.5
    if text.strip():
        score += 0.8
    if context.lower() == "html_anchor":
        score += 3.0
    return score


def _template_key(url: str) -> str:
    parts: list[str] = []
    for segment in _path_segments(url):
        if segment.isdigit():
            parts.append("{num}")
        elif re.match(r"^[0-9a-f-]{8,}$", segment):
            parts.append("{id}")
        else:
            parts.append(segment)
        if len(parts) >= 6:
            break
    return "/" + "/".join(parts) if parts else "/"


def _scored_link_pool(
    *,
    page_url: str,
    seed_url: str,
    focus_tokens: set[str],
    links: list[dict[str, str]],
    pdf_links: list[dict[str, str]],
    visited_urls: set[str],
) -> list[tuple[float, NavigationCandidate]]:
    visited_normalized = {
        normalized
        for visited in visited_urls
        if (normalized := _normalize_url(page_url, str(visited)))
    }
    seen: set[str] = set()
    scored: list[tuple[float, NavigationCandidate]] = []

    def _append_row(row: dict[str, str], *, is_pdf: bool) -> None:
        normalized = _normalize_url(page_url, str(row.get("url", "")))
        if not normalized or not _same_site(seed_url, normalized):
            return
        if normalized in visited_normalized or normalized in seen:
            return
        seen.add(normalized)

        text = str(row.get("text", "")).strip()
        context = str(row.get("context", "")).strip()
        score = _context_similarity(
            seed_url=seed_url,
            page_url=page_url,
            candidate_url=normalized,
            text=text,
            context=context,
            focus_tokens=focus_tokens,
            is_pdf=is_pdf,
        )

        # Boost links with explicit descriptive anchor text because these are
        # usually content links vs global nav chrome.
        if len(_tokenize(text)) >= 2:
            score += 0.9
        elif len(_tokenize(text)) == 1:
            score += 0.4

        # Prefer new branch templates when semantic relevance is comparable.
        score += min(0.9, 0.15 * float(len(_path_segments(normalized))))

        candidate = NavigationCandidate(
            url=normalized,
            reason=(text[:220] if text else "fallback_context_similarity"),
            priority=max(0.2, min(0.95, 0.4 + (score / 20.0))),
            page_type="pdf" if is_pdf else "unknown",
            branch_hint=_template_hint(normalized),
            expected_yield=max(0.1, min(1.0, 0.25 + (score / 18.0))),
        )
        scored.append((score, candidate))

    for row in pdf_links:
        _append_row(row, is_pdf=True)
    for row in links:
        _append_row(row, is_pdf=False)

    scored.sort(key=lambda row: row[0], reverse=True)
    return scored


def _diversify_candidates(
    *,
    scored: list[tuple[float, NavigationCandidate]],
    max_items: int,
) -> list[NavigationCandidate]:
    if not scored:
        return []

    best_score = scored[0][0]
    score_cutoff = max(2.0, best_score * 0.2)
    filtered = [row for row in scored if row[0] >= score_cutoff]
    pool = filtered if len(filtered) >= min(3, max_items) else scored

    by_template: dict[str, NavigationCandidate] = {}
    ordered_all: list[NavigationCandidate] = []
    for _, item in pool:
        ordered_all.append(item)
        template = _template_key(item.url)
        if template not in by_template:
            by_template[template] = item

    selected: list[NavigationCandidate] = list(by_template.values())[:max_items]
    selected_urls = {item.url for item in selected}
    if len(selected) >= max_items:
        return selected[:max_items]

    for item in ordered_all:
        if item.url in selected_urls:
            continue
        selected.append(item)
        selected_urls.add(item.url)
        if len(selected) >= max_items:
            break
    return selected[:max_items]


def _path_prefix(url: str, depth: int = 2) -> str:
    segments = [segment for segment in _path_segments(url) if segment]
    if not segments:
        return "/"
    return "/" + "/".join(segments[:depth])


def _ensure_cross_prefix_coverage(
    *,
    selected: list[NavigationCandidate],
    scored: list[tuple[float, NavigationCandidate]],
    seed_url: str,
    page_url: str,
    max_items: int,
) -> list[NavigationCandidate]:
    if not selected or not scored or max_items <= 0:
        return selected[:max_items]
    if len(selected) <= max(3, max_items // 2):
        return selected[:max_items]

    core_prefixes = {_path_prefix(seed_url), _path_prefix(page_url)}
    cross_prefix_rows: list[tuple[float, NavigationCandidate]] = []
    for score, item in scored:
        if _path_prefix(item.url) in core_prefixes:
            continue
        reason_tokens = set(_tokenize(item.reason or ""))
        signal_bonus = 1.2 if reason_tokens.intersection(CONTENT_SIGNAL_TOKENS) else 0.0
        adjusted = score + (0.2 * float(len(_path_segments(item.url)))) + signal_bonus
        cross_prefix_rows.append((adjusted, item))
    if not cross_prefix_rows:
        return selected[:max_items]
    cross_prefix_rows.sort(key=lambda row: row[0], reverse=True)

    reserve_slots = min(2, max(1, max_items // 6), len(cross_prefix_rows))
    target_cross_urls = [item.url for _, item in cross_prefix_rows[:reserve_slots]]
    signal_cross_urls = [
        item.url
        for _, item in cross_prefix_rows
        if set(_tokenize(item.reason or "")).intersection(CONTENT_SIGNAL_TOKENS)
    ]
    if signal_cross_urls:
        preferred_signal = signal_cross_urls[0]
        if preferred_signal not in target_cross_urls:
            if target_cross_urls:
                target_cross_urls[-1] = preferred_signal
            else:
                target_cross_urls.append(preferred_signal)

    chosen: list[NavigationCandidate] = selected[:max_items]
    chosen_urls = {item.url for item in chosen}
    missing_cross = [url for url in target_cross_urls if url not in chosen_urls]
    if not missing_cross:
        return chosen[:max_items]

    by_url: dict[str, NavigationCandidate] = {item.url: item for _, item in scored}
    replacements = [by_url[url] for url in missing_cross if url in by_url]
    if not replacements:
        return selected[:max_items]

    # Replace low-priority core-prefix items from the tail to keep queue size stable.
    for replacement in replacements:
        if replacement.url in chosen_urls:
            continue
        replace_index = None
        for idx in range(len(chosen) - 1, -1, -1):
            candidate = chosen[idx]
            if _path_prefix(candidate.url) in core_prefixes:
                replace_index = idx
                break
        if replace_index is None:
            if len(chosen) < max_items:
                chosen.append(replacement)
                chosen_urls.add(replacement.url)
            continue
        chosen.pop(replace_index)
        chosen.append(replacement)
        chosen_urls.add(replacement.url)

    dedup: dict[str, NavigationCandidate] = {}
    for item in chosen:
        dedup[item.url] = item
    return list(dedup.values())[:max_items]


def _prepare_links_for_llm(
    *,
    page_url: str,
    seed_url: str,
    title: str,
    top_headings: list[str],
    summary_text: str,
    links: list[dict[str, str]],
    pdf_links: list[dict[str, str]],
    visited_urls: set[str],
    max_items: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    focus_tokens = _focus_tokens(
        seed_url=seed_url,
        page_url=page_url,
        title=title,
        top_headings=top_headings,
        summary_text=summary_text,
    )
    scored = _scored_link_pool(
        page_url=page_url,
        seed_url=seed_url,
        focus_tokens=focus_tokens,
        links=links,
        pdf_links=pdf_links,
        visited_urls=visited_urls,
    )
    diversified = _diversify_candidates(scored=scored, max_items=max_items)
    diversified = _ensure_cross_prefix_coverage(
        selected=diversified,
        scored=scored,
        seed_url=seed_url,
        page_url=page_url,
        max_items=max_items,
    )

    source_by_url: dict[str, dict[str, str]] = {}
    for row in [*links, *pdf_links]:
        normalized = _normalize_url(page_url, str(row.get("url", "")))
        if not normalized:
            continue
        source_by_url[normalized] = {
            "text": str(row.get("text", ""))[:240],
            "context": str(row.get("context", ""))[:180],
        }

    llm_links: list[dict[str, str]] = []
    llm_pdfs: list[dict[str, str]] = []
    for item in diversified:
        source = source_by_url.get(item.url, {})
        row = {
            "url": item.url,
            "text": source.get("text", ""),
            "context": source.get("context", item.branch_hint or "ranked"),
        }
        if item.page_type == "pdf":
            llm_pdfs.append(row)
        else:
            llm_links.append(row)

    if not llm_links and not llm_pdfs:
        llm_links = links[:max_items]
        llm_pdfs = pdf_links[: max(1, max_items // 3)]

    return llm_links[:max_items], llm_pdfs[: max(1, max_items // 2)]


def _fallback_select(
    *,
    page_url: str,
    seed_url: str,
    title: str,
    top_headings: list[str],
    summary_text: str,
    links: list[dict[str, str]],
    pdf_links: list[dict[str, str]],
    visited_urls: set[str],
    max_next_urls: int,
    reason: str,
    llm_attempts: int = 0,
    llm_failures: int = 0,
    llm_http_failures: int = 0,
    llm_parse_failures: int = 0,
) -> NavigationDecisionResult:
    focus_tokens = _focus_tokens(
        seed_url=seed_url,
        page_url=page_url,
        title=title,
        top_headings=top_headings,
        summary_text=summary_text,
    )
    scored = _scored_link_pool(
        page_url=page_url,
        seed_url=seed_url,
        focus_tokens=focus_tokens,
        links=links,
        pdf_links=pdf_links,
        visited_urls=visited_urls,
    )
    ranked_candidates = _diversify_candidates(scored=scored, max_items=max_next_urls)
    ranked_candidates = _ensure_cross_prefix_coverage(
        selected=ranked_candidates,
        scored=scored,
        seed_url=seed_url,
        page_url=page_url,
        max_items=max_next_urls,
    )

    return NavigationDecisionResult(
        next_urls=ranked_candidates,
        stop=False,
        stop_reason=None,
        extraction_hint="fallback_context_selection",
        debug=NavigationDecisionDebug(
            used_llm=llm_attempts > 0,
            success=False,
            used_fallback=True,
            fallback_reason=reason,
            llm_attempts=llm_attempts,
            llm_failures=llm_failures,
            llm_http_failures=llm_http_failures,
            llm_parse_failures=llm_parse_failures,
        ),
    )


def _sanitize_decision(
    *,
    page_url: str,
    seed_url: str,
    decision: NavigationDecisionResult,
    visited_urls: set[str],
    max_next_urls: int,
) -> NavigationDecisionResult:
    visited_normalized = {
        normalized
        for visited in visited_urls
        if (normalized := _normalize_url(page_url, str(visited)))
    }
    dedup: dict[str, NavigationCandidate] = {}
    for item in decision.next_urls:
        normalized = _normalize_url(page_url, item.url)
        if not normalized:
            continue
        if not _same_site(seed_url, normalized):
            continue
        if normalized in visited_normalized:
            continue
        dedup[normalized] = NavigationCandidate(
            url=normalized,
            reason=item.reason[:300] if item.reason else "llm_selected",
            priority=float(item.priority),
            page_type=(item.page_type or "unknown")[:64],
            branch_hint=(item.branch_hint or _template_hint(normalized))[:120],
            expected_yield=max(0.0, min(1.0, float(item.expected_yield if item.expected_yield is not None else item.priority))),
        )
        if len(dedup) >= max_next_urls:
            break

    decision.next_urls = list(dedup.values())
    return decision


async def decide_next(
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
    max_next_urls_override: int | None = None,
    llm_retry_override: int | None = None,
) -> NavigationDecisionResult:
    configured_max_next_urls = max_next_urls_override if max_next_urls_override is not None else int(settings.nav_max_next_urls)
    max_next_urls = max(1, int(configured_max_next_urls))
    compact_summary = summary_text[: max(300, int(settings.nav_summary_text_chars))]
    prepared_links, prepared_pdf_links = _prepare_links_for_llm(
        page_url=page_url,
        seed_url=seed_url,
        title=title,
        top_headings=top_headings,
        summary_text=compact_summary,
        links=links,
        pdf_links=pdf_links,
        visited_urls=visited_urls,
        max_items=max(24, max_next_urls * 8),
    )

    if not settings.deepseek_api_key:
        return _fallback_select(
            page_url=page_url,
            seed_url=seed_url,
            title=title,
            top_headings=top_headings,
            summary_text=compact_summary,
            links=prepared_links or links,
            pdf_links=prepared_pdf_links or pdf_links,
            visited_urls=visited_urls,
            max_next_urls=max_next_urls,
            reason="missing_api_key",
        )

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="navigation")
    selected_timeout = select_llm_timeout(settings, stage="navigation", default_timeout_seconds=30.0)
    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

    payload = {
        "page_url": page_url,
        "title": title,
        "top_headings": top_headings[:12],
        "summary_text": compact_summary,
        "links": prepared_links[:120],
        "pdf_links": prepared_pdf_links[:40],
        "current_physician_like_count": current_physician_like_count,
        "remaining_page_budget": max(0, remaining_page_budget),
        "remaining_depth": max(0, remaining_depth),
        "visited_urls": list(visited_urls)[:300],
        "frontier_context": frontier_context or {},
        "branch_stats": branch_stats or {},
        "tried_templates": (tried_templates or [])[:120],
        "page_novelty": page_novelty or {},
        "rules": {
            "same_domain_only": True,
            "max_next_urls": max_next_urls,
            "conference_context_consistency": True,
            "allow_stop_true_only_if_confident": True,
            "return_branch_hint_and_expected_yield": True,
        },
    }

    system_prompt = (
        "You are a navigation planner for conference speaker scraping. "
        "Choose next URLs only from links/pdf_links and decide whether to stop. "
        "Stay focused on the same conference context as the seed URL and current page content. "
        "Prioritize conference-year lineage pages (for example same-series pages for earlier years) over unrelated site sections. "
        "Use frontier_context.seed_year and frontier_context.series_focus_tokens to keep year coverage broad but on-topic. "
        "Select links that are most likely to add new person/session evidence, not generic site navigation. "
        "Use semantic overlap from title/headings/summary/link text/context rather than URL pattern guessing. "
        "For each next_urls item include branch_hint (template style hint) and expected_yield (0-1). "
        "Return strict JSON with keys: next_urls, stop, stop_reason, extraction_hint."
    )

    configured_retry_count = llm_retry_override if llm_retry_override is not None else int(settings.nav_llm_retry_count)
    retry_count = max(1, int(configured_retry_count))
    async with httpx.AsyncClient(timeout=selected_timeout) as client:
        for attempt in range(retry_count):
            llm_attempts += 1
            user_content = json.dumps(payload, ensure_ascii=True)
            if attempt > 0:
                user_content += "\nPrevious output invalid. Return schema-valid JSON only."
            try:
                response = await client.post(
                    endpoint,
                    headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
                    json={
                        "model": selected_model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_content},
                        ],
                        "response_format": {"type": "json_object"},
                        "temperature": 0,
                    },
                )
            except httpx.HTTPError:
                llm_failures += 1
                llm_http_failures += 1
                continue

            if response.status_code >= 400:
                llm_failures += 1
                llm_http_failures += 1
                continue

            try:
                body = response.json()
                content = extract_message_text(body)
                parsed = _extract_json_obj(str(content))
                if not parsed:
                    raise json.JSONDecodeError("No valid JSON object in model output", str(content), 0)
                validated = NavigationDecisionResult.model_validate(parsed)
                validated = _sanitize_decision(
                    page_url=page_url,
                    seed_url=seed_url,
                    decision=validated,
                    visited_urls=visited_urls,
                    max_next_urls=max_next_urls,
                )
                validated.debug = NavigationDecisionDebug(
                    used_llm=True,
                    success=True,
                    used_fallback=False,
                    llm_attempts=llm_attempts,
                    llm_failures=llm_failures,
                    llm_http_failures=llm_http_failures,
                    llm_parse_failures=llm_parse_failures,
                    selected_model=selected_model,
                    selected_timeout_seconds=selected_timeout,
                )
                if not validated.stop and not validated.next_urls:
                    fallback = _fallback_select(
                        page_url=page_url,
                        seed_url=seed_url,
                        title=title,
                        top_headings=top_headings,
                        summary_text=compact_summary,
                        links=prepared_links or links,
                        pdf_links=prepared_pdf_links or pdf_links,
                        visited_urls=visited_urls,
                        max_next_urls=max_next_urls,
                        reason="llm_empty_selection",
                        llm_attempts=llm_attempts,
                        llm_failures=llm_failures,
                        llm_http_failures=llm_http_failures,
                        llm_parse_failures=llm_parse_failures,
                    )
                    fallback.debug.selected_model = selected_model
                    fallback.debug.selected_timeout_seconds = selected_timeout
                    return fallback
                return validated
            except (KeyError, TypeError, json.JSONDecodeError, ValidationError):
                llm_failures += 1
                llm_parse_failures += 1
                continue

    fallback = _fallback_select(
        page_url=page_url,
        seed_url=seed_url,
        title=title,
        top_headings=top_headings,
        summary_text=compact_summary,
        links=prepared_links or links,
        pdf_links=prepared_pdf_links or pdf_links,
        visited_urls=visited_urls,
        max_next_urls=max_next_urls,
        reason="llm_invalid_or_unavailable",
        llm_attempts=llm_attempts,
        llm_failures=llm_failures,
        llm_http_failures=llm_http_failures,
        llm_parse_failures=llm_parse_failures,
    )
    fallback.debug.selected_model = selected_model
    fallback.debug.selected_timeout_seconds = selected_timeout
    return fallback

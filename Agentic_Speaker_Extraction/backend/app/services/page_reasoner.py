from __future__ import annotations

import json
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
from app.services.extract_llm import AttributionTargetHint, ExtractedSpeaker
from app.services.llm_response import extract_json_object, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout
from app.services.navigation_llm import NavigationCandidate


class SegmentDebug(BaseModel):
    segment_index: int
    segment_total: int
    chars: int
    duration_ms: int = 0
    success: bool = False
    speaker_count: int = 0
    next_link_count: int = 0
    error: str | None = None


class PageReasonerDebug(BaseModel):
    used_llm: bool = False
    success: bool = False
    used_fallback: bool = False
    fallback_reason: str | None = None
    llm_attempts: int = 0
    llm_failures: int = 0
    llm_http_failures: int = 0
    llm_parse_failures: int = 0
    selected_model: str | None = None
    selected_timeout_seconds: float | None = None
    segments_used: int = 0
    markdown_chars: int = 0


class PageReasonerResult(BaseModel):
    speakers: list[ExtractedSpeaker] = Field(default_factory=list)
    next_links: list[NavigationCandidate] = Field(default_factory=list)
    stop: bool = False
    stop_reason: str | None = None
    segment_debug: list[SegmentDebug] = Field(default_factory=list)
    markdown_candidates: list[dict[str, Any]] = Field(default_factory=list)
    debug: PageReasonerDebug = Field(default_factory=PageReasonerDebug)


def _clean_text(value: Any) -> str:
    text = str(value or "").replace("\x00", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _canonical_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "https").lower()
    host = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return urlunparse((scheme, host, path, "", parsed.query, ""))


def _registrable_domain(host: str) -> str:
    parts = [p for p in host.lower().split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _same_site(seed_url: str, candidate_url: str) -> bool:
    return _registrable_domain(urlparse(seed_url).netloc) == _registrable_domain(urlparse(candidate_url).netloc)


def _segment_markdown(
    text: str,
    *,
    max_chars: int,
    segment_chars: int,
    segment_overlap: int,
    max_segments: int,
) -> list[str]:
    value = text[: max(1000, int(max_chars) * max(1, int(max_segments)))]
    if len(value) <= max_chars:
        return [value]

    segments: list[str] = []
    cursor = 0
    step = max(500, int(segment_chars) - max(0, int(segment_overlap)))
    while cursor < len(value) and len(segments) < max(1, int(max_segments)):
        chunk = value[cursor : cursor + int(segment_chars)]
        if not chunk.strip():
            break
        segments.append(chunk)
        if cursor + int(segment_chars) >= len(value):
            break
        cursor += step
    return segments or [value[:max_chars]]


def _coerce_speaker(raw: dict[str, Any]) -> ExtractedSpeaker | None:
    full_name = _clean_text(raw.get("full_name") or raw.get("name"))
    if not full_name:
        return None

    aliases: list[str] = []
    aliases_raw = raw.get("aliases")
    if isinstance(aliases_raw, list):
        aliases = [_clean_text(item)[:140] for item in aliases_raw if _clean_text(item)]

    confidence = 0.65
    try:
        confidence = float(raw.get("confidence", 0.65))
    except (TypeError, ValueError):
        confidence = 0.65
    confidence = _clamp(confidence)

    targets: list[AttributionTargetHint] = []
    raw_targets = raw.get("attribution_targets")
    if isinstance(raw_targets, list):
        for item in raw_targets:
            if not isinstance(item, dict):
                continue
            try:
                targets.append(AttributionTargetHint.model_validate(item))
            except ValidationError:
                continue

    try:
        return ExtractedSpeaker(
            full_name=full_name,
            designation=_clean_text(raw.get("designation"))[:80] or None,
            affiliation=_clean_text(raw.get("affiliation"))[:180] or None,
            location=_clean_text(raw.get("location"))[:120] or None,
            role=_clean_text(raw.get("role"))[:120] or None,
            session_title=_clean_text(raw.get("session_title"))[:240] or None,
            talk_brief_extracted=_clean_text(raw.get("talk_brief_extracted"))[:600] or None,
            aliases=aliases[:10],
            is_physician_candidate=bool(raw.get("is_physician_candidate", True)),
            confidence=confidence,
            evidence_span=_clean_text(raw.get("evidence_span"))[:900] or None,
            attribution_targets=targets[:8],
        )
    except ValidationError:
        return None


def _coerce_link(
    *,
    raw: dict[str, Any],
    base_url: str,
    seed_url: str,
    allowed_urls: set[str],
) -> NavigationCandidate | None:
    raw_url = _clean_text(raw.get("url"))
    if not raw_url:
        return None
    url = urljoin(base_url, raw_url)
    if not _same_site(seed_url, url):
        return None
    canonical = _canonical_url(url)
    if allowed_urls and canonical not in allowed_urls:
        return None

    try:
        priority = float(raw.get("priority", 0.6))
    except (TypeError, ValueError):
        priority = 0.6
    expected_yield_raw = raw.get("expected_yield")
    expected_yield: float | None
    if expected_yield_raw is None:
        expected_yield = None
    else:
        try:
            expected_yield = _clamp(float(expected_yield_raw))
        except (TypeError, ValueError):
            expected_yield = None

    return NavigationCandidate(
        url=url,
        reason=_clean_text(raw.get("reason"))[:220] or "markdown_reasoner",
        priority=_clamp(priority),
        page_type=_clean_text(raw.get("intent") or raw.get("page_type"))[:48] or "unknown",
        branch_hint=_clean_text(raw.get("branch_hint"))[:180] or None,
        expected_yield=expected_yield,
    )


async def extract_and_decide(
    settings: Settings,
    *,
    seed_url: str,
    page_url: str,
    title: str,
    top_headings: list[str],
    markdown_text: str,
    internal_links: list[dict[str, str]],
    pdf_links: list[dict[str, str]],
    conference_context: dict[str, Any],
    visited_urls: set[str],
    max_next_urls: int,
) -> PageReasonerResult:
    markdown = _clean_text(markdown_text)
    if not markdown:
        return PageReasonerResult(
            debug=PageReasonerDebug(
                used_llm=False,
                success=False,
                used_fallback=True,
                fallback_reason="empty_markdown",
            )
        )

    if not settings.deepseek_api_key:
        return PageReasonerResult(
            debug=PageReasonerDebug(
                used_llm=False,
                success=False,
                used_fallback=True,
                fallback_reason="missing_api_key",
            )
        )

    selected_model = select_llm_model(settings, stage="extraction")
    selected_timeout = select_llm_timeout(
        settings,
        stage="extraction",
        default_timeout_seconds=max(float(getattr(settings, "llm_request_timeout_seconds", 120) or 120), 10.0),
    )

    segments = _segment_markdown(
        markdown,
        max_chars=max(1000, int(settings.markdown_reasoner_max_chars)),
        segment_chars=max(1000, int(settings.markdown_segment_chars)),
        segment_overlap=max(0, int(settings.markdown_segment_overlap)),
        max_segments=max(1, int(settings.markdown_segment_max)),
    )

    allowed_urls = {
        _canonical_url(urljoin(page_url, str(row.get("url", ""))))
        for row in [*internal_links, *pdf_links]
        if str(row.get("url", "")).strip()
    }

    system_prompt = (
        "You are a conference speaker extraction and navigation engine. "
        "Use ONLY the provided markdown and links. "
        "Return strict JSON with keys: speakers, next_links, stop, stop_reason. "
        "speakers: extract real people only from agenda/program style text. "
        "Agenda lines may look like 'Session Title ... Person Name'; extract the person as full_name and keep the session title separately. "
        "Keep full_name as person-only tokens, move credentials to designation, and preserve session_title when available. "
        "If speaker names appear inline after session titles, extract them. "
        "next_links: choose URLs only from provided links/pdf_links that are most likely to add speaker/session evidence and past-year conference lineage. "
        "Prefer links semantically aligned to conference archives/programs/abstracts/sessions and avoid utility pages. "
        "Prefer conference archive/year pages over housing/travel/sponsor/news pages. "
        "Never hallucinate fields. Use null for unknowns and [] for empty arrays."
    )

    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0
    segment_debug: list[SegmentDebug] = []
    speakers: list[ExtractedSpeaker] = []
    next_links: list[NavigationCandidate] = []
    stop_votes: list[bool] = []
    stop_reasons: list[str] = []

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"

    async with httpx.AsyncClient(timeout=selected_timeout) as client:
        for idx, segment in enumerate(segments, start=1):
            seg_dbg = SegmentDebug(segment_index=idx, segment_total=len(segments), chars=len(segment))
            started = time.monotonic()
            parsed: dict[str, Any] | None = None

            payload = {
                "page_url": page_url,
                "title": title,
                "top_headings": top_headings[:12],
                "segment_index": idx,
                "segment_total": len(segments),
                "markdown_segment": segment,
                "links": internal_links[:120],
                "pdf_links": pdf_links[:40],
                "conference_context": conference_context,
                "visited_urls": list(visited_urls)[:300],
                "max_next_urls": max(1, int(max_next_urls)),
            }

            for attempt in range(2):
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
                    parsed = extract_json_object(content)
                    if not parsed:
                        raise json.JSONDecodeError("invalid json", content, 0)
                    break
                except (json.JSONDecodeError, TypeError, ValueError):
                    llm_failures += 1
                    llm_parse_failures += 1
                    parsed = None
                    continue

            seg_dbg.duration_ms = int((time.monotonic() - started) * 1000)

            if not parsed:
                seg_dbg.success = False
                seg_dbg.error = "llm_parse_failed"
                segment_debug.append(seg_dbg)
                continue

            raw_speakers = parsed.get("speakers")
            if isinstance(raw_speakers, list):
                for row in raw_speakers:
                    if not isinstance(row, dict):
                        continue
                    coerced = _coerce_speaker(row)
                    if coerced is not None:
                        speakers.append(coerced)

            raw_links = parsed.get("next_links")
            if isinstance(raw_links, list):
                for row in raw_links:
                    if not isinstance(row, dict):
                        continue
                    coerced_link = _coerce_link(
                        raw=row,
                        base_url=page_url,
                        seed_url=seed_url,
                        allowed_urls=allowed_urls,
                    )
                    if coerced_link is not None:
                        next_links.append(coerced_link)

            stop_votes.append(bool(parsed.get("stop", False)))
            stop_reason = _clean_text(parsed.get("stop_reason"))
            if stop_reason:
                stop_reasons.append(stop_reason)

            seg_dbg.success = True
            seg_dbg.speaker_count = len(raw_speakers) if isinstance(raw_speakers, list) else 0
            seg_dbg.next_link_count = len(raw_links) if isinstance(raw_links, list) else 0
            segment_debug.append(seg_dbg)

    dedup_speakers: dict[tuple[str, str], ExtractedSpeaker] = {}
    for speaker in speakers:
        key = ((speaker.full_name or "").strip().lower(), (speaker.session_title or "").strip().lower())
        existing = dedup_speakers.get(key)
        if existing is None or (speaker.confidence or 0) > (existing.confidence or 0):
            dedup_speakers[key] = speaker

    dedup_links: dict[str, NavigationCandidate] = {}
    for link in next_links:
        canonical = _canonical_url(link.url)
        existing = dedup_links.get(canonical)
        if existing is None or float(link.priority) > float(existing.priority):
            dedup_links[canonical] = link

    ordered_links = sorted(dedup_links.values(), key=lambda row: float(row.priority), reverse=True)[: max(1, int(max_next_urls))]
    successful_segments = [row for row in segment_debug if row.success]
    success = bool(successful_segments)

    if not success:
        return PageReasonerResult(
            speakers=[],
            next_links=[],
            stop=False,
            stop_reason=None,
            segment_debug=segment_debug,
            markdown_candidates=[
                {
                    "candidate_type": "markdown_segment",
                    "source_url": page_url,
                    "segment_index": idx,
                    "text": segment[:1400],
                }
                for idx, segment in enumerate(segments)
            ],
            debug=PageReasonerDebug(
                used_llm=True,
                success=False,
                used_fallback=True,
                fallback_reason="reasoner_failed",
                llm_attempts=llm_attempts,
                llm_failures=llm_failures,
                llm_http_failures=llm_http_failures,
                llm_parse_failures=llm_parse_failures,
                selected_model=selected_model,
                selected_timeout_seconds=selected_timeout,
                segments_used=len(segments),
                markdown_chars=len(markdown),
            ),
        )

    stop = bool(stop_votes) and all(stop_votes) and not ordered_links
    stop_reason = stop_reasons[-1] if stop_reasons else None

    return PageReasonerResult(
        speakers=list(dedup_speakers.values()),
        next_links=ordered_links,
        stop=stop,
        stop_reason=stop_reason,
        segment_debug=segment_debug,
        markdown_candidates=[
            {
                "candidate_type": "markdown_segment",
                "source_url": page_url,
                "segment_index": idx,
                "text": segment[:1400],
            }
            for idx, segment in enumerate(segments)
        ],
        debug=PageReasonerDebug(
            used_llm=True,
            success=True,
            used_fallback=False,
            llm_attempts=llm_attempts,
            llm_failures=llm_failures,
            llm_http_failures=llm_http_failures,
            llm_parse_failures=llm_parse_failures,
            selected_model=selected_model,
            selected_timeout_seconds=selected_timeout,
            segments_used=len(segments),
            markdown_chars=len(markdown),
        ),
    )

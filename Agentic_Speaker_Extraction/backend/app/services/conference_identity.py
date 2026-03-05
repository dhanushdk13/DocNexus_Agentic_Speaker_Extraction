from __future__ import annotations

import json
import re
from typing import Literal
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
from app.services.extract_candidates import extract_visible_text
from app.services.llm_response import extract_json_object, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout


EVENT_TERMS = {
    "annual meeting",
    "meeting",
    "summit",
    "conference",
    "congress",
    "forum",
    "symposium",
    "assembly",
    "expo",
    "workshop",
    "program",
}


class ConferenceIdentityDebug(BaseModel):
    stage: Literal["conference_identity"] = "conference_identity"
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


class ConferenceIdentityResult(BaseModel):
    organizer_name: str | None = None
    event_series_name: str | None = None
    display_name: str
    canonical_name: str
    confidence: float = Field(ge=0.0, le=1.0)
    debug: ConferenceIdentityDebug = Field(default_factory=ConferenceIdentityDebug)


def _clean(text: str | None) -> str:
    return " ".join(str(text or "").split()).strip()


def _title_parts(title: str) -> list[str]:
    if not title.strip():
        return []
    return [_clean(part) for part in re.split(r"[|\-–—:]", title) if _clean(part)]


def _title_case_slug(value: str) -> str:
    tokens = [token for token in re.split(r"[^a-zA-Z0-9]+", value) if token]
    return " ".join(token.capitalize() for token in tokens)


def _event_name_from_url(home_url: str) -> str | None:
    path_parts = [part for part in urlparse(home_url).path.split("/") if part]
    if not path_parts:
        return None
    for part in reversed(path_parts):
        lowered = part.lower()
        if lowered in {"events", "event", "program", "agenda", "schedule", "index", "details"}:
            continue
        if lowered.isdigit():
            continue
        if re.match(r"^[0-9a-f-]{8,}$", lowered):
            continue
        candidate = _title_case_slug(part)
        if candidate:
            return candidate
    return None


def _extract_meta_site_name(html: str) -> str | None:
    if not html.strip():
        return None
    soup = BeautifulSoup(html, "lxml")
    selectors = [
        "meta[property='og:site_name']",
        "meta[name='application-name']",
        "meta[name='apple-mobile-web-app-title']",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            content = _clean(node.get("content"))
            if content:
                return content
    return None


def _contains_event_term(value: str) -> bool:
    lowered = value.lower()
    return any(term in lowered for term in EVENT_TERMS)


def _fallback_identity(
    *,
    home_url: str,
    page_title: str,
    html: str,
    year_hints: list[int],
) -> ConferenceIdentityResult:
    site_name = _extract_meta_site_name(html)
    parts = _title_parts(page_title)

    organizer = site_name
    if not organizer and parts:
        organizer = parts[-1]
    if not organizer:
        host = urlparse(home_url).netloc.lower()
        root = host.split(".")[0]
        organizer = _title_case_slug(root) or "Conference Organizer"

    event_series = None
    if parts:
        event_candidates = [part for part in parts if _contains_event_term(part)]
        if event_candidates:
            event_series = event_candidates[0]
        elif len(parts) >= 2:
            event_series = parts[0]
    if not event_series:
        event_series = _event_name_from_url(home_url)
    if not event_series:
        event_series = "Conference Event"

    organizer_clean = _clean(organizer)
    event_clean = _clean(event_series)
    if organizer_clean and event_clean and organizer_clean.lower() not in event_clean.lower():
        display_name = f"{organizer_clean} - {event_clean}"
    else:
        display_name = event_clean or organizer_clean or "Conference"

    # Include explicit year in event series only when present in page evidence.
    if year_hints and not re.search(r"\b(19|20|21)\d{2}\b", display_name):
        preferred_year = max(year_hints)
        if preferred_year >= 1990:
            display_name = f"{display_name} {preferred_year}"

    canonical = _clean(display_name).lower()
    return ConferenceIdentityResult(
        organizer_name=organizer_clean or None,
        event_series_name=event_clean or None,
        display_name=display_name[:255],
        canonical_name=canonical[:255] or "conference",
        confidence=0.62,
        debug=ConferenceIdentityDebug(
            used_llm=False,
            success=False,
            used_fallback=True,
            fallback_reason="meta_title_fallback",
        ),
    )


async def infer_conference_identity(
    settings: Settings,
    *,
    home_url: str,
    page_title: str,
    html: str,
    top_headings: list[str],
    year_hints: list[int],
) -> ConferenceIdentityResult:
    fallback = _fallback_identity(
        home_url=home_url,
        page_title=page_title,
        html=html,
        year_hints=year_hints,
    )
    if not settings.deepseek_api_key:
        return fallback

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="identity")
    selected_timeout = select_llm_timeout(settings, stage="identity", default_timeout_seconds=40.0)
    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

    payload = {
        "home_url": home_url,
        "page_title": page_title,
        "meta_site_name": _extract_meta_site_name(html),
        "top_headings": top_headings[:15],
        "year_hints": sorted({int(y) for y in year_hints if 1990 <= int(y) <= 2100}),
        "visible_text": extract_visible_text(html, max_chars=4000),
        "fallback": fallback.model_dump(exclude={"debug"}),
    }

    async with httpx.AsyncClient(timeout=selected_timeout) as client:
        for attempt in range(2):
            llm_attempts += 1
            try:
                response = await client.post(
                    endpoint,
                    headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
                    json={
                        "model": selected_model,
                        "temperature": 0,
                        "response_format": {"type": "json_object"},
                        "messages": [
                            {
                                "role": "system",
                                "content": (
                                    "Infer conference identity from seed evidence. "
                                    "Return strict JSON with keys: organizer_name, event_series_name, display_name, canonical_name, confidence. "
                                    "Use organizer + event series form when possible."
                                ),
                            },
                            {
                                "role": "user",
                                "content": json.dumps(payload, ensure_ascii=True)
                                if attempt == 0
                                else json.dumps(payload, ensure_ascii=True)
                                + "\nPrevious output invalid. Return schema-valid JSON only.",
                            },
                        ],
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
                if not isinstance(parsed, dict):
                    raise json.JSONDecodeError("invalid JSON object", content, 0)
                validated = ConferenceIdentityResult.model_validate(parsed)
                validated.display_name = _clean(validated.display_name)[:255]
                validated.canonical_name = _clean(validated.canonical_name).lower()[:255]
                if not validated.display_name:
                    raise ValueError("empty display name")
                if not validated.canonical_name:
                    validated.canonical_name = validated.display_name.lower()
                validated.debug = ConferenceIdentityDebug(
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
                return validated
            except (KeyError, ValueError, TypeError, ValidationError, json.JSONDecodeError):
                llm_failures += 1
                llm_parse_failures += 1
                continue

    fallback.debug = ConferenceIdentityDebug(
        used_llm=True,
        success=False,
        used_fallback=True,
        fallback_reason="llm_invalid_or_unavailable",
        llm_attempts=llm_attempts,
        llm_failures=llm_failures,
        llm_http_failures=llm_http_failures,
        llm_parse_failures=llm_parse_failures,
        selected_model=selected_model,
        selected_timeout_seconds=selected_timeout,
    )
    return fallback

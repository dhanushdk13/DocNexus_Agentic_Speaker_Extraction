from __future__ import annotations

import json
import re
from typing import Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
from app.services.extract_candidates import extract_internal_links, extract_page_title, extract_visible_text
from app.services.llm_response import extract_json_object, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout


class SeedSummaryPacket(BaseModel):
    home_url: str
    domain: str
    content_type: str
    http_status: int | None
    blocked: bool
    title: str
    visible_text: str
    top_internal_links: list[dict[str, str]] = Field(default_factory=list)
    year_hints: list[int] = Field(default_factory=list)


class SeedClassificationDebug(BaseModel):
    stage: Literal["classify_seed"] = "classify_seed"
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


class SeedClassificationResult(BaseModel):
    page_type: Literal[
        "homepage",
        "program",
        "agenda",
        "speakers",
        "registration",
        "platform_shell",
        "pdf",
        "unknown",
    ] = "unknown"
    suggested_paths: list[str] = Field(default_factory=list)
    priority_links: list[str] = Field(default_factory=list)
    stop_rules: list[str] = Field(default_factory=list)
    debug: SeedClassificationDebug = Field(default_factory=SeedClassificationDebug)


KEYWORD_SCORES = {
    "speakers": 10,
    "speaker": 10,
    "faculty": 10,
    "presenter": 8,
    "agenda": 8,
    "program": 8,
    "schedule": 8,
    "sessions": 8,
    "session": 7,
    "abstract": 7,
    "symposium": 6,
    "workshop": 6,
}


def _fallback_classification(packet: SeedSummaryPacket, reason: str) -> SeedClassificationResult:
    page_type = "pdf" if packet.content_type == "pdf" else "homepage"
    lowered = f"{packet.title} {packet.visible_text}".lower()
    if any(term in lowered for term in ["speaker", "faculty", "presenter"]):
        page_type = "speakers"
    elif any(term in lowered for term in ["agenda", "program", "session"]):
        page_type = "program"

    scored: list[tuple[int, str]] = []
    for item in packet.top_internal_links:
        blob = f"{item.get('url', '')} {item.get('anchor', '')}".lower()
        score = sum(weight for term, weight in KEYWORD_SCORES.items() if term in blob)
        if score > 0:
            scored.append((score, item["url"]))

    scored.sort(key=lambda row: row[0], reverse=True)
    priority_links = []
    seen: set[str] = set()
    for _, url in scored:
        if url in seen:
            continue
        seen.add(url)
        priority_links.append(url)
        if len(priority_links) >= 15:
            break

    return SeedClassificationResult(
        page_type=page_type,
        suggested_paths=["/speakers", "/faculty", "/program", "/agenda", "/schedule"],
        priority_links=priority_links,
        stop_rules=["need speaker bios or explicit speaker/session listings"],
        debug=SeedClassificationDebug(
            used_llm=False,
            success=False,
            used_fallback=True,
            fallback_reason=reason,
        ),
    )


def build_seed_summary_packet(
    *,
    home_url: str,
    html: str,
    content_type: str,
    http_status: int | None,
    blocked: bool,
) -> SeedSummaryPacket:
    visible = extract_visible_text(html, max_chars=2000)
    years = sorted({int(y) for y in re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", f"{extract_page_title(html)} {visible}")})

    return SeedSummaryPacket(
        home_url=home_url,
        domain=httpx.URL(home_url).host or "",
        content_type=content_type,
        http_status=http_status,
        blocked=blocked,
        title=extract_page_title(html),
        visible_text=visible,
        top_internal_links=extract_internal_links(html, home_url, max_links=30),
        year_hints=years[:12],
    )


async def classify_seed_page(settings: Settings, packet: SeedSummaryPacket) -> SeedClassificationResult:
    if packet.blocked:
        return _fallback_classification(packet, "seed_blocked")

    if not settings.deepseek_api_key:
        return _fallback_classification(packet, "missing_api_key")

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="preflight")
    selected_timeout = select_llm_timeout(settings, stage="preflight", default_timeout_seconds=30.0)
    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

    system_prompt = (
        "You classify conference seed pages and propose high-value internal links. "
        "Return strict JSON with keys: page_type, suggested_paths, priority_links, stop_rules."
    )
    user_prompt = json.dumps(packet.model_dump(), ensure_ascii=True)

    async with httpx.AsyncClient(timeout=selected_timeout) as client:
        for attempt in range(2):
            llm_attempts += 1
            try:
                response = await client.post(
                    endpoint,
                    headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
                    json={
                        "model": selected_model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {
                                "role": "user",
                                "content": (
                                    user_prompt
                                    if attempt == 0
                                    else user_prompt + "\nPrevious output was invalid. Return schema-valid JSON only."
                                ),
                            },
                        ],
                        "temperature": 0,
                        "response_format": {"type": "json_object"},
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
                validated = SeedClassificationResult.model_validate(parsed)

                validated.priority_links = list(dict.fromkeys(validated.priority_links))[:15]
                validated.suggested_paths = list(dict.fromkeys(validated.suggested_paths))[:12]
                validated.stop_rules = list(dict.fromkeys(validated.stop_rules))[:8]
                validated.debug = SeedClassificationDebug(
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
            except (KeyError, json.JSONDecodeError, ValidationError, TypeError):
                llm_failures += 1
                llm_parse_failures += 1
                continue

    fallback = _fallback_classification(packet, "llm_invalid_or_unavailable")
    fallback.debug = SeedClassificationDebug(
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

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
from app.models.enums import SourceCategory, SourceMethod
from app.services.llm_response import extract_json_object, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout
from app.services.search import SearchResult


class TriageSelectedItem(BaseModel):
    url: str
    category: SourceCategory
    method: SourceMethod
    score: float = Field(ge=0.0, le=1.0)
    reason: str


class TriageDiscardedItem(BaseModel):
    url: str
    reason: str


class PlatformGuess(BaseModel):
    name: str
    evidence: list[str] = Field(default_factory=list)


class TriageDebug(BaseModel):
    stage: Literal["triage_urls"] = "triage_urls"
    used_llm: bool = False
    success: bool = True
    used_fallback: bool = False
    fallback_reason: str | None = "not_reported"
    llm_attempts: int = 0
    llm_failures: int = 0
    llm_http_failures: int = 0
    llm_parse_failures: int = 0
    selected_model: str | None = None
    selected_timeout_seconds: float | None = None


class TriageResult(BaseModel):
    selected: list[TriageSelectedItem] = Field(default_factory=list)
    discarded: list[TriageDiscardedItem] = Field(default_factory=list)
    platform_guess: PlatformGuess
    debug: TriageDebug = Field(default_factory=TriageDebug)


@dataclass(slots=True)
class TriageInputResult:
    url: str
    title: str
    snippet: str


def _fallback_triage(
    results: list[SearchResult],
    reason: str,
    *,
    llm_attempts: int = 0,
    llm_failures: int = 0,
    llm_http_failures: int = 0,
    llm_parse_failures: int = 0,
) -> TriageResult:
    selected: list[TriageSelectedItem] = []
    for item in results[:8]:
        lowered = f"{item.title} {item.snippet} {item.url}".lower()
        category = SourceCategory.unknown
        method = SourceMethod.http_static

        if "pdf" in lowered or item.url.lower().endswith(".pdf"):
            category = SourceCategory.pdf_program
            method = SourceMethod.pdf_text
        elif any(k in lowered for k in ["speaker", "faculty"]):
            category = SourceCategory.official_speakers
            method = SourceMethod.http_static
        elif any(k in lowered for k in ["program", "agenda", "schedule"]):
            category = SourceCategory.official_program
            method = SourceMethod.http_static
        elif any(k in lowered for k in ["cvent", "swapcard", "whova"]):
            category = SourceCategory.platform
            method = SourceMethod.playwright_dom

        selected.append(
            TriageSelectedItem(
                url=item.url,
                category=category,
                method=method,
                score=0.45,
                reason="Fallback ranking used due missing/invalid LLM output",
            )
        )

    return TriageResult(
        selected=selected,
        discarded=[],
        platform_guess=PlatformGuess(name="none", evidence=[]),
        debug=TriageDebug(
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


async def triage_urls(settings: Settings, results: list[SearchResult]) -> TriageResult:
    if not results:
        return TriageResult(
            selected=[],
            discarded=[],
            platform_guess=PlatformGuess(name="none", evidence=[]),
            debug=TriageDebug(
                used_llm=False,
                success=True,
                used_fallback=False,
                fallback_reason="no_search_results",
            ),
        )

    if not settings.deepseek_api_key:
        return _fallback_triage(results, reason="missing_api_key")

    payload = [asdict(TriageInputResult(url=r.url, title=r.title, snippet=r.snippet)) for r in results]

    system_prompt = (
        "You are a URL triage assistant for conference speaker scraping. "
        "Return strict JSON only, no markdown. "
        "Never invent URLs."
    )
    user_prompt = (
        "Classify and select the best links for speaker/program extraction. "
        "Use schema with keys selected/discarded/platform_guess. "
        "For selected items use only enum values: "
        f"category={','.join([e.value for e in SourceCategory])}; "
        f"method={','.join([e.value for e in SourceMethod])}. "
        "Input results:\n"
        + json.dumps(payload, ensure_ascii=True)
    )

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="triage")
    selected_timeout = select_llm_timeout(settings, stage="triage", default_timeout_seconds=45.0)

    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

    async with httpx.AsyncClient(timeout=selected_timeout) as client:
        for attempt in range(2):
            llm_attempts += 1
            messages = [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": user_prompt if attempt == 0 else user_prompt + "\nPrevious output invalid. Return valid JSON only.",
                },
            ]
            try:
                response = await client.post(
                    endpoint,
                    headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
                    json={
                        "model": selected_model,
                        "messages": messages,
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
                if not isinstance(parsed, dict):
                    raise json.JSONDecodeError("invalid JSON object", content, 0)
                validated = TriageResult.model_validate(parsed)
                validated.debug = TriageDebug(
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

    fallback = _fallback_triage(
        results,
        reason="llm_invalid_or_unavailable",
        llm_attempts=llm_attempts,
        llm_failures=llm_failures,
        llm_http_failures=llm_http_failures,
        llm_parse_failures=llm_parse_failures,
    )
    fallback.debug.selected_model = selected_model
    fallback.debug.selected_timeout_seconds = selected_timeout
    return fallback


class LinkRerankDebug(BaseModel):
    stage: Literal["rerank_links"] = "rerank_links"
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


class LinkRerankResult(BaseModel):
    selected_urls: list[str] = Field(default_factory=list)
    debug: LinkRerankDebug = Field(default_factory=LinkRerankDebug)


def _fallback_rerank(candidates: list[dict[str, Any]], limit: int, reason: str) -> LinkRerankResult:
    ranked = sorted(
        candidates,
        key=lambda item: float(item.get("score", 0.0)),
        reverse=True,
    )
    selected: list[str] = []
    seen: set[str] = set()
    for item in ranked:
        url = str(item.get("url", "")).strip()
        if not url or url in seen:
            continue
        seen.add(url)
        selected.append(url)
        if len(selected) >= limit:
            break
    return LinkRerankResult(
        selected_urls=selected,
        debug=LinkRerankDebug(
            used_llm=False,
            success=False,
            used_fallback=True,
            fallback_reason=reason,
        ),
    )


async def rerank_links(settings: Settings, candidates: list[dict[str, Any]], limit: int = 15) -> LinkRerankResult:
    if not candidates:
        return LinkRerankResult(
            selected_urls=[],
            debug=LinkRerankDebug(
                used_llm=False,
                success=True,
                used_fallback=False,
                fallback_reason="no_candidates",
            ),
        )

    if not settings.deepseek_api_key:
        return _fallback_rerank(candidates, limit, "missing_api_key")

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="triage")
    selected_timeout = select_llm_timeout(settings, stage="triage", default_timeout_seconds=30.0)
    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

    system_prompt = (
        "Rank internal conference links for speaker/program extraction. "
        "Return strict JSON with key selected_urls only."
    )
    payload = {"candidates": candidates[:120], "limit": limit}

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
                            {"role": "system", "content": system_prompt},
                            {
                                "role": "user",
                                "content": (
                                    json.dumps(payload, ensure_ascii=True)
                                    if attempt == 0
                                    else json.dumps(payload, ensure_ascii=True)
                                    + "\nPrevious output invalid. Return schema-valid JSON only."
                                ),
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
                selected_raw = parsed.get("selected_urls", [])
                if not isinstance(selected_raw, list):
                    raise TypeError("selected_urls must be list")

                selected: list[str] = []
                seen: set[str] = set()
                allowed = {str(item.get("url", "")).strip() for item in candidates if str(item.get("url", "")).strip()}
                for row in selected_raw:
                    url = str(row).strip()
                    if not url or url not in allowed or url in seen:
                        continue
                    seen.add(url)
                    selected.append(url)
                    if len(selected) >= limit:
                        break

                result = LinkRerankResult(
                    selected_urls=selected,
                    debug=LinkRerankDebug(
                        used_llm=True,
                        success=True,
                        used_fallback=False,
                        llm_attempts=llm_attempts,
                        llm_failures=llm_failures,
                        llm_http_failures=llm_http_failures,
                        llm_parse_failures=llm_parse_failures,
                        selected_model=selected_model,
                        selected_timeout_seconds=selected_timeout,
                    ),
                )
                return result
            except (KeyError, json.JSONDecodeError, TypeError, ValidationError):
                llm_failures += 1
                llm_parse_failures += 1
                continue

    fallback = _fallback_rerank(candidates, limit, "llm_invalid_or_unavailable")
    fallback.debug = LinkRerankDebug(
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

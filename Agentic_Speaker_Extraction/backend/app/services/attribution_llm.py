from __future__ import annotations

import json
import re
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
from app.services.llm_response import extract_json_object, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout


class AttributionTarget(BaseModel):
    conference_name: str
    year: int = Field(ge=1990, le=2100)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    reason: str


class AttributionDebug(BaseModel):
    stage: Literal["attribution_resolve"] = "attribution_resolve"
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


class AttributionResult(BaseModel):
    targets: list[AttributionTarget] = Field(default_factory=list)
    unresolved_reason: str | None = None
    debug: AttributionDebug = Field(default_factory=AttributionDebug)


class AttributionBatchItem(BaseModel):
    index: int = Field(ge=0)
    targets: list[AttributionTarget] = Field(default_factory=list)
    unresolved_reason: str | None = None


class AttributionBatchResult(BaseModel):
    results: list[AttributionBatchItem] = Field(default_factory=list)
    debug: AttributionDebug = Field(default_factory=AttributionDebug)


def _extract_year_hints(*texts: str) -> list[int]:
    years: set[int] = set()
    for text in texts:
        if not text:
            continue
        for value in re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", text):
            years.add(int(value))
    return sorted(years)


def _fallback_attribution(
    record: dict,
    source_context: dict,
    known_targets: list[dict],
    *,
    default_conference_name: str | None = None,
    page_year_hints: list[int] | None = None,
) -> AttributionResult:
    _ = source_context, known_targets, default_conference_name, page_year_hints
    explicit_targets: list[AttributionTarget] = []
    for row in record.get("attribution_targets") or []:
        if not isinstance(row, dict):
            continue
        conference_name = str(row.get("conference_name") or "").strip()
        try:
            year = int(row.get("year"))
        except (TypeError, ValueError):
            continue
        if not conference_name or year < 1990 or year > 2100:
            continue
        confidence_raw = row.get("confidence")
        try:
            confidence = float(confidence_raw) if confidence_raw is not None else None
        except (TypeError, ValueError):
            confidence = None
        explicit_targets.append(
            AttributionTarget(
                conference_name=conference_name,
                year=year,
                confidence=confidence,
                reason=str(row.get("reason") or "record_attribution_target_fallback"),
            )
        )

    if explicit_targets:
        return AttributionResult(
            targets=explicit_targets,
            debug=AttributionDebug(
                used_llm=False,
                success=False,
                used_fallback=True,
                fallback_reason="record_targets_only",
            ),
        )

    return AttributionResult(
        targets=[],
        unresolved_reason="llm_unavailable_or_invalid",
        debug=AttributionDebug(
            used_llm=False,
            success=False,
            used_fallback=True,
            fallback_reason="llm_unavailable_or_invalid",
        ),
    )


def _attribution_system_prompt() -> str:
    return (
        "Resolve conference-year attribution for speaker records. "
        "Use evidence from record fields plus page/source context. "
        "Archive year lists are weak evidence and must not override schedule-local evidence. "
        "If a page includes an archive list (for example 2010-2025) and a concrete schedule section "
        "with local date/session context (for example June 10, 2025), attribute speakers to that local schedule year. "
        "Example: Continuum page with archive years 2010-2025 and speaker under June 10, 2025 session -> Continuum 2025. "
        "Return strict JSON only. Never hallucinate conferences or years. "
        "If evidence is truly missing, return empty targets with unresolved_reason."
    )


async def resolve_attribution(
    settings: Settings,
    *,
    record: dict,
    source_context: dict,
    known_targets: list[dict],
    default_conference_name: str | None = None,
    page_year_hints: list[int] | None = None,
) -> AttributionResult:
    if not settings.deepseek_api_key:
        return _fallback_attribution(
            record,
            source_context,
            known_targets,
            default_conference_name=default_conference_name,
            page_year_hints=page_year_hints,
        )

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="attribution")
    selected_timeout = select_llm_timeout(settings, stage="attribution", default_timeout_seconds=45.0)
    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

    payload = {
        "record": record,
        "source_context": source_context,
        "known_targets": known_targets,
        "default_conference_name": default_conference_name,
        "page_year_hints": page_year_hints or [],
        "rules": {
            "multi_link_allowed": True,
            "require_year": True,
            "unknown_to_empty": True,
        },
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
                                "content": _attribution_system_prompt(),
                            },
                            {
                                "role": "user",
                                "content": (
                                    json.dumps(payload, ensure_ascii=True)
                                    if attempt == 0
                                    else json.dumps(payload, ensure_ascii=True)
                                    + "\nPrevious output invalid. Return valid JSON only."
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
                validated = AttributionResult.model_validate(parsed)
                validated.debug = AttributionDebug(
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
                if not validated.targets and not validated.unresolved_reason:
                    validated.unresolved_reason = "no_targets_returned"
                return validated
            except (KeyError, json.JSONDecodeError, ValidationError, TypeError):
                llm_failures += 1
                llm_parse_failures += 1
                continue

    fallback = _fallback_attribution(
        record,
        source_context,
        known_targets,
        default_conference_name=default_conference_name,
        page_year_hints=page_year_hints,
    )
    fallback.debug = AttributionDebug(
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


async def resolve_attribution_batch(
    settings: Settings,
    *,
    records: list[dict[str, Any]],
    source_context: dict[str, Any],
    known_targets: list[dict[str, Any]],
    default_conference_name: str | None = None,
    page_year_hints: list[int] | None = None,
) -> AttributionBatchResult:
    if not records:
        return AttributionBatchResult()

    if not settings.deepseek_api_key:
        out: list[AttributionBatchItem] = []
        for idx, row in enumerate(records):
            fallback = _fallback_attribution(
                row,
                source_context,
                known_targets,
                default_conference_name=default_conference_name,
                page_year_hints=page_year_hints,
            )
            out.append(
                AttributionBatchItem(
                    index=idx,
                    targets=fallback.targets,
                    unresolved_reason=fallback.unresolved_reason,
                )
            )
        return AttributionBatchResult(
            results=out,
            debug=AttributionDebug(
                used_llm=False,
                success=False,
                used_fallback=True,
                fallback_reason="missing_api_key",
            ),
        )

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="attribution")
    selected_timeout = select_llm_timeout(settings, stage="attribution", default_timeout_seconds=45.0)
    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

    payload = {
        "records": records,
        "source_context": source_context,
        "known_targets": known_targets,
        "default_conference_name": default_conference_name,
        "page_year_hints": page_year_hints or [],
        "rules": {
            "multi_link_allowed": True,
            "require_year": True,
            "unknown_to_empty": True,
            "return_per_record_result": True,
        },
    }

    system_prompt = (
        _attribution_system_prompt()
        + " Return strict JSON with key 'results', where each item has: "
        "index (0-based), targets (array), unresolved_reason (nullable)."
    )

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
                                    + "\nPrevious output invalid. Return valid JSON only."
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
                validated = AttributionBatchResult.model_validate(parsed)
                validated.debug = AttributionDebug(
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

    fallback_rows: list[AttributionBatchItem] = []
    for idx, row in enumerate(records):
        fallback = _fallback_attribution(
            row,
            source_context,
            known_targets,
            default_conference_name=default_conference_name,
            page_year_hints=page_year_hints,
        )
        fallback_rows.append(
            AttributionBatchItem(
                index=idx,
                targets=fallback.targets,
                unresolved_reason=fallback.unresolved_reason,
            )
        )

    return AttributionBatchResult(
        results=fallback_rows,
        debug=AttributionDebug(
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
        ),
    )

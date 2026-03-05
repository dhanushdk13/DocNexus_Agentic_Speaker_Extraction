from __future__ import annotations

import json
import re
from math import ceil
from typing import Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
from app.services.llm_response import extract_json_object, extract_json_payload, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout


class LLMCallDebug(BaseModel):
    stage: Literal["extract_speakers", "generate_talk_brief", "llm_normalize"]
    used_llm: bool = False
    success: bool = False
    used_fallback: bool = False
    fallback_reason: str | None = None
    llm_attempts: int = 0
    llm_failures: int = 0
    llm_http_failures: int = 0
    llm_parse_failures: int = 0
    llm_batches_started: int = 0
    llm_batches_completed: int = 0
    llm_batches_timed_out: int = 0
    selected_model: str | None = None
    selected_timeout_seconds: float | None = None


class AttributionTargetHint(BaseModel):
    conference_name: str
    year: int = Field(ge=1990, le=2100)
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str | None = None


class ExtractedSpeaker(BaseModel):
    full_name: str
    designation: str | None = None
    affiliation: str | None = None
    location: str | None = None
    role: str | None = None
    session_title: str | None = None
    talk_brief_extracted: str | None = None
    aliases: list[str] = Field(default_factory=list)
    is_physician_candidate: bool = False
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_span: str | None = None
    attribution_targets: list[AttributionTargetHint] = Field(default_factory=list)


class NameRefineItem(BaseModel):
    idx: int
    full_name: str
    designation: str | None = None
    drop: bool = False


class ExtractionResult(BaseModel):
    records: list[ExtractedSpeaker] = Field(default_factory=list)
    debug: LLMCallDebug = Field(
        default_factory=lambda: LLMCallDebug(
            stage="extract_speakers",
            used_llm=False,
            success=True,
            used_fallback=False,
            fallback_reason="not_reported",
        )
    )


class NormalizeResult(BaseModel):
    records: list[ExtractedSpeaker] = Field(default_factory=list)
    debug: LLMCallDebug = Field(
        default_factory=lambda: LLMCallDebug(
            stage="llm_normalize",
            used_llm=False,
            success=True,
            used_fallback=False,
            fallback_reason="not_reported",
        )
    )


NON_PERSON_NAME_TOKENS = {
    "conference program",
    "program committee",
    "eastern standard",
    "session objectives",
    "terms and conditions",
    "hotel information",
    "register now",
}
CREDENTIAL_PATTERN = re.compile(r"\b(MD|M\.D\.?|DO|D\.O\.?|OD|O\.D\.?|PhD|Ph\.D\.?|MBBS|NP|PA-C|PA|RN|DNP|PharmD|MPH)\b", re.I)


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _looks_non_person_name(name: str) -> bool:
    lowered = _normalize_whitespace(name).lower()
    if not lowered:
        return True
    if lowered in NON_PERSON_NAME_TOKENS:
        return True
    if any(token in lowered for token in NON_PERSON_NAME_TOKENS):
        return True
    words = [w for w in re.split(r"\s+", lowered) if w]
    if len(words) < 2 or len(words) > 5:
        return True
    if not re.search(r"[a-z]", lowered):
        return True
    return False


def _extract_json_payload_from_text(content: str) -> dict | list | None:
    return extract_json_payload(content)


def _coerce_record(raw: dict) -> ExtractedSpeaker | None:
    full_name = _normalize_whitespace(str(raw.get("full_name") or raw.get("name") or ""))
    if not full_name or _looks_non_person_name(full_name):
        return None

    designation = raw.get("designation")
    if designation is not None:
        designation = _normalize_whitespace(str(designation))[:80] or None

    evidence = _normalize_whitespace(str(raw.get("evidence_span") or ""))
    confidence_raw = raw.get("confidence", 0.7)
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        confidence = 0.7
    confidence = max(0.0, min(1.0, confidence))

    is_physician_candidate = raw.get("is_physician_candidate")
    if isinstance(is_physician_candidate, bool):
        physician_candidate = is_physician_candidate
    else:
        physician_candidate = bool(designation and CREDENTIAL_PATTERN.search(designation))

    aliases_raw = raw.get("aliases")
    aliases: list[str] = []
    if isinstance(aliases_raw, list):
        for item in aliases_raw:
            alias = _normalize_whitespace(str(item))
            if alias and alias.lower() != full_name.lower():
                aliases.append(alias[:140])

    attribution_targets_raw = raw.get("attribution_targets")
    attribution_targets: list[AttributionTargetHint] = []
    if isinstance(attribution_targets_raw, list):
        for target in attribution_targets_raw:
            if not isinstance(target, dict):
                continue
            try:
                attribution_targets.append(AttributionTargetHint.model_validate(target))
            except ValidationError:
                continue

    try:
        return ExtractedSpeaker(
            full_name=full_name,
            designation=designation,
            affiliation=_normalize_whitespace(str(raw.get("affiliation") or ""))[:180] or None,
            location=_normalize_whitespace(str(raw.get("location") or ""))[:120] or None,
            role=_normalize_whitespace(str(raw.get("role") or ""))[:120] or None,
            session_title=_normalize_whitespace(str(raw.get("session_title") or ""))[:220] or None,
            talk_brief_extracted=_normalize_whitespace(str(raw.get("talk_brief_extracted") or ""))[:600] or None,
            aliases=aliases[:10],
            is_physician_candidate=physician_candidate,
            confidence=confidence,
            evidence_span=evidence[:800] or None,
            attribution_targets=attribution_targets[:6],
        )
    except ValidationError:
        return None


def _records_from_llm_payload(parsed: dict | list) -> list[ExtractedSpeaker]:
    if isinstance(parsed, dict):
        raw_records = parsed.get("records")
        if isinstance(raw_records, list):
            candidates = raw_records
        else:
            candidates = []
    elif isinstance(parsed, list):
        candidates = parsed
    else:
        candidates = []

    out: list[ExtractedSpeaker] = []
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        record = _coerce_record(raw)
        if record is None:
            continue
        out.append(record)
    return out


async def _refine_names_with_llm(
    *,
    client: httpx.AsyncClient,
    endpoint: str,
    settings: Settings,
    records: list[ExtractedSpeaker],
) -> tuple[list[ExtractedSpeaker], int, int, int, int]:
    if not records:
        return records, 0, 0, 0, 0

    payload = {
        "records": [
            {
                "idx": idx,
                "full_name": record.full_name,
                "designation": record.designation,
                "role": record.role,
                "session_title": record.session_title,
                "evidence_span": record.evidence_span,
            }
            for idx, record in enumerate(records)
        ]
    }
    system_prompt = (
        "You are a strict name normalizer for conference speaker records. "
        "Return JSON only: {\"records\":[{\"idx\":int,\"full_name\":str,\"designation\":str|null,\"drop\":bool}]}. "
        "Rules: "
        "1) full_name must be only person-name tokens, usually 2-4 tokens. "
        "2) Remove role/topic prefixes or suffixes from names. "
        "3) Move credentials/certifications into designation. "
        "4) If item is not a real person, set drop=true. "
        "5) Do not invent names or credentials."
    )
    selected_model = select_llm_model(settings, stage="extraction")

    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0

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
                                json.dumps(payload, ensure_ascii=True)
                                if attempt == 0
                                else json.dumps(payload, ensure_ascii=True)
                                + "\nPrevious output invalid. Return valid JSON only."
                            ),
                        },
                    ],
                    "response_format": {"type": "json_object"},
                    "max_tokens": 1400,
                    "temperature": 0,
                },
            )
        except httpx.TimeoutException:
            llm_failures += 1
            llm_http_failures += 1
            continue
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
            parsed = _extract_json_payload_from_text(content)
            if not isinstance(parsed, dict):
                raise json.JSONDecodeError("invalid shape", content, 0)
            raw_items = parsed.get("records")
            if not isinstance(raw_items, list):
                raise json.JSONDecodeError("missing records list", content, 0)

            updates: dict[int, NameRefineItem] = {}
            for raw in raw_items:
                if not isinstance(raw, dict):
                    continue
                try:
                    item = NameRefineItem.model_validate(raw)
                except ValidationError:
                    continue
                updates[item.idx] = item

            refined: list[ExtractedSpeaker] = []
            for idx, record in enumerate(records):
                update = updates.get(idx)
                if update is None:
                    refined.append(record)
                    continue
                if update.drop:
                    continue
                cleaned_name = _normalize_whitespace(update.full_name)
                if not cleaned_name or _looks_non_person_name(cleaned_name):
                    continue
                designation = update.designation
                if designation is not None:
                    designation = _normalize_whitespace(str(designation))[:80] or None
                refined.append(record.model_copy(update={"full_name": cleaned_name, "designation": designation}))
            return refined, llm_attempts, llm_failures, llm_http_failures, llm_parse_failures
        except (KeyError, json.JSONDecodeError, ValidationError, TypeError):
            llm_failures += 1
            llm_parse_failures += 1
            continue

    return records, llm_attempts, llm_failures, llm_http_failures, llm_parse_failures


def _compact_candidate_for_llm(candidate: dict) -> dict:
    candidate_type = str(candidate.get("candidate_type", ""))[:40]
    if candidate_type == "page_segment":
        text_limit = 2600
        raw_limit = 300
    elif candidate_type == "session_speaker_pair":
        text_limit = 1200
        raw_limit = 600
    elif candidate_type in {"dom_block", "pdf_block"}:
        text_limit = 1300
        raw_limit = 350
    elif candidate_type in {"embedded_json", "network_json"}:
        text_limit = 1100
        raw_limit = 900
    else:
        text_limit = 700
        raw_limit = 350

    compact = {
        "candidate_type": candidate_type,
        "source_url": str(candidate.get("source_url", ""))[:500],
    }
    if "segment_index" in candidate and candidate.get("segment_index") is not None:
        compact["segment_index"] = candidate.get("segment_index")
    if "session_title" in candidate and candidate.get("session_title") is not None:
        compact["session_title"] = _normalize_whitespace(str(candidate.get("session_title")))[:260]
    if "speaker_name_raw" in candidate and candidate.get("speaker_name_raw") is not None:
        compact["speaker_name_raw"] = _normalize_whitespace(str(candidate.get("speaker_name_raw")))[:140]
    if "context_snippet" in candidate and candidate.get("context_snippet") is not None:
        compact["context_snippet"] = _normalize_whitespace(str(candidate.get("context_snippet")))[:500]
    if "text" in candidate and candidate.get("text") is not None:
        compact["text"] = _normalize_whitespace(str(candidate.get("text")))[:text_limit]
    if "raw" in candidate and candidate.get("raw") is not None:
        compact["raw"] = _normalize_whitespace(str(candidate.get("raw")))[:raw_limit]
    return compact


def _extract_empty_result(reason: str, stage: Literal["extract_speakers", "llm_normalize"] = "extract_speakers", *, used_llm: bool = False, llm_attempts: int = 0, llm_failures: int = 0, llm_http_failures: int = 0, llm_parse_failures: int = 0) -> ExtractionResult:
    return ExtractionResult(
        records=[],
        debug=LLMCallDebug(
            stage=stage,
            used_llm=used_llm,
            success=False,
            used_fallback=True,
            fallback_reason=reason,
            llm_attempts=llm_attempts,
            llm_failures=llm_failures,
            llm_http_failures=llm_http_failures,
            llm_parse_failures=llm_parse_failures,
        ),
    )


def _heuristic_fallback_records(candidates: list[dict]) -> list[ExtractedSpeaker]:
    output: list[ExtractedSpeaker] = []
    credential_name_pattern = re.compile(
        r"\b([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,3}),\s*"
        r"(MD|M\.D\.?|DO|D\.O\.?|OD|O\.D\.?|PhD|Ph\.D\.?|MBBS|NP|PA-C|PA|RN|DNP|PharmD|MPH)\b"
    )
    dr_name_pattern = re.compile(r"\bDr\.?\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,3})\b")

    for row in candidates:
        text = _normalize_whitespace(str(row.get("text") or row.get("raw") or ""))
        if len(text) < 20:
            continue
        matched_any = False
        for match in credential_name_pattern.finditer(text):
            name = _normalize_whitespace(match.group(1))
            if _looks_non_person_name(name):
                continue
            designation = _normalize_whitespace(match.group(2)).upper().replace(".", "")
            if designation == "PHD":
                designation = "PhD"
            output.append(
                ExtractedSpeaker(
                    full_name=name,
                    designation=designation,
                    affiliation=None,
                    location=None,
                    role=None,
                    session_title=None,
                    talk_brief_extracted=None,
                    aliases=[],
                    is_physician_candidate=True,
                    confidence=0.62,
                    evidence_span=text[:450],
                    attribution_targets=[],
                )
            )
            matched_any = True

        if matched_any:
            continue

        dr_match = dr_name_pattern.search(text)
        if dr_match:
            name = _normalize_whitespace(dr_match.group(1))
            if _looks_non_person_name(name):
                continue
            output.append(
                ExtractedSpeaker(
                    full_name=name,
                    designation=None,
                    affiliation=None,
                    location=None,
                    role=None,
                    session_title=None,
                    talk_brief_extracted=None,
                    aliases=[],
                    is_physician_candidate=False,
                    confidence=0.45,
                    evidence_span=text[:450],
                    attribution_targets=[],
                )
            )

        if len(output) >= 30:
            break

    dedup: dict[tuple[str, str | None], ExtractedSpeaker] = {}
    for item in output:
        dedup[(item.full_name.lower(), item.session_title)] = item
    return list(dedup.values())


def heuristic_normalize_candidates(candidates: list[dict]) -> list[ExtractedSpeaker]:
    return _heuristic_fallback_records(candidates)


async def normalize_candidates(
    settings: Settings,
    candidates: list[dict],
    conference_year_hints: list[dict[str, int | str]] | None = None,
    *,
    batch_size: int = 40,
) -> NormalizeResult:
    if not candidates:
        return NormalizeResult(
            records=[],
            debug=LLMCallDebug(
                stage="llm_normalize",
                used_llm=False,
                success=False,
                used_fallback=True,
                fallback_reason="no_candidates",
            ),
        )

    if not settings.deepseek_api_key:
        fallback_records = _heuristic_fallback_records(candidates)
        return NormalizeResult(
            records=fallback_records,
            debug=LLMCallDebug(
                stage="llm_normalize",
                used_llm=False,
                success=False,
                used_fallback=True,
                fallback_reason="missing_api_key",
            ),
        )

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="extraction")
    selected_timeout = select_llm_timeout(
        settings,
        stage="extraction",
        default_timeout_seconds=max(float(getattr(settings, "llm_request_timeout_seconds", 120) or 120), 10.0),
    )
    llm_attempts = 0
    llm_failures = 0
    llm_http_failures = 0
    llm_parse_failures = 0
    all_records: list[ExtractedSpeaker] = []

    request_timeout = selected_timeout
    llm_batches_started = 0
    llm_batches_completed = 0
    llm_batches_timed_out = 0

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        total_batches = ceil(len(candidates) / batch_size)
        for batch_idx in range(total_batches):
            batch = candidates[batch_idx * batch_size : (batch_idx + 1) * batch_size]
            if not batch:
                continue
            llm_batches_started += 1

            compact_batch = [_compact_candidate_for_llm(item) for item in batch]
            payload = {
                "conference_year_hints": conference_year_hints or [],
                "candidates": compact_batch,
                "requirements": {
                    "physician_only": True,
                    "allow_multi_attribution": True,
                    "unknown_fields_to_null": True,
                    "no_hallucination": True,
                    "talk_brief_extracted_only": True,
                },
            }

            system_prompt = (
                "You normalize conference speaker/session candidates into strict JSON. "
                "Return only {\"records\":[...]} with no markdown. "
                "Include only real people, never program labels. "
                "Process all provided candidates thoroughly; do not stop after first few names. "
                "Coverage rule: if candidate_type is session_speaker_pair, always extract the provided speaker when person-like, "
                "even if designation is missing. "
                "For session_speaker_pair candidates, treat speaker_name_raw as the primary person signal and do not replace it with topic words. "
                "When candidate_type is page_segment, scan the full segment and output every distinct person mentioned with evidence. "
                "Name quality rules are mandatory: "
                "1) full_name must be only the person's name, typically 2-4 tokens. "
                "2) Never include credentials/titles/roles/topics in full_name. "
                "3) designation must contain only credentials/certifications (e.g., MD/DO/PhD/FIDSA), never affiliation/location. "
                "4) If a string is 'role/topic phrase + person name', strip the role/topic phrase and keep only the person name. "
                "5) If uncertain whether a token belongs to the name, exclude it from full_name and preserve raw text in aliases/evidence. "
                "6) Reject non-person outputs entirely. "
                "Each record keys: full_name, designation, affiliation, location, role, session_title, "
                "talk_brief_extracted, aliases, is_physician_candidate, confidence, evidence_span, attribution_targets. "
                "Keep evidence_span concise (<=220 chars). "
                "Use null for missing optional fields and [] for empty arrays. "
                "If no person can be extracted, return {\"records\":[]}. "
                "Do not require credentials to classify a real speaker as extractable. "
                "Never return topic terms such as treatment/prevention/session/program as a person's name. "
                "Do not hallucinate."
            )

            batch_done = False
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
                                        json.dumps(payload, ensure_ascii=True)
                                        if attempt == 0
                                        else json.dumps(payload, ensure_ascii=True)
                                        + "\nPrevious output invalid. Return valid JSON only."
                                    ),
                                },
                            ],
                            "response_format": {"type": "json_object"},
                            "max_tokens": 3200,
                            "temperature": 0,
                        },
                    )
                except httpx.TimeoutException:
                    llm_failures += 1
                    llm_http_failures += 1
                    llm_batches_timed_out += 1
                    continue
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
                    parsed = _extract_json_payload_from_text(content)
                    if parsed is None:
                        raise json.JSONDecodeError("No JSON object found", content, 0)
                    parsed_records = _records_from_llm_payload(parsed)
                    (
                        parsed_records,
                        refine_attempts,
                        refine_failures,
                        refine_http_failures,
                        refine_parse_failures,
                    ) = await _refine_names_with_llm(
                        client=client,
                        endpoint=endpoint,
                        settings=settings,
                        records=parsed_records,
                    )
                    llm_attempts += refine_attempts
                    llm_failures += refine_failures
                    llm_http_failures += refine_http_failures
                    llm_parse_failures += refine_parse_failures
                    all_records.extend(parsed_records)
                    llm_batches_completed += 1
                    batch_done = True
                    break
                except (KeyError, json.JSONDecodeError, ValidationError, TypeError):
                    llm_failures += 1
                    llm_parse_failures += 1
                    continue

            if not batch_done:
                fallback_records = _heuristic_fallback_records(batch)
                (
                    fallback_records,
                    refine_attempts,
                    refine_failures,
                    refine_http_failures,
                    refine_parse_failures,
                ) = await _refine_names_with_llm(
                    client=client,
                    endpoint=endpoint,
                    settings=settings,
                    records=fallback_records,
                )
                llm_attempts += refine_attempts
                llm_failures += refine_failures
                llm_http_failures += refine_http_failures
                llm_parse_failures += refine_parse_failures
                all_records.extend(fallback_records)

    debug = LLMCallDebug(
        stage="llm_normalize",
        used_llm=True,
        success=len(all_records) > 0,
        used_fallback=llm_failures > 0,
        fallback_reason="partial_batch_failures" if llm_failures > 0 else None,
        llm_attempts=llm_attempts,
        llm_failures=llm_failures,
        llm_http_failures=llm_http_failures,
        llm_parse_failures=llm_parse_failures,
        llm_batches_started=llm_batches_started,
        llm_batches_completed=llm_batches_completed,
        llm_batches_timed_out=llm_batches_timed_out,
        selected_model=selected_model,
        selected_timeout_seconds=selected_timeout,
    )

    return NormalizeResult(records=all_records, debug=debug)


async def extract_speakers(settings: Settings, candidate_blocks: list[str]) -> ExtractionResult:
    candidates = [{"candidate_type": "text_block", "text": block} for block in candidate_blocks]
    normalized = await normalize_candidates(settings, candidates, conference_year_hints=[])
    return ExtractionResult(
        records=normalized.records,
        debug=LLMCallDebug(
            stage="extract_speakers",
            used_llm=normalized.debug.used_llm,
            success=normalized.debug.success,
            used_fallback=normalized.debug.used_fallback,
            fallback_reason=normalized.debug.fallback_reason,
            llm_attempts=normalized.debug.llm_attempts,
            llm_failures=normalized.debug.llm_failures,
            llm_http_failures=normalized.debug.llm_http_failures,
            llm_parse_failures=normalized.debug.llm_parse_failures,
        ),
    )


async def generate_talk_brief(settings: Settings, session_title: str, raw_context: str) -> str | None:
    if not settings.deepseek_api_key or not session_title:
        return None

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="talk_brief")
    selected_timeout = select_llm_timeout(settings, stage="talk_brief", default_timeout_seconds=30.0)
    compact_context = _normalize_whitespace(raw_context)[:1800]
    prompt = (
        "Create a concise 1-2 sentence summary of the talk based strictly on the given context. "
        "If context is insufficient, return null."
    )

    try:
        async with httpx.AsyncClient(timeout=selected_timeout) as client:
            response = await client.post(
                endpoint,
                headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
                json={
                    "model": selected_model,
                    "messages": [
                        {"role": "system", "content": "Return JSON only with key brief."},
                        {
                            "role": "user",
                            "content": json.dumps(
                                {
                                    "instruction": prompt,
                                    "session_title": session_title,
                                    "context": compact_context,
                                }
                            ),
                        },
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0,
                },
            )
            if response.status_code >= 400:
                return None

            try:
                body = response.json()
                content = extract_message_text(body)
                parsed = extract_json_object(content)
                if not isinstance(parsed, dict):
                    return None
                brief = parsed.get("brief")
                if isinstance(brief, str) and brief.strip():
                    return brief.strip()
            except (KeyError, json.JSONDecodeError, TypeError, AttributeError):
                return None
    except httpx.HTTPError:
        return None

    return None

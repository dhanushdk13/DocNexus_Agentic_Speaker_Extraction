from __future__ import annotations

import asyncio
from dataclasses import asdict
import json
import re
from dataclasses import dataclass
from urllib.parse import quote_plus, urlparse

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, ValidationError
try:
    from serpapi import GoogleSearch
except Exception:  # pragma: no cover - optional dependency
    GoogleSearch = None

from app.config import Settings
from app.services.llm_response import extract_json_object, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout


@dataclass(slots=True)
class EvidenceHit:
    source: str
    url: str
    title: str
    snippet: str


class EnrichmentDebug(BaseModel):
    used_search: bool = False
    search_results: int = 0
    search_errors: int = 0
    used_llm: bool = False
    llm_attempts: int = 0
    llm_failures: int = 0
    llm_http_failures: int = 0
    llm_parse_failures: int = 0
    used_fallback: bool = False
    fallback_reason: str | None = None
    providers_used: list[str] = Field(default_factory=list)
    provider_counts: dict[str, int] = Field(default_factory=dict)
    selected_model: str | None = None
    selected_timeout_seconds: float | None = None


class IdentityResolutionOutput(BaseModel):
    ambiguous: bool = True
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    same_person: bool = False
    identity_signature: str | None = None
    reason: str | None = None
    selected_evidence_urls: list[str] = Field(default_factory=list)


class ProfileSynthesisOutput(BaseModel):
    full_name_normalized: str | None = None
    designation_normalized: str | None = None
    specialty: str | None = None
    affiliation: str | None = None
    location: str | None = None
    education: str | None = None
    bio_short: str | None = None
    profile_url: str | None = None
    photo_url: str | None = None
    photo_source_url: str | None = None
    bio_source_url: str | None = None
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)


class PhysicianEnrichmentResult(BaseModel):
    ambiguous: bool = True
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    same_person: bool = False
    identity_signature: str | None = None
    full_name_normalized: str | None = None
    designation_normalized: str | None = None
    specialty: str | None = None
    affiliation: str | None = None
    location: str | None = None
    education: str | None = None
    bio_short: str | None = None
    profile_url: str | None = None
    photo_url: str | None = None
    photo_source_url: str | None = None
    bio_source_url: str | None = None
    reason: str | None = None
    debug: EnrichmentDebug = Field(default_factory=EnrichmentDebug)


def _normalize_whitespace(value: str | None) -> str | None:
    if not value:
        return None
    compact = re.sub(r"\s+", " ", value).strip()
    return compact or None


def _split_name(full_name: str) -> tuple[str | None, str | None]:
    tokens = [item for item in re.split(r"\s+", full_name.strip()) if item]
    if len(tokens) < 2:
        return None, None
    return tokens[0], tokens[-1]


def _build_queries(
    *,
    full_name: str,
    conference_name: str | None,
    year: int | None,
    session_title: str | None,
    designation_hint: str | None,
) -> list[str]:
    conf = _normalize_whitespace(conference_name)
    ses = _normalize_whitespace(session_title)
    desig = _normalize_whitespace(designation_hint)
    year_text = str(year) if year is not None else ""

    queries = [
        f'"{full_name}" physician specialty affiliation',
        f'"{full_name}" profile hospital university',
    ]
    if conf:
        queries.append(f'"{full_name}" "{conf}" speaker {year_text}')
    if ses:
        queries.append(f'"{full_name}" "{ses}" "{conf or ""}"')
    if desig:
        queries.append(f'"{full_name}" {desig} bio')

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        normalized = " ".join(query.split())
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(normalized)
    return deduped


def _asvidence(
    source: str,
    url: str | None,
    title: str | None,
    snippet: str | None,
) -> EvidenceHit | None:
    clean_url = _normalize_whitespace(url)
    if not clean_url:
        return None
    return EvidenceHit(
        source=source,
        url=clean_url[:800],
        title=_normalize_whitespace(title or "") or "",
        snippet=_normalize_whitespace(snippet or "") or "",
    )


def _collect_local_evidence(local_evidence: list[dict[str, str]] | None) -> list[EvidenceHit]:
    out: list[EvidenceHit] = []
    if not local_evidence:
        return out
    for row in local_evidence:
        if not isinstance(row, dict):
            continue
        hit = _asvidence(
            "local",
            row.get("url"),
            row.get("title") or row.get("source") or "Local source",
            row.get("snippet") or row.get("text") or "",
        )
        if hit:
            out.append(hit)
    return out


async def _provider_npi(
    settings: Settings,
    *,
    full_name: str,
    timeout: float,
) -> list[EvidenceHit]:
    if not getattr(settings, "physician_enrichment_enable_npi", True):
        return []
    first_name, last_name = _split_name(full_name)
    if not first_name or not last_name:
        return []

    params = {
        "version": "2.1",
        "first_name": first_name,
        "last_name": last_name,
        "limit": "5",
    }
    url = "https://npiregistry.cms.hhs.gov/api/"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, params=params)
        if response.status_code >= 400:
            return []
        body = response.json()
    except (httpx.HTTPError, json.JSONDecodeError, ValueError):
        return []

    results = body.get("results")
    if not isinstance(results, list):
        return []

    out: list[EvidenceHit] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        number = _normalize_whitespace(str(item.get("number", "")))
        basic = item.get("basic") if isinstance(item.get("basic"), dict) else {}
        first = _normalize_whitespace(str((basic or {}).get("first_name", ""))) or ""
        last = _normalize_whitespace(str((basic or {}).get("last_name", ""))) or ""
        credential = _normalize_whitespace(str((basic or {}).get("credential", ""))) or ""
        org = _normalize_whitespace(str((basic or {}).get("organization_name", ""))) or ""
        taxonomies = item.get("taxonomies") if isinstance(item.get("taxonomies"), list) else []
        specialty = ""
        if taxonomies:
            primary = taxonomies[0]
            if isinstance(primary, dict):
                specialty = _normalize_whitespace(str(primary.get("desc", ""))) or ""
        title = " ".join(part for part in [first, last, credential] if part).strip() or full_name
        snippet = " | ".join(part for part in [specialty, org] if part)
        profile_url = f"https://npiregistry.cms.hhs.gov/provider-view/{number}" if number else None
        hit = _asvidence("npi", profile_url, title, snippet)
        if hit:
            out.append(hit)
    return out


async def _provider_pubmed(
    settings: Settings,
    *,
    full_name: str,
    timeout: float,
) -> list[EvidenceHit]:
    if not getattr(settings, "physician_enrichment_enable_pubmed", True):
        return []
    term = f'"{full_name}"[Author]'
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            search_response = await client.get(
                f"{base}/esearch.fcgi",
                params={"db": "pubmed", "retmode": "json", "retmax": "5", "term": term},
            )
        if search_response.status_code >= 400:
            return []
        search_json = search_response.json()
        ids = (((search_json.get("esearchresult") or {}).get("idlist")) or [])
        if not isinstance(ids, list) or not ids:
            return []

        async with httpx.AsyncClient(timeout=timeout) as client:
            summary_response = await client.get(
                f"{base}/esummary.fcgi",
                params={"db": "pubmed", "retmode": "json", "id": ",".join(ids[:5])},
            )
        if summary_response.status_code >= 400:
            return []
        summary_json = summary_response.json()
    except (httpx.HTTPError, json.JSONDecodeError, ValueError):
        return []

    out: list[EvidenceHit] = []
    result_block = summary_json.get("result")
    if not isinstance(result_block, dict):
        return out
    for uid in ids[:5]:
        row = result_block.get(str(uid))
        if not isinstance(row, dict):
            continue
        title = _normalize_whitespace(str(row.get("title", ""))) or "PubMed result"
        pubdate = _normalize_whitespace(str(row.get("pubdate", ""))) or ""
        source = _normalize_whitespace(str(row.get("source", ""))) or ""
        snippet = " | ".join(part for part in [source, pubdate] if part)
        hit = _asvidence("pubmed", f"https://pubmed.ncbi.nlm.nih.gov/{uid}/", title, snippet)
        if hit:
            out.append(hit)
    return out


async def _provider_openalex(
    settings: Settings,
    *,
    full_name: str,
    timeout: float,
) -> list[EvidenceHit]:
    if not getattr(settings, "physician_enrichment_enable_openalex", True):
        return []
    url = "https://api.openalex.org/authors"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, params={"search": full_name, "per-page": "5"})
        if response.status_code >= 400:
            return []
        body = response.json()
    except (httpx.HTTPError, json.JSONDecodeError, ValueError):
        return []

    results = body.get("results")
    if not isinstance(results, list):
        return []
    out: list[EvidenceHit] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        display_name = _normalize_whitespace(str(row.get("display_name", ""))) or full_name
        works_count = row.get("works_count")
        institution = ""
        lki = row.get("last_known_institution")
        if isinstance(lki, dict):
            institution = _normalize_whitespace(str(lki.get("display_name", ""))) or ""
        url_value = _normalize_whitespace(str(row.get("id", "")))
        snippet_parts = [institution]
        if isinstance(works_count, int):
            snippet_parts.append(f"works={works_count}")
        hit = _asvidence("openalex", url_value, display_name, " | ".join(part for part in snippet_parts if part))
        if hit:
            out.append(hit)
    return out


async def _provider_duckduckgo(
    settings: Settings,
    *,
    query: str,
    timeout: float,
) -> list[EvidenceHit]:
    if not getattr(settings, "physician_enrichment_enable_duckduckgo", True):
        return []
    url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(url, headers=headers)
        if response.status_code >= 400:
            return []
        soup = BeautifulSoup(response.text, "lxml")
    except httpx.HTTPError:
        return []

    out: list[EvidenceHit] = []
    for result in soup.select(".result"):
        anchor = result.select_one("a.result__a")
        if not anchor:
            continue
        href = _normalize_whitespace(anchor.get("href"))
        title = _normalize_whitespace(anchor.get_text(" ", strip=True)) or ""
        snippet_node = result.select_one(".result__snippet")
        snippet = _normalize_whitespace(snippet_node.get_text(" ", strip=True) if snippet_node else "") or ""
        hit = _asvidence("duckduckgo", href, title, snippet)
        if hit:
            out.append(hit)
        if len(out) >= 5:
            break
    return out


def _search_serpapi(params: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
    if GoogleSearch is None:
        return {}
    try:
        search = GoogleSearch(params)
        search.timeout = timeout_seconds
        payload = search.get_dict()
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


async def _provider_serpapi(
    settings: Settings,
    *,
    query: str,
    timeout: float,
) -> list[EvidenceHit]:
    if not settings.serpapi_api_key:
        return []
    params = {
        "q": query,
        "location": settings.serpapi_location,
        "google_domain": settings.serpapi_google_domain,
        "hl": settings.serpapi_hl,
        "gl": settings.serpapi_gl,
        "api_key": settings.serpapi_api_key,
    }
    payload = await asyncio.to_thread(_search_serpapi, params, timeout)
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("error"), str) and payload.get("error"):
        return []
    organic = payload.get("organic_results")
    if not isinstance(organic, list):
        return []

    out: list[EvidenceHit] = []
    for row in organic[:5]:
        if not isinstance(row, dict):
            continue
        hit = _asvidence(
            "serpapi",
            str(row.get("link", "")),
            str(row.get("title", "")),
            str(row.get("snippet", "")),
        )
        if hit:
            out.append(hit)
    return out


def _dedupe_evidence(hits: list[EvidenceHit], max_items: int) -> list[EvidenceHit]:
    dedup: dict[str, EvidenceHit] = {}
    for hit in hits:
        url = hit.url.strip()
        if not url:
            continue
        if url in dedup:
            continue
        dedup[url] = hit
        if len(dedup) >= max_items:
            break
    return list(dedup.values())


def _serialize_evidence_hits(hits: list[EvidenceHit]) -> list[dict[str, str]]:
    return [asdict(hit) for hit in hits]


def _extract_json_obj(content: str) -> dict[str, Any] | None:
    return extract_json_object(content)


def _is_valid_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


async def _identity_pass(
    settings: Settings,
    *,
    full_name: str,
    conference_name: str | None,
    year: int | None,
    session_title: str | None,
    designation_hint: str | None,
    affiliation_hint: str | None,
    location_hint: str | None,
    evidence_hits: list[EvidenceHit],
    debug: EnrichmentDebug,
) -> IdentityResolutionOutput | None:
    if not settings.deepseek_api_key:
        return None

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="enrichment")
    selected_timeout = select_llm_timeout(
        settings,
        stage="enrichment",
        default_timeout_seconds=float(getattr(settings, "llm_request_timeout_seconds", 150) or 150),
    )
    debug.selected_model = selected_model
    debug.selected_timeout_seconds = selected_timeout
    payload = {
        "candidate": {
            "full_name": full_name,
            "conference_name": conference_name,
            "year": year,
            "session_title": session_title,
            "designation_hint": designation_hint,
            "affiliation_hint": affiliation_hint,
            "location_hint": location_hint,
        },
        "evidence": _serialize_evidence_hits(evidence_hits),
        "output_schema": {
            "ambiguous": "bool",
            "confidence": "float_0_to_1",
            "same_person": "bool",
            "identity_signature": "string_or_null",
            "reason": "string_or_null",
            "selected_evidence_urls": ["string_url"],
        },
        "rules": {
            "no_hallucination": True,
            "use_only_evidence": True,
            "ambiguous_if_conflict": True,
        },
    }

    retries = max(1, int(getattr(settings, "physician_enrichment_llm_passes", 2) or 2))
    async with httpx.AsyncClient(timeout=selected_timeout) as client:
        for attempt in range(retries):
            debug.used_llm = True
            debug.llm_attempts += 1
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
                                    "Resolve physician identity from evidence. Return strict JSON only with keys: "
                                    "ambiguous, confidence, same_person, identity_signature, reason, selected_evidence_urls."
                                ),
                            },
                            {
                                "role": "user",
                                "content": json.dumps(payload, ensure_ascii=True)
                                + ("\nPrevious output invalid. Return schema-valid JSON only." if attempt > 0 else ""),
                            },
                        ],
                    },
                )
            except httpx.HTTPError:
                debug.llm_failures += 1
                debug.llm_http_failures += 1
                continue

            if response.status_code >= 400:
                debug.llm_failures += 1
                debug.llm_http_failures += 1
                continue
            try:
                body = response.json()
                content = extract_message_text(body)
                parsed = _extract_json_obj(content)
                if not parsed:
                    raise ValidationError.from_exception_data("IdentityResolutionOutput", [])
                result = IdentityResolutionOutput.model_validate(parsed)
                return result
            except (KeyError, TypeError, ValidationError, json.JSONDecodeError, ValueError):
                debug.llm_failures += 1
                debug.llm_parse_failures += 1
                continue
    return None


async def _profile_pass(
    settings: Settings,
    *,
    identity: IdentityResolutionOutput,
    evidence_hits: list[EvidenceHit],
    full_name: str,
    conference_name: str | None,
    year: int | None,
    designation_hint: str | None,
    debug: EnrichmentDebug,
) -> ProfileSynthesisOutput | None:
    if not settings.deepseek_api_key:
        return None

    endpoint = f"{settings.deepseek_base_url.rstrip('/')}/chat/completions"
    selected_model = select_llm_model(settings, stage="enrichment")
    selected_timeout = select_llm_timeout(
        settings,
        stage="enrichment",
        default_timeout_seconds=float(getattr(settings, "llm_request_timeout_seconds", 150) or 150),
    )
    debug.selected_model = selected_model
    debug.selected_timeout_seconds = selected_timeout
    selected_urls = set(identity.selected_evidence_urls or [])
    selected_hits = [hit for hit in evidence_hits if hit.url in selected_urls] or evidence_hits
    payload = {
        "identity": identity.model_dump(),
        "candidate": {
            "full_name": full_name,
            "conference_name": conference_name,
            "year": year,
            "designation_hint": designation_hint,
        },
        "evidence": _serialize_evidence_hits(selected_hits),
        "output_schema": {
            "full_name_normalized": "string_or_null",
            "designation_normalized": "string_or_null",
            "specialty": "string_or_null",
            "affiliation": "string_or_null",
            "location": "string_or_null",
            "education": "string_or_null",
            "bio_short": "string_or_null<=280",
            "profile_url": "string_url_or_null",
            "bio_source_url": "string_url_or_null",
            "confidence": "float_0_to_1",
        },
        "rules": {
            "no_hallucination": True,
            "use_only_evidence": True,
            "if_unknown_return_null": True,
            "prefer_medical_or_academic_primary_sources": True,
            "bio_style": "professional_2_sentence",
            "bio_include": ["specialty", "primary_affiliation_or_role"],
        },
    }

    retries = max(1, int(getattr(settings, "physician_enrichment_llm_passes", 2) or 2))
    async with httpx.AsyncClient(timeout=selected_timeout) as client:
        for attempt in range(retries):
            debug.used_llm = True
            debug.llm_attempts += 1
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
                                    "Synthesize physician profile from evidence. Return strict JSON only with keys: "
                                    "full_name_normalized, designation_normalized, specialty, affiliation, location, education, "
                                    "bio_short, profile_url, bio_source_url, confidence. "
                                    "Keep bio_short professional and factual (1-2 sentences), and use null when evidence is weak."
                                ),
                            },
                            {
                                "role": "user",
                                "content": json.dumps(payload, ensure_ascii=True)
                                + ("\nPrevious output invalid. Return schema-valid JSON only." if attempt > 0 else ""),
                            },
                        ],
                    },
                )
            except httpx.HTTPError:
                debug.llm_failures += 1
                debug.llm_http_failures += 1
                continue

            if response.status_code >= 400:
                debug.llm_failures += 1
                debug.llm_http_failures += 1
                continue
            try:
                body = response.json()
                content = extract_message_text(body)
                parsed = _extract_json_obj(content)
                if not parsed:
                    raise ValidationError.from_exception_data("ProfileSynthesisOutput", [])
                result = ProfileSynthesisOutput.model_validate(parsed)
                return result
            except (KeyError, TypeError, ValidationError, json.JSONDecodeError, ValueError):
                debug.llm_failures += 1
                debug.llm_parse_failures += 1
                continue
    return None


async def enrich_physician_profile(
    settings: Settings,
    *,
    full_name: str,
    conference_name: str | None,
    year: int | None,
    session_title: str | None,
    designation_hint: str | None,
    affiliation_hint: str | None,
    location_hint: str | None,
    local_evidence: list[dict[str, str]] | None = None,
) -> PhysicianEnrichmentResult:
    if not getattr(settings, "physician_enrichment_enabled", True):
        return PhysicianEnrichmentResult(
            ambiguous=True,
            confidence=0.0,
            same_person=False,
            reason="enrichment_disabled",
            debug=EnrichmentDebug(used_fallback=True, fallback_reason="enrichment_disabled"),
        )

    timeout = max(2.0, float(getattr(settings, "physician_enrichment_source_timeout_seconds", 10) or 10))
    max_evidence = max(1, int(getattr(settings, "physician_enrichment_max_evidence_urls", 10) or 10))
    queries = _build_queries(
        full_name=full_name,
        conference_name=conference_name,
        year=year,
        session_title=session_title,
        designation_hint=designation_hint,
    )

    debug = EnrichmentDebug(used_search=True)
    evidence_hits: list[EvidenceHit] = []

    local_hits = _collect_local_evidence(local_evidence)
    if local_hits:
        debug.providers_used.append("local")
        debug.provider_counts["local"] = len(local_hits)
        evidence_hits.extend(local_hits)

    provider_results = await asyncio.gather(
        _provider_npi(settings, full_name=full_name, timeout=timeout),
        _provider_pubmed(settings, full_name=full_name, timeout=timeout),
        _provider_openalex(settings, full_name=full_name, timeout=timeout),
        return_exceptions=True,
    )
    provider_names = ["npi", "pubmed", "openalex"]
    for name, result in zip(provider_names, provider_results):
        if isinstance(result, Exception):
            debug.search_errors += 1
            continue
        if not result:
            continue
        debug.providers_used.append(name)
        debug.provider_counts[name] = len(result)
        evidence_hits.extend(result)

    for query in queries[:2]:
        ddg = await _provider_duckduckgo(settings, query=query, timeout=timeout)
        if ddg:
            debug.providers_used.append("duckduckgo")
            debug.provider_counts["duckduckgo"] = debug.provider_counts.get("duckduckgo", 0) + len(ddg)
            evidence_hits.extend(ddg)
        if len(evidence_hits) >= max_evidence:
            break

    if len(evidence_hits) < max_evidence and settings.serpapi_api_key:
        serp = await _provider_serpapi(settings, query=queries[0], timeout=timeout)
        if serp:
            debug.providers_used.append("serpapi")
            debug.provider_counts["serpapi"] = len(serp)
            evidence_hits.extend(serp)

    evidence_hits = _dedupe_evidence(evidence_hits, max_items=max_evidence)
    debug.search_results = len(evidence_hits)

    if not evidence_hits:
        debug.used_fallback = True
        debug.fallback_reason = "no_evidence_found"
        return PhysicianEnrichmentResult(
            ambiguous=True,
            confidence=0.0,
            same_person=False,
            reason="no_evidence_found",
            debug=debug,
        )

    identity = await _identity_pass(
        settings,
        full_name=full_name,
        conference_name=conference_name,
        year=year,
        session_title=session_title,
        designation_hint=designation_hint,
        affiliation_hint=affiliation_hint,
        location_hint=location_hint,
        evidence_hits=evidence_hits,
        debug=debug,
    )
    if not identity:
        debug.used_fallback = True
        debug.fallback_reason = "identity_resolution_failed"
        return PhysicianEnrichmentResult(
            ambiguous=True,
            confidence=0.0,
            same_person=False,
            reason="identity_resolution_failed",
            debug=debug,
        )

    min_conf = max(0.0, min(1.0, float(getattr(settings, "physician_enrichment_min_confidence", 0.7) or 0.7)))
    if identity.ambiguous or not identity.same_person or identity.confidence < min_conf:
        return PhysicianEnrichmentResult(
            ambiguous=True,
            confidence=identity.confidence,
            same_person=identity.same_person,
            identity_signature=identity.identity_signature,
            reason=identity.reason or "identity_ambiguous",
            debug=debug,
        )

    profile = await _profile_pass(
        settings,
        identity=identity,
        evidence_hits=evidence_hits,
        full_name=full_name,
        conference_name=conference_name,
        year=year,
        designation_hint=designation_hint,
        debug=debug,
    )
    if not profile:
        debug.used_fallback = True
        debug.fallback_reason = "profile_synthesis_failed"
        return PhysicianEnrichmentResult(
            ambiguous=True,
            confidence=identity.confidence,
            same_person=True,
            identity_signature=identity.identity_signature,
            reason="profile_synthesis_failed",
            debug=debug,
        )

    profile_url = profile.profile_url if _is_valid_url(profile.profile_url) else None
    bio_source_url = profile.bio_source_url if _is_valid_url(profile.bio_source_url) else None
    bio_short = _normalize_whitespace(profile.bio_short)
    if bio_short and len(bio_short) > 500:
        bio_short = bio_short[:500].rstrip()

    return PhysicianEnrichmentResult(
        ambiguous=False,
        confidence=max(identity.confidence, profile.confidence),
        same_person=True,
        identity_signature=identity.identity_signature,
        full_name_normalized=_normalize_whitespace(profile.full_name_normalized),
        designation_normalized=_normalize_whitespace(profile.designation_normalized),
        specialty=_normalize_whitespace(profile.specialty),
        affiliation=_normalize_whitespace(profile.affiliation),
        location=_normalize_whitespace(profile.location),
        education=_normalize_whitespace(profile.education),
        bio_short=bio_short,
        profile_url=profile_url,
        photo_url=None,
        photo_source_url=None,
        bio_source_url=bio_source_url,
        reason=identity.reason,
        debug=debug,
    )

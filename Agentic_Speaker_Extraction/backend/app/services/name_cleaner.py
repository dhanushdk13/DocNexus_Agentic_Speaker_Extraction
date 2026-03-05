from __future__ import annotations

import re
from dataclasses import dataclass


DESIGNATION_CANONICAL: dict[str, str] = {
    "MD": "MD",
    "M.D": "MD",
    "M.D.": "MD",
    "DO": "DO",
    "D.O": "DO",
    "D.O.": "DO",
    "OD": "OD",
    "O.D": "OD",
    "O.D.": "OD",
    "PHD": "PhD",
    "PH.D": "PhD",
    "PH.D.": "PhD",
    "MBBS": "MBBS",
    "MPH": "MPH",
    "MS": "MS",
    "MSC": "MSc",
    "RN": "RN",
    "DNP": "DNP",
    "NP": "NP",
    "PA": "PA",
    "PA-C": "PA-C",
    "PAC": "PA-C",
    "PHARMD": "PharmD",
    "FIDSA": "FIDSA",
    "AAHIVS": "AAHIVS",
    "FAAHIVS": "FAAHIVS",
    "FAAO": "FAAO",
    "FACS": "FACS",
    "FRCS": "FRCS",
    "MSPH": "MSPH",
}

LEADING_NOISE_SINGLE = {
    "hiv",
    "treatment",
    "session",
    "track",
    "workshop",
    "panel",
    "agenda",
    "program",
    "course",
    "lecture",
}

LEADING_NOISE_PHRASES = {
    ("hiv", "treatment"),
    ("hiv", "the"),
    ("welcome", "from"),
    ("session", "overview"),
}

NAME_STOPWORDS = {
    "conference",
    "program",
    "session",
    "agenda",
    "workshop",
    "panel",
    "track",
    "time",
    "zone",
}

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z'\-\.]*")
UPPER_TOKEN_RE = re.compile(r"^[A-Z][A-Z0-9\-\.]{1,10}$")
PERSON_TOKEN_RE = re.compile(r"^[A-Z][a-zA-Z'\-]+$")


@dataclass(slots=True)
class CanonicalPerson:
    full_name: str
    designation: str | None
    aliases: list[str]
    is_valid: bool
    reason: str | None = None


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _canonical_designation_token(token: str) -> str | None:
    cleaned = token.strip().replace(",", "").replace(";", "")
    if not cleaned:
        return None
    normalized = cleaned.upper().replace(" ", "")
    return DESIGNATION_CANONICAL.get(normalized)


def _extract_tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(text)


def _collect_designations(name: str, designation: str | None) -> list[str]:
    out: list[str] = []
    for text in (name, designation or ""):
        for token in _extract_tokens(text):
            canon = _canonical_designation_token(token)
            if canon and canon not in out:
                out.append(canon)
    return out


def _strip_leading_noise(tokens: list[str], designations: list[str]) -> list[str]:
    items = list(tokens)
    while items:
        lowered = items[0].lower()
        phrase = tuple(t.lower() for t in items[:2]) if len(items) >= 2 else ()
        canon = _canonical_designation_token(items[0])
        if canon:
            if canon not in designations:
                designations.append(canon)
            items.pop(0)
            continue
        if phrase in LEADING_NOISE_PHRASES:
            items = items[2:]
            continue
        if lowered in LEADING_NOISE_SINGLE:
            items.pop(0)
            continue
        if UPPER_TOKEN_RE.match(items[0]) and len(items[0]) <= 6:
            items.pop(0)
            continue
        break
    return items


def _strip_trailing_designations(tokens: list[str], designations: list[str]) -> list[str]:
    items = list(tokens)
    while items:
        canon = _canonical_designation_token(items[-1])
        if canon:
            if canon not in designations:
                designations.append(canon)
            items.pop()
            continue
        break
    return items


def _window_score(window: list[str]) -> int:
    score = 0
    for token in window:
        lowered = token.lower()
        if lowered in NAME_STOPWORDS:
            score -= 4
        if _canonical_designation_token(token):
            score -= 4
        if PERSON_TOKEN_RE.match(token):
            score += 3
        elif UPPER_TOKEN_RE.match(token):
            score -= 2
        else:
            score += 1
    return score


def _pick_best_name_window(tokens: list[str]) -> list[str]:
    if len(tokens) <= 4:
        return tokens
    best_score = -10_000
    best_window: list[str] = tokens[:2]
    for size in (4, 3, 2):
        for start in range(0, len(tokens) - size + 1):
            window = tokens[start : start + size]
            score = _window_score(window)
            if score > best_score:
                best_score = score
                best_window = window
    return best_window


def _title_case_token(token: str) -> str:
    if not token:
        return token
    if token.isupper() and len(token) <= 3:
        return token
    return token[0].upper() + token[1:].lower()


def canonicalize_person_name(
    *,
    full_name: str,
    designation: str | None = None,
    role: str | None = None,
    evidence: str | None = None,
) -> CanonicalPerson:
    raw_name = _normalize_spaces(full_name)
    if not raw_name:
        return CanonicalPerson(full_name="", designation=designation, aliases=[], is_valid=False, reason="empty_name")

    designations = _collect_designations(raw_name, designation)
    scrubbed = re.sub(r"\([^)]*\)", " ", raw_name)
    scrubbed = re.sub(r"[,:;|/]+", " ", scrubbed)
    tokens = _extract_tokens(scrubbed)
    if not tokens:
        return CanonicalPerson(full_name="", designation=", ".join(designations) or designation, aliases=[raw_name], is_valid=False, reason="no_tokens")

    tokens = _strip_leading_noise(tokens, designations)
    tokens = _strip_trailing_designations(tokens, designations)
    if len(tokens) > 4:
        tokens = _pick_best_name_window(tokens)

    cleaned_tokens = [_title_case_token(token) for token in tokens if token]
    cleaned_tokens = [token for token in cleaned_tokens if token.lower() not in NAME_STOPWORDS]
    if len(cleaned_tokens) < 2:
        return CanonicalPerson(
            full_name="",
            designation=", ".join(designations) or designation,
            aliases=[raw_name],
            is_valid=False,
            reason="not_person_like",
        )

    candidate_name = _normalize_spaces(" ".join(cleaned_tokens))
    pieces = candidate_name.split()
    if len(pieces) < 2 or len(pieces) > 5:
        return CanonicalPerson(
            full_name="",
            designation=", ".join(designations) or designation,
            aliases=[raw_name],
            is_valid=False,
            reason="invalid_token_count",
        )
    if any(piece.lower() in NAME_STOPWORDS for piece in pieces):
        return CanonicalPerson(
            full_name="",
            designation=", ".join(designations) or designation,
            aliases=[raw_name],
            is_valid=False,
            reason="contains_stopword",
        )

    designation_out = ", ".join(designations) if designations else _normalize_spaces(designation or "") or None
    aliases: list[str] = []
    if raw_name and raw_name.lower() != candidate_name.lower():
        aliases.append(raw_name)
    if designation_out:
        with_designation = f"{candidate_name}, {designation_out}"
        if with_designation.lower() != raw_name.lower():
            aliases.append(with_designation)
    if role:
        role_value = _normalize_spaces(role)
        if role_value:
            aliases.append(f"{candidate_name} ({role_value})")

    unique_aliases: list[str] = []
    seen_aliases: set[str] = set()
    for alias in aliases:
        key = alias.lower()
        if key in seen_aliases:
            continue
        seen_aliases.add(key)
        unique_aliases.append(alias[:255])

    _ = evidence  # reserved for future rule tuning
    return CanonicalPerson(
        full_name=candidate_name,
        designation=designation_out,
        aliases=unique_aliases,
        is_valid=True,
        reason=None,
    )

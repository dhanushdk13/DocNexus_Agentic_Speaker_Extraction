from __future__ import annotations

import unicodedata
from dataclasses import dataclass
import hashlib
import re
from urllib.parse import urlparse

from rapidfuzz import fuzz
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.models import Appearance, Physician, PhysicianAlias
from app.services.name_cleaner import canonicalize_person_name


CREDENTIAL_STRIP = {
    "md",
    "do",
    "od",
    "mbbs",
    "phd",
    "facs",
    "faao",
    "frcs",
    "mph",
    "ms",
    "dnp",
    "rn",
    "np",
    "pa",
    "pac",
    "pac",
    "pharmd",
    "fidsa",
    "aahivs",
    "faahivs",
    "msph",
}

PHYSICIAN_HINTS = [
    "md",
    "do",
    "od",
    "mbbs",
    "dr ",
    "dr.",
    "physician",
    "surgeon",
    "doctor",
    "ophthalm",
    "cardi",
    "neurolog",
    "hiv",
]


def normalize_text(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_value).strip().lower()


def name_key(name: str) -> str:
    base = normalize_text(name)
    base = re.sub(r"[^a-z0-9\s]", " ", base)
    parts = [p for p in base.split() if p]
    filtered = [p for p in parts if p not in CREDENTIAL_STRIP and p not in {"dr"}]
    return " ".join(filtered)


def alias_key(alias: str) -> str:
    return name_key(alias)


def _base_name_key(key: str) -> str:
    return key.split("::", 1)[0]


def _similarity(a: str | None, b: str | None) -> float:
    left = normalize_text(a or "")
    right = normalize_text(b or "")
    if not left or not right:
        return 0.0
    return float(fuzz.token_set_ratio(left, right))


def _has_profile_conflict(
    *,
    incoming_affiliation: str | None,
    incoming_location: str | None,
    existing_affiliation: str | None,
    existing_location: str | None,
) -> bool:
    aff_conflict = False
    loc_conflict = False
    if incoming_affiliation and existing_affiliation:
        aff_conflict = _similarity(incoming_affiliation, existing_affiliation) < 45.0
    if incoming_location and existing_location:
        loc_conflict = _similarity(incoming_location, existing_location) < 45.0
    return aff_conflict or loc_conflict


def _profile_compatibility_score(
    *,
    incoming_designation: str | None,
    incoming_affiliation: str | None,
    incoming_location: str | None,
    incoming_specialty: str | None = None,
    incoming_profile_url: str | None = None,
    existing: Physician,
) -> float:
    score = 0.0
    weight = 0.0

    if incoming_affiliation and existing.primary_affiliation:
        weight += 0.5
        score += 0.5 * (_similarity(incoming_affiliation, existing.primary_affiliation) / 100.0)
    if incoming_location and existing.primary_location:
        weight += 0.3
        score += 0.3 * (_similarity(incoming_location, existing.primary_location) / 100.0)
    if incoming_specialty and existing.primary_specialty:
        weight += 0.35
        score += 0.35 * (_similarity(incoming_specialty, existing.primary_specialty) / 100.0)
    if incoming_profile_url and existing.primary_profile_url:
        weight += 0.2
        score += 0.2 * (1.0 if _domain_of_url(incoming_profile_url) == _domain_of_url(existing.primary_profile_url) else 0.0)
    if incoming_designation and existing.primary_designation:
        weight += 0.2
        score += 0.2 * (_similarity(incoming_designation, existing.primary_designation) / 100.0)

    if weight == 0:
        return 0.0
    return score / weight


def _variant_name_key(
    *,
    base_key: str,
    designation: str | None,
    affiliation: str | None,
    location: str | None,
) -> str:
    fingerprint = normalize_text(" | ".join([designation or "", affiliation or "", location or ""]))
    if not fingerprint:
        fingerprint = "unknown-context"
    digest = hashlib.sha1(fingerprint.encode("utf-8", errors="ignore")).hexdigest()[:10]
    return f"{base_key}::{digest}"


def _domain_of_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        host = urlparse(url).netloc.strip().lower()
    except ValueError:
        return None
    return host or None


def _merge_signal_count(
    *,
    specialty_a: str | None,
    specialty_b: str | None,
    affiliation_a: str | None,
    affiliation_b: str | None,
    profile_url_a: str | None,
    profile_url_b: str | None,
    location_a: str | None,
    location_b: str | None,
) -> int:
    signals = 0
    if specialty_a and specialty_b and (_similarity(specialty_a, specialty_b) / 100.0) >= 0.80:
        signals += 1
    if affiliation_a and affiliation_b and (_similarity(affiliation_a, affiliation_b) / 100.0) >= 0.70:
        signals += 1
    domain_a = _domain_of_url(profile_url_a)
    domain_b = _domain_of_url(profile_url_b)
    if domain_a and domain_b and domain_a == domain_b:
        signals += 1
    if location_a and location_b and (_similarity(location_a, location_b) / 100.0) >= 0.70:
        signals += 1
    return signals


def is_physician_like(
    full_name: str,
    designation: str | None,
    affiliation: str | None,
    role: str | None,
    *,
    session_title: str | None = None,
    evidence_span: str | None = None,
) -> bool:
    blob = " ".join(
        [
            full_name or "",
            designation or "",
            affiliation or "",
            role or "",
            session_title or "",
            evidence_span or "",
        ]
    ).lower()
    return any(h in blob for h in PHYSICIAN_HINTS)


def _split_name_key(key: str) -> list[str]:
    return [part for part in key.split() if part]


def _identity_score(target: str, candidate: str) -> float:
    target_parts = _split_name_key(target)
    candidate_parts = _split_name_key(candidate)
    if len(target_parts) < 2 or len(candidate_parts) < 2:
        return 0.0

    target_first = target_parts[0]
    target_last = target_parts[-1]
    cand_first = candidate_parts[0]
    cand_last = candidate_parts[-1]

    token_score = float(fuzz.token_set_ratio(target, candidate))
    ratio_score = float(fuzz.ratio(target, candidate))
    first_score = float(fuzz.ratio(target_first, cand_first))
    last_score = float(fuzz.ratio(target_last, cand_last))
    initials_match = target_first[:1] == cand_first[:1]

    if not initials_match:
        return 0.0

    weighted = (0.45 * token_score) + (0.20 * ratio_score) + (0.20 * first_score) + (0.15 * last_score)
    return weighted


def _safe_fuzzy_match(db: Session, key: str) -> Physician | None:
    candidate_rows = db.execute(select(Physician).limit(5000)).scalars().all()
    target_parts = _split_name_key(key)
    if len(target_parts) < 2:
        return None

    best: tuple[float, Physician | None] = (0.0, None)
    for candidate in candidate_rows:
        score = _identity_score(key, candidate.name_key)
        if score > best[0]:
            best = (score, candidate)

    if best[0] >= 88.0 and best[1] is not None:
        return best[1]
    return None


def get_or_create_physician(
    db: Session,
    full_name: str,
    designation: str | None,
    affiliation: str | None,
    location: str | None,
    aliases: list[str],
    specialty: str | None = None,
    profile_url: str | None = None,
) -> Physician:
    canonical = canonicalize_person_name(
        full_name=full_name,
        designation=designation,
        role=None,
        evidence=None,
    )
    canonical_name = canonical.full_name if canonical.is_valid else full_name.strip()
    designation_out = canonical.designation or designation
    combined_aliases = list(aliases) + list(canonical.aliases)

    key = name_key(canonical_name)
    base_key = _base_name_key(key)
    same_name_candidates = db.execute(
        select(Physician).where(
            or_(
                Physician.name_key == base_key,
                Physician.name_key.like(f"{base_key}::%"),
            )
        )
    ).scalars().all()

    existing: Physician | None = None
    if same_name_candidates:
        exact_key_match = next((candidate for candidate in same_name_candidates if candidate.name_key == key), None)
        if exact_key_match is not None:
            if _has_profile_conflict(
                incoming_affiliation=affiliation,
                incoming_location=location,
                existing_affiliation=exact_key_match.primary_affiliation,
                existing_location=exact_key_match.primary_location,
            ):
                variant_key = _variant_name_key(
                    base_key=base_key,
                    designation=designation_out,
                    affiliation=affiliation,
                    location=location,
                )
                variant_match = next((candidate for candidate in same_name_candidates if candidate.name_key == variant_key), None)
                if variant_match is not None:
                    existing = variant_match
                else:
                    existing = None
                    key = variant_key
            else:
                existing = exact_key_match
        else:
            best_score = 0.0
            best_candidate: Physician | None = None
            for candidate in same_name_candidates:
                score = _profile_compatibility_score(
                    incoming_designation=designation_out,
                    incoming_affiliation=affiliation,
                    incoming_location=location,
                    incoming_specialty=specialty,
                    incoming_profile_url=profile_url,
                    existing=candidate,
                )
                if score > best_score:
                    best_score = score
                    best_candidate = candidate

            if best_candidate is not None and best_score >= 0.80:
                existing = best_candidate
            elif any(
                _has_profile_conflict(
                    incoming_affiliation=affiliation,
                    incoming_location=location,
                    existing_affiliation=candidate.primary_affiliation,
                    existing_location=candidate.primary_location,
                )
                for candidate in same_name_candidates
            ):
                key = _variant_name_key(
                    base_key=base_key,
                    designation=designation_out,
                    affiliation=affiliation,
                    location=location,
                )
                existing = next((candidate for candidate in same_name_candidates if candidate.name_key == key), None)

    if existing is None and not same_name_candidates:
        existing = _safe_fuzzy_match(db, key)

    if existing:
        if not existing.primary_designation and designation_out:
            existing.primary_designation = designation_out
        if not existing.primary_affiliation and affiliation:
            existing.primary_affiliation = affiliation
        if not existing.primary_location and location:
            existing.primary_location = location
        if not existing.primary_specialty and specialty:
            existing.primary_specialty = specialty
        if not existing.primary_profile_url and profile_url:
            existing.primary_profile_url = profile_url
        physician = existing
    else:
        physician = Physician(
            full_name=canonical_name.strip(),
            name_key=key,
            primary_designation=designation_out,
            primary_affiliation=affiliation,
            primary_location=location,
            primary_specialty=specialty,
            primary_profile_url=profile_url,
        )
        db.add(physician)
        db.flush()

    seen_aliases = {alias_key(canonical_name)}
    for alias in combined_aliases + [canonical_name]:
        cleaned = alias.strip()
        if not cleaned:
            continue
        akey = alias_key(cleaned)
        if not akey or akey in seen_aliases:
            continue
        seen_aliases.add(akey)
        existing_alias = db.execute(select(PhysicianAlias).where(PhysicianAlias.alias_key == akey)).scalar_one_or_none()
        if existing_alias:
            continue
        db.add(PhysicianAlias(physician_id=physician.id, alias=cleaned, alias_key=akey))

    return physician


@dataclass(slots=True)
class MergeStats:
    merged_physicians: int = 0
    moved_aliases: int = 0
    moved_appearances: int = 0
    duplicate_appearances_skipped: int = 0


def _move_aliases(db: Session, source: Physician, target: Physician) -> tuple[int, int]:
    moved = 0
    skipped = 0
    source_aliases = db.execute(select(PhysicianAlias).where(PhysicianAlias.physician_id == source.id)).scalars().all()
    for alias in source_aliases:
        existing = db.execute(select(PhysicianAlias).where(PhysicianAlias.alias_key == alias.alias_key)).scalar_one_or_none()
        if existing and existing.physician_id != source.id:
            db.delete(alias)
            skipped += 1
            continue
        alias.physician = target
        moved += 1
    return moved, skipped


def _move_appearances(db: Session, source: Physician, target: Physician) -> tuple[int, int]:
    moved = 0
    skipped = 0
    source_appearances = db.execute(select(Appearance).where(Appearance.physician_id == source.id)).scalars().all()
    for appearance in source_appearances:
        existing = db.execute(
            select(Appearance).where(
                and_(
                    Appearance.physician_id == target.id,
                    Appearance.conference_year_id == appearance.conference_year_id,
                    Appearance.session_title == appearance.session_title,
                )
            )
        ).scalar_one_or_none()
        if existing:
            db.delete(appearance)
            skipped += 1
            continue
        appearance.physician = target
        moved += 1
    return moved, skipped


def _merge_into(db: Session, source: Physician, target: Physician) -> MergeStats:
    stats = MergeStats()
    if not target.primary_designation and source.primary_designation:
        target.primary_designation = source.primary_designation
    if not target.primary_affiliation and source.primary_affiliation:
        target.primary_affiliation = source.primary_affiliation
    if not target.primary_location and source.primary_location:
        target.primary_location = source.primary_location
    if not target.primary_specialty and source.primary_specialty:
        target.primary_specialty = source.primary_specialty
    if not target.primary_profile_url and source.primary_profile_url:
        target.primary_profile_url = source.primary_profile_url
    if not target.bio_short and source.bio_short:
        target.bio_short = source.bio_short
    if not target.bio_source_url and source.bio_source_url:
        target.bio_source_url = source.bio_source_url
    if not target.primary_education and source.primary_education:
        target.primary_education = source.primary_education
    if not target.enrichment_confidence and source.enrichment_confidence:
        target.enrichment_confidence = source.enrichment_confidence
    if not target.enrichment_updated_at and source.enrichment_updated_at:
        target.enrichment_updated_at = source.enrichment_updated_at

    alias_moved, _ = _move_aliases(db, source, target)
    appearance_moved, appearance_skipped = _move_appearances(db, source, target)
    stats.moved_aliases += alias_moved
    stats.moved_appearances += appearance_moved
    stats.duplicate_appearances_skipped += appearance_skipped

    source_alias_key = alias_key(source.full_name)
    existing_alias = db.execute(select(PhysicianAlias).where(PhysicianAlias.alias_key == source_alias_key)).scalar_one_or_none()
    if source_alias_key and not existing_alias:
        db.add(PhysicianAlias(physician_id=target.id, alias=source.full_name, alias_key=source_alias_key))
        stats.moved_aliases += 1

    db.delete(source)
    stats.merged_physicians += 1
    return stats


def merge_close_physicians(
    db: Session,
    *,
    physician_ids: set[int] | None = None,
) -> MergeStats:
    stats = MergeStats()
    physicians = db.execute(select(Physician).order_by(Physician.id.asc())).scalars().all()
    if len(physicians) < 2:
        return stats

    active_ids = physician_ids or {row.id for row in physicians}
    by_first_initial: dict[str, list[Physician]] = {}
    for physician in physicians:
        parts = _split_name_key(physician.name_key)
        if len(parts) < 2:
            continue
        first_initial = parts[0][0]
        by_first_initial.setdefault(first_initial, []).append(physician)

    removed_ids: set[int] = set()
    for group in by_first_initial.values():
        group_sorted = sorted(group, key=lambda row: row.id)
        for idx, left in enumerate(group_sorted):
            if left.id in removed_ids:
                continue
            if left.id not in active_ids and not any(item.id in active_ids for item in group_sorted[idx + 1 :]):
                continue
            for right in group_sorted[idx + 1 :]:
                if right.id in removed_ids or left.id == right.id:
                    continue
                if left.id not in active_ids and right.id not in active_ids:
                    continue
                if _has_profile_conflict(
                    incoming_affiliation=left.primary_affiliation,
                    incoming_location=left.primary_location,
                    existing_affiliation=right.primary_affiliation,
                    existing_location=right.primary_location,
                ):
                    continue
                merge_signals = _merge_signal_count(
                    specialty_a=left.primary_specialty,
                    specialty_b=right.primary_specialty,
                    affiliation_a=left.primary_affiliation,
                    affiliation_b=right.primary_affiliation,
                    profile_url_a=left.primary_profile_url,
                    profile_url_b=right.primary_profile_url,
                    location_a=left.primary_location,
                    location_b=right.primary_location,
                )
                if merge_signals < 2:
                    continue
                score = _identity_score(_base_name_key(left.name_key), _base_name_key(right.name_key))
                if score < 88.0:
                    continue
                target, source = (left, right) if left.id < right.id else (right, left)
                merge_stats = _merge_into(db, source=source, target=target)
                stats.merged_physicians += merge_stats.merged_physicians
                stats.moved_aliases += merge_stats.moved_aliases
                stats.moved_appearances += merge_stats.moved_appearances
                stats.duplicate_appearances_skipped += merge_stats.duplicate_appearances_skipped
                removed_ids.add(source.id)
                active_ids.add(target.id)
                db.flush()

    return stats

from __future__ import annotations

import math
from datetime import datetime, timezone
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.entities import NavigationTemplateMemory


def registrable_domain(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    parts = [part for part in host.split(".") if part]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _memory_score(
    row: NavigationTemplateMemory,
    *,
    now: datetime,
    decay_days: int,
    min_visits: int,
) -> float:
    visits = max(1, int(row.visits or 0))
    speaker_rate = float(row.speaker_hits or 0) / float(visits)
    appearance_rate = float(row.appearance_hits or 0) / float(visits)
    base = (0.6 * speaker_rate) + (0.4 * appearance_rate)

    if row.last_seen_at is not None and decay_days > 0:
        age_days = max(0.0, (now - row.last_seen_at).total_seconds() / 86400.0)
        decay = math.exp(-age_days / float(decay_days))
    else:
        decay = 1.0

    confidence_boost = 0.0
    if visits >= max(1, int(min_visits)):
        confidence_boost = min(0.15, (visits - min_visits) * 0.01)

    zero_penalty = min(0.5, float(max(0, int(row.zero_yield_streak or 0))) * 0.08)
    return _clamp((base * decay) + confidence_boost - zero_penalty)


def get_template_memory_scores(
    db: Session,
    *,
    domain: str,
    template_keys: list[str],
    decay_days: int,
    min_visits: int,
) -> dict[str, float]:
    keys = sorted({(key or "").strip() for key in template_keys if (key or "").strip()})
    if not keys:
        return {}

    rows = db.execute(
        select(NavigationTemplateMemory).where(
            NavigationTemplateMemory.domain == domain,
            NavigationTemplateMemory.template_key.in_(keys),
        )
    ).scalars().all()

    now = datetime.now(timezone.utc)
    out: dict[str, float] = {}
    for row in rows:
        out[row.template_key] = _memory_score(
            row,
            now=now,
            decay_days=max(1, int(decay_days)),
            min_visits=max(1, int(min_visits)),
        )
    return out


def update_template_memory(
    db: Session,
    *,
    domain: str,
    template_key: str,
    intent: str | None,
    speaker_hit: bool,
    appearance_hit: bool,
) -> NavigationTemplateMemory:
    now = datetime.now(timezone.utc)
    row = db.execute(
        select(NavigationTemplateMemory).where(
            NavigationTemplateMemory.domain == domain,
            NavigationTemplateMemory.template_key == template_key,
        )
    ).scalar_one_or_none()

    if row is None:
        row = NavigationTemplateMemory(
            domain=domain,
            template_key=template_key,
            intent=(intent or "")[:64] or None,
            visits=0,
            speaker_hits=0,
            appearance_hits=0,
            zero_yield_streak=0,
            last_seen_at=now,
            updated_at=now,
        )
        db.add(row)
        db.flush()

    row.visits = int(row.visits or 0) + 1
    if speaker_hit:
        row.speaker_hits = int(row.speaker_hits or 0) + 1
    if appearance_hit:
        row.appearance_hits = int(row.appearance_hits or 0) + 1

    if speaker_hit or appearance_hit:
        row.zero_yield_streak = 0
    else:
        row.zero_yield_streak = int(row.zero_yield_streak or 0) + 1

    if intent:
        row.intent = intent[:64]
    row.last_seen_at = now
    row.updated_at = now
    return row

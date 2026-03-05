from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse


UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
HEX_RE = re.compile(r"^[0-9a-f]{10,}$", re.I)
NUMERIC_RE = re.compile(r"^\d+$")


@dataclass(slots=True)
class BranchStats:
    pages_seen: int = 0
    new_candidates: int = 0
    new_normalized: int = 0
    new_links: int = 0
    new_physicians: int = 0
    linked_appearances: int = 0


def _normalized_path_segment(segment: str) -> str:
    value = segment.strip().lower()
    if not value:
        return ""
    if NUMERIC_RE.match(value):
        return "{num}"
    if UUID_RE.match(value):
        return "{uuid}"
    if HEX_RE.match(value):
        return "{id}"
    return value


def template_key_for_url(url: str) -> str:
    parsed = urlparse(url)
    segments = [_normalized_path_segment(part) for part in parsed.path.split("/") if part]
    if not segments:
        return "/"
    return "/" + "/".join(segments)


def branch_id_for_url(url: str, hint: str | None = None) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if hint:
        normalized_hint = _normalized_path_segment(hint.strip().strip("/"))
    else:
        normalized_hint = ""
    template = template_key_for_url(url)
    suffix = normalized_hint or template
    return f"{host}::{suffix}"


def branch_yield_score(stats: BranchStats | None) -> float:
    if stats is None or stats.pages_seen <= 0:
        return 0.0
    candidate_rate = min(1.0, float(stats.new_candidates) / float(max(stats.pages_seen * 10, 1)))
    normalize_rate = min(1.0, float(stats.new_normalized) / float(max(stats.pages_seen * 3, 1)))
    physician_rate = min(1.0, float(stats.new_physicians) / float(max(stats.pages_seen * 2, 1)))
    link_rate = min(1.0, float(stats.new_links) / float(max(stats.pages_seen * 8, 1)))
    appearance_rate = min(1.0, float(stats.linked_appearances) / float(max(stats.pages_seen * 2, 1)))
    score = (
        0.30 * candidate_rate
        + 0.25 * normalize_rate
        + 0.20 * physician_rate
        + 0.15 * link_rate
        + 0.10 * appearance_rate
    )
    return max(0.0, min(1.0, score))


def frontier_priority(
    *,
    llm_priority: float,
    branch_stats: BranchStats | None,
    novelty_score: float,
    depth: int,
) -> float:
    bounded_llm = max(0.0, min(1.0, llm_priority))
    bounded_novelty = max(0.0, min(1.0, novelty_score))
    bounded_depth = max(0, depth)
    branch_score = branch_yield_score(branch_stats)
    score = (0.45 * bounded_llm) + (0.30 * branch_score) + (0.20 * bounded_novelty) - (0.05 * float(bounded_depth))
    return max(0.01, min(1.0, score))

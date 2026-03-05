from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings


@dataclass(slots=True)
class ModalBreakerDecision:
    should_attempt: bool
    reason: str
    dynamic_signal: bool


MODAL_PAGE_INTENTS = {"gatekeeper", "listing", "session_detail"}
MODAL_DYNAMIC_HINTS = (
    "aria-expanded",
    "aria-controls",
    "role=\"tab\"",
    "role='tab'",
    "accordion",
    "expand",
    "collapse",
    "toggle",
    "view session",
    "view faculty",
    "load more",
    "show more",
    "session-toggle",
    "speaker-toggle",
)


def expand_all_js_script() -> str:
    return """
(() => {
  const selectors = [
    '.session-toggle',
    '.view-faculty',
    '.expand-all',
    '[data-action*="expand"]',
    '[data-toggle*="collapse"]',
    '[aria-expanded="false"]',
    '[role="tab"]',
    'button',
    'a[role="button"]'
  ];
  const clicked = new Set();
  for (const selector of selectors) {
    const nodes = document.querySelectorAll(selector);
    nodes.forEach((node) => {
      if (!(node instanceof HTMLElement)) return;
      if (node.offsetParent === null) return;
      const label = (node.innerText || node.getAttribute('aria-label') || '').toLowerCase();
      if (!label) return;
      if (!/(speaker|faculty|session|agenda|program|plenary|abstract|expand|show|view)/.test(label)) return;
      if (clicked.has(node)) return;
      clicked.add(node);
      try { node.click(); } catch (_) {}
    });
  }
})();
"""


def has_dynamic_speaker_signals(*, html_snapshot: str, title: str, summary_text: str, url: str) -> bool:
    blob = " ".join([url.lower(), title.lower(), summary_text.lower(), (html_snapshot or "").lower()[:80000]])
    return any(hint in blob for hint in MODAL_DYNAMIC_HINTS)


def should_attempt_modal_breaker(
    settings: Settings,
    *,
    page_intent: str,
    candidate_count: int,
    normalized_count: int,
    already_attempted: bool,
    html_snapshot: str,
    title: str,
    summary_text: str,
    url: str,
) -> ModalBreakerDecision:
    if not settings.modal_breaker_enabled:
        return ModalBreakerDecision(False, "disabled", False)
    if already_attempted:
        return ModalBreakerDecision(False, "already_attempted", False)
    if page_intent not in MODAL_PAGE_INTENTS:
        return ModalBreakerDecision(False, f"intent_{page_intent}_not_targeted", False)

    dynamic_signal = has_dynamic_speaker_signals(
        html_snapshot=html_snapshot,
        title=title,
        summary_text=summary_text,
        url=url,
    )
    if not dynamic_signal:
        return ModalBreakerDecision(False, "no_dynamic_signal", False)

    min_candidates = max(1, int(settings.modal_breaker_min_candidates))
    if candidate_count >= min_candidates or normalized_count > 0:
        return ModalBreakerDecision(False, "baseline_yield_sufficient", dynamic_signal)

    return ModalBreakerDecision(True, "low_yield_dynamic_page", dynamic_signal)


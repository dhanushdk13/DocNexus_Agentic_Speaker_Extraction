from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from app.config import Settings
from app.services.extract_candidates import extract_internal_links, extract_visible_text
from app.services.fetchers import PlaywrightDomainSessionManager


INTERACTION_SELECTORS = [
    "main [role='tab']",
    "main button",
    "main [aria-controls]",
    "main [aria-expanded]",
    "main a[href]",
    "[role='tab']",
    "button",
]


@dataclass(slots=True)
class InteractionActionResult:
    label: str
    selector: str
    index: int
    clicked: bool
    url_after: str
    discovered_links: int
    text_delta: int


@dataclass(slots=True)
class InteractionExploreResult:
    actions_total: int = 0
    actions_with_novelty: int = 0
    discovered_links: list[dict[str, str]] = field(default_factory=list)
    interaction_blocks: list[str] = field(default_factory=list)
    network_payloads: list[dict[str, Any]] = field(default_factory=list)
    actions: list[InteractionActionResult] = field(default_factory=list)
    stop_reason: str = "not_started"


def _effective_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _same_site(seed_url: str, candidate_url: str) -> bool:
    return _effective_domain(seed_url) == _effective_domain(candidate_url)


def _canonical_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "https").lower()
    host = parsed.netloc.lower()
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return f"{scheme}://{host}{path}"


def _safe_json_from_text(value: str) -> Any | None:
    text = value.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


async def _collect_response_json(resp) -> Any | None:  # noqa: ANN001
    try:
        raw = await resp.body()
    except Exception:
        raw = b""
    if raw:
        parsed = _safe_json_from_text(raw.decode("utf-8", errors="ignore"))
        if parsed is not None:
            return parsed
    try:
        return await resp.json()
    except Exception:
        return None


async def explore_interactions(
    settings: Settings,
    *,
    url: str,
    seed_url: str,
    session_manager: PlaywrightDomainSessionManager,
    known_canonical_urls: set[str],
    max_actions_per_page: int | None = None,
    no_novelty_limit: int | None = None,
) -> InteractionExploreResult:
    result = InteractionExploreResult(stop_reason="disabled")
    if not settings.interaction_explorer_enabled:
        return result
    if settings.app_env.lower() == "test":
        result.stop_reason = "test_env_disabled"
        return result

    actions_cap = max_actions_per_page or max(1, int(settings.interaction_max_actions_per_page))
    novelty_limit = no_novelty_limit or max(1, int(settings.interaction_no_novelty_limit))
    context = await session_manager.context_for_url(url)
    page = await context.new_page()
    network_payloads: list[dict[str, Any]] = []
    discovered_links: dict[str, dict[str, str]] = {}
    seen_text_fingerprints: set[str] = set()

    try:
        async def on_response(resp):  # noqa: ANN001
            request = resp.request
            if request.resource_type not in {"xhr", "fetch"}:
                return
            parsed = await _collect_response_json(resp)
            if parsed is None:
                return
            network_payloads.append({"url": resp.url, "status": resp.status, "data": parsed})

        page.on("response", on_response)
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(1200)
        baseline_html = await page.content()
        baseline_links = {_canonical_url(item["url"]) for item in extract_internal_links(baseline_html, page.url, max_links=180)}

        no_novelty_streak = 0
        for selector in INTERACTION_SELECTORS:
            if result.actions_total >= actions_cap:
                break
            locator = page.locator(selector)
            try:
                count = await locator.count()
            except Exception:
                continue
            if count <= 0:
                continue
            for idx in range(min(count, actions_cap * 2)):
                if result.actions_total >= actions_cap:
                    break
                element = locator.nth(idx)
                label = ""
                try:
                    label = (await element.inner_text(timeout=1200)).strip()
                except Exception:
                    label = ""
                if not label:
                    try:
                        label = (await element.get_attribute("aria-label")) or ""
                    except Exception:
                        label = ""
                label = " ".join(label.split())[:120]
                if len(label) < 2:
                    continue

                before_url = page.url
                before_text = extract_visible_text(await page.content(), max_chars=6000)
                try:
                    await element.click(timeout=2500)
                except Exception:
                    continue
                result.actions_total += 1
                await page.wait_for_timeout(900)

                html_after = await page.content()
                text_after = extract_visible_text(html_after, max_chars=8000)
                text_fingerprint = text_after[:600]
                if text_fingerprint:
                    seen_text_fingerprints.add(text_fingerprint)

                link_rows = extract_internal_links(html_after, page.url, max_links=220)
                current_links = {_canonical_url(item["url"]) for item in link_rows}
                added_canonical = [
                    canonical
                    for canonical in current_links
                    if canonical not in baseline_links and canonical not in known_canonical_urls
                ]
                for row in link_rows:
                    canonical = _canonical_url(row["url"])
                    if canonical not in added_canonical:
                        continue
                    if not _same_site(seed_url, row["url"]):
                        continue
                    discovered_links[canonical] = {
                        "url": row["url"],
                        "text": row.get("anchor", ""),
                        "context": f"interaction:{label[:60]}",
                    }

                text_delta = max(0, len(text_after) - len(before_text))
                has_novelty = bool(added_canonical) or text_delta >= 180
                if has_novelty:
                    result.actions_with_novelty += 1
                    no_novelty_streak = 0
                    if text_after and len(text_after) >= 80:
                        block = f"[BLOCK interaction]\n{text_after[:1200]}"
                        result.interaction_blocks.append(block)
                else:
                    no_novelty_streak += 1

                result.actions.append(
                    InteractionActionResult(
                        label=label,
                        selector=selector,
                        index=idx,
                        clicked=True,
                        url_after=page.url,
                        discovered_links=len(added_canonical),
                        text_delta=text_delta,
                    )
                )
                if page.url != before_url:
                    try:
                        await page.go_back(wait_until="domcontentloaded", timeout=12000)
                    except Exception:
                        try:
                            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        except Exception:
                            pass
                    await page.wait_for_timeout(800)

                if no_novelty_streak >= novelty_limit:
                    result.stop_reason = "no_novelty_limit"
                    break
            if no_novelty_streak >= novelty_limit:
                break

        if not result.stop_reason or result.stop_reason == "disabled":
            result.stop_reason = "action_cap_reached" if result.actions_total >= actions_cap else "selectors_exhausted"
    except Exception:
        result.stop_reason = "interaction_error"
    finally:
        try:
            await page.close()
        except Exception:
            pass

    result.discovered_links = list(discovered_links.values())[:200]
    result.network_payloads = network_payloads[:200]
    dedup_blocks: dict[str, None] = {}
    for block in result.interaction_blocks:
        dedup_blocks[block] = None
    result.interaction_blocks = list(dedup_blocks.keys())[:80]
    return result

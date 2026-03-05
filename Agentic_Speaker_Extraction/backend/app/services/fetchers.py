from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx
from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright

from app.config import get_settings
from app.models.enums import SourceMethod

try:
    from playwright_stealth import Stealth
except Exception:  # pragma: no cover
    Stealth = None  # type: ignore[assignment]

try:
    from seleniumbase import SB
except Exception:  # pragma: no cover
    SB = None  # type: ignore[assignment]


PLATFORM_HINTS = [
    "cvent",
    "swapcard",
    "whova",
    "grip.events",
    "eventsair",
    "mapyourshow",
    "rainfocus",
]


@dataclass(slots=True)
class FetchResult:
    url: str
    method: SourceMethod
    fetch_status: str
    http_status: int | None
    content_type: str
    text: str
    blocked: bool
    network_payloads: list[dict[str, Any]] = field(default_factory=list)


def _map_same_site(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    lowered = value.strip().lower()
    if lowered == "strict":
        return "Strict"
    if lowered == "none":
        return "None"
    if lowered == "lax":
        return "Lax"
    return None


def _selenium_cookies_to_storage_state(cookies: list[dict[str, Any]]) -> dict[str, Any]:
    converted: list[dict[str, Any]] = []
    for cookie in cookies:
        name = str(cookie.get("name", "")).strip()
        value = str(cookie.get("value", "")).strip()
        domain = str(cookie.get("domain", "")).strip()
        if not (name and value and domain):
            continue

        converted_cookie: dict[str, Any] = {
            "name": name,
            "value": value,
            "domain": domain,
            "path": str(cookie.get("path", "/") or "/"),
            "expires": float(cookie.get("expiry", -1) or -1),
            "httpOnly": bool(cookie.get("httpOnly", False)),
            "secure": bool(cookie.get("secure", False)),
        }
        same_site = _map_same_site(cookie.get("sameSite"))
        if same_site:
            converted_cookie["sameSite"] = same_site
        converted.append(converted_cookie)

    return {"cookies": converted, "origins": []}


def _bootstrap_storage_state_via_selenium(url: str, timeout_seconds: int) -> tuple[dict[str, Any] | None, str]:
    if SB is None:
        return None, "seleniumbase_not_installed"

    try:
        with SB(uc=True, headless=True) as sb:
            sb.open(url)
            sb.wait_for_ready_state_complete(timeout=max(timeout_seconds, 5))
            sb.sleep(2)
            cookies = sb.driver.get_cookies()
    except Exception as exc:  # noqa: BLE001
        return None, f"bootstrap_failed:{type(exc).__name__}"

    if not cookies:
        return None, "no_cookies_captured"

    storage_state = _selenium_cookies_to_storage_state(cookies)
    if not storage_state["cookies"]:
        return None, "no_valid_cookies"
    return storage_state, "ok"


class PlaywrightDomainSessionManager:
    def __init__(self, *, enable_stealth: bool | None = None) -> None:
        settings = get_settings()
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._contexts: dict[str, BrowserContext] = {}
        if enable_stealth is None:
            enable_stealth = bool(settings.playwright_stealth_enabled)
        self._enable_stealth = enable_stealth
        self._stealth = Stealth() if self._enable_stealth and Stealth is not None else None
        self._enable_selenium_bootstrap = bool(settings.selenium_bootstrap_enabled)
        self._selenium_bootstrap_timeout = int(settings.selenium_bootstrap_timeout_seconds)
        self._bootstrap_results: dict[str, dict[str, Any]] = {}

    async def _start(self) -> None:
        if self._playwright is not None and self._browser is not None:
            return
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)

    async def context_for_url(self, url: str) -> BrowserContext:
        await self._start()
        assert self._browser is not None
        domain = urlparse(url).netloc.lower()
        context = self._contexts.get(domain)
        if context is not None:
            return context

        storage_state: dict[str, Any] | None = None
        if self._enable_selenium_bootstrap:
            state, reason = await asyncio.to_thread(
                _bootstrap_storage_state_via_selenium,
                url,
                self._selenium_bootstrap_timeout,
            )
            if state is not None:
                storage_state = state
                self._bootstrap_results[domain] = {
                    "attempted": True,
                    "success": True,
                    "reason": reason,
                    "cookies": len(state.get("cookies", [])),
                }
            else:
                self._bootstrap_results[domain] = {
                    "attempted": True,
                    "success": False,
                    "reason": reason,
                    "cookies": 0,
                }
        else:
            self._bootstrap_results[domain] = {
                "attempted": False,
                "success": False,
                "reason": "disabled",
                "cookies": 0,
            }

        if storage_state is not None:
            context = await self._browser.new_context(storage_state=storage_state)
        else:
            context = await self._browser.new_context()
        if self._stealth is not None:
            try:
                await self._stealth.apply_stealth_async(context)
            except Exception:
                pass
        self._contexts[domain] = context
        return context

    def bootstrap_status_for_url(self, url: str) -> dict[str, Any]:
        domain = urlparse(url).netloc.lower()
        return self._bootstrap_results.get(
            domain,
            {
                "attempted": False,
                "success": False,
                "reason": "not_attempted",
                "cookies": 0,
            },
        )

    async def close(self) -> None:
        for context in self._contexts.values():
            try:
                await context.close()
            except Exception:
                continue
        self._contexts.clear()

        if self._browser is not None:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None

        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None


def detect_blocked(status_code: int | None, text: str) -> bool:
    _ = status_code
    _ = text
    return False


def is_platform_url(url: str) -> bool:
    lowered = url.lower()
    host = urlparse(url).netloc.lower()
    return any(hint in lowered or hint in host for hint in PLATFORM_HINTS)


def choose_fetch_method(url: str, content_type_hint: str | None = None) -> SourceMethod:
    lowered = url.lower()
    if lowered.endswith(".pdf") or (content_type_hint and "pdf" in content_type_hint.lower()):
        return SourceMethod.pdf_text
    if is_platform_url(url):
        return SourceMethod.playwright_network
    return SourceMethod.http_static


def _normalize_content_type(raw: str, url: str) -> str:
    lowered = (raw or "").lower()
    if "application/pdf" in lowered or url.lower().endswith(".pdf"):
        return "pdf"
    if "json" in lowered:
        return "json"
    if "html" in lowered or not lowered:
        return "html"
    return "unknown"


async def fetch_http(url: str, timeout_seconds: float = 15.0) -> FetchResult:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
        )
    }
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
            response = await client.get(url)
    except httpx.HTTPError:
        return FetchResult(
            url=url,
            method=SourceMethod.http_static,
            fetch_status="error",
            http_status=None,
            content_type="unknown",
            text="",
            blocked=False,
        )

    content_type = _normalize_content_type(response.headers.get("content-type", ""), url)

    if content_type == "pdf":
        text = response.content.decode("latin-1", errors="ignore")
    else:
        text = response.text

    has_http_error = response.status_code >= 400

    return FetchResult(
        url=url,
        method=SourceMethod.http_static,
        fetch_status="error" if has_http_error else "fetched",
        http_status=response.status_code,
        content_type=content_type,
        text=text,
        blocked=False,
    )


def _safe_json_from_text(text: str) -> Any | None:
    candidate = text.strip()
    if not candidate:
        return None
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        return None


async def _collect_response_json(resp) -> Any | None:  # noqa: ANN001
    try:
        body_bytes = await resp.body()
    except Exception:
        body_bytes = b""

    if body_bytes:
        decoded = body_bytes.decode("utf-8", errors="ignore")
        parsed = _safe_json_from_text(decoded)
        if parsed is not None:
            return parsed

    try:
        return await resp.json()
    except Exception:
        return None


async def _playwright_fetch_with_context(
    context: BrowserContext,
    url: str,
    *,
    capture_network: bool,
    goto_timeout_seconds: float = 30.0,
    total_timeout_seconds: float = 90.0,
    scroll_iterations: int = 6,
) -> FetchResult:
    network_payloads: list[dict[str, Any]] = []
    started = time.monotonic()

    page = await context.new_page()
    try:
        async def on_response(resp):  # noqa: ANN001
            if not capture_network:
                return
            request = resp.request
            if request.resource_type not in {"xhr", "fetch"}:
                return

            parsed = await _collect_response_json(resp)
            if parsed is None:
                return

            network_payloads.append(
                {
                    "url": resp.url,
                    "status": resp.status,
                    "data": parsed,
                }
            )

        page.on("response", on_response)

        response = await page.goto(url, wait_until="domcontentloaded", timeout=int(goto_timeout_seconds * 1000))
        await page.wait_for_timeout(1500)

        prev_height = -1
        prev_text_len = -1
        stagnation = 0

        for _ in range(scroll_iterations):
            if time.monotonic() - started >= total_timeout_seconds:
                break

            metrics = await page.evaluate(
                """() => {
                    const body = document.body;
                    const text = body ? body.innerText || '' : '';
                    const height = Math.max(
                      document.documentElement ? document.documentElement.scrollHeight : 0,
                      body ? body.scrollHeight : 0
                    );
                    return {height, textLength: text.length};
                }"""
            )

            current_height = int(metrics.get("height", 0) or 0)
            current_text_len = int(metrics.get("textLength", 0) or 0)

            if current_height <= prev_height and current_text_len <= prev_text_len:
                stagnation += 1
            else:
                stagnation = 0

            if stagnation >= 2:
                break

            prev_height = current_height
            prev_text_len = current_text_len

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

        html = await page.content()
        status = response.status if response else None
        blocked = detect_blocked(status, html)

        return FetchResult(
            url=url,
            method=SourceMethod.playwright_network if capture_network else SourceMethod.playwright_dom,
            fetch_status="blocked" if blocked else "fetched",
            http_status=status,
            content_type="json" if network_payloads else "html",
            text=html,
            blocked=blocked,
            network_payloads=network_payloads,
        )
    except Exception:
        return FetchResult(
            url=url,
            method=SourceMethod.playwright_network if capture_network else SourceMethod.playwright_dom,
            fetch_status="error",
            http_status=None,
            content_type="unknown",
            text="",
            blocked=False,
            network_payloads=[],
        )
    finally:
        try:
            await page.close()
        except Exception:
            pass


async def _playwright_fetch(
    url: str,
    *,
    capture_network: bool,
    session_manager: PlaywrightDomainSessionManager | None = None,
    goto_timeout_seconds: float = 30.0,
    total_timeout_seconds: float = 90.0,
    scroll_iterations: int = 6,
) -> FetchResult:
    if session_manager is not None:
        context = await session_manager.context_for_url(url)
        return await _playwright_fetch_with_context(
            context,
            url,
            capture_network=capture_network,
            goto_timeout_seconds=goto_timeout_seconds,
            total_timeout_seconds=total_timeout_seconds,
            scroll_iterations=scroll_iterations,
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        try:
            return await _playwright_fetch_with_context(
                context,
                url,
                capture_network=capture_network,
                goto_timeout_seconds=goto_timeout_seconds,
                total_timeout_seconds=total_timeout_seconds,
                scroll_iterations=scroll_iterations,
            )
        finally:
            await context.close()
            await browser.close()


async def fetch_playwright_dom(
    url: str,
    *,
    session_manager: PlaywrightDomainSessionManager | None = None,
) -> FetchResult:
    return await _playwright_fetch(url, capture_network=False, session_manager=session_manager)


async def fetch_playwright_network(
    url: str,
    *,
    session_manager: PlaywrightDomainSessionManager | None = None,
) -> FetchResult:
    return await _playwright_fetch(url, capture_network=True, session_manager=session_manager)


async def fetch_source(
    url: str,
    method: SourceMethod,
    *,
    session_manager: PlaywrightDomainSessionManager | None = None,
) -> FetchResult:
    if method == SourceMethod.playwright_dom:
        return await fetch_playwright_dom(url, session_manager=session_manager)
    if method == SourceMethod.playwright_network:
        return await fetch_playwright_network(url, session_manager=session_manager)
    if method == SourceMethod.pdf_text:
        result = await fetch_http(url, timeout_seconds=15.0)
        result.method = SourceMethod.pdf_text
        if result.content_type != "pdf":
            result.content_type = "pdf"
        return result

    result = await fetch_http(url, timeout_seconds=15.0)
    if result.fetch_status != "fetched":
        return result

    # Thin HTML fallback once to Playwright DOM capture.
    if result.content_type == "html" and len(result.text.strip()) < 1200:
        fallback = await fetch_playwright_dom(url, session_manager=session_manager)
        if fallback.fetch_status == "fetched" and len(fallback.text.strip()) > len(result.text.strip()):
            return fallback

    return result


def serialize_network_payloads(payloads: list[dict[str, Any]]) -> str:
    return json.dumps(payloads, ensure_ascii=True)

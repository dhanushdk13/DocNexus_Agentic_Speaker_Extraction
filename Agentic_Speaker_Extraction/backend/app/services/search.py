from __future__ import annotations

import asyncio
from dataclasses import dataclass
from serpapi import GoogleSearch

from app.config import get_settings


@dataclass(slots=True)
class SearchResult:
    url: str
    title: str
    snippet: str


def build_queries(conference_name: str, year: int) -> list[str]:
    return [
        f'"{conference_name}" {year} speakers',
        f'"{conference_name}" {year} faculty',
        f'"{conference_name}" {year} program agenda schedule',
        f'"{conference_name}" {year} filetype:pdf program OR agenda OR brochure',
        f'"{conference_name}" {year} keynote plenary speakers',
    ]


def _search_serpapi(params: dict[str, str], timeout_seconds: float = 12.0) -> dict:
    try:
        search = GoogleSearch(params)
        search.timeout = timeout_seconds
        result = search.get_dict()
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


async def search_duckduckgo(conference_name: str, year: int, max_results: int = 30) -> list[SearchResult]:
    settings = get_settings()
    if not settings.serpapi_api_key:
        return []

    dedup: dict[str, SearchResult] = {}
    for query in build_queries(conference_name, year):
        if len(dedup) >= max_results:
            break

        params = {
            "q": query,
            "location": settings.serpapi_location,
            "google_domain": settings.serpapi_google_domain,
            "hl": settings.serpapi_hl,
            "gl": settings.serpapi_gl,
            "api_key": settings.serpapi_api_key,
        }
        payload = await asyncio.to_thread(_search_serpapi, params)
        error_message = payload.get("error")
        if isinstance(error_message, str) and error_message.strip():
            raise RuntimeError(f"SerpAPI error: {error_message.strip()}")
        organic_results = payload.get("organic_results", [])
        if not isinstance(organic_results, list):
            continue

        for item in organic_results:
            if len(dedup) >= max_results:
                break
            if not isinstance(item, dict):
                continue

            url = str(item.get("link", "")).strip()
            if not url or url in dedup:
                continue
            title = str(item.get("title", "")).strip()
            snippet = str(item.get("snippet", "")).strip()
            dedup[url] = SearchResult(url=url, title=title, snippet=snippet)

    return list(dedup.values())[:max_results]

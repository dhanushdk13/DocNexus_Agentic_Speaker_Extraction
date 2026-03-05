from __future__ import annotations

import pytest

from app.config import Settings
from app.services.page_reasoner import extract_and_decide


@pytest.mark.asyncio
async def test_page_reasoner_falls_back_without_api_key() -> None:
    settings = Settings(deepseek_api_key="", markdown_reasoner_max_chars=2000, markdown_segment_chars=1000)
    result = await extract_and_decide(
        settings,
        seed_url="https://example.org/conferences/test",
        page_url="https://example.org/conferences/test",
        title="Test Conference",
        top_headings=["Program"],
        markdown_text="Session A John Doe MD",
        internal_links=[{"url": "https://example.org/conferences/test/program", "text": "Program", "context": "nav"}],
        pdf_links=[],
        conference_context={"conference_name_hint": "Test"},
        visited_urls=set(),
        max_next_urls=8,
    )
    assert result.debug.used_fallback is True
    assert result.debug.fallback_reason == "missing_api_key"
    assert result.speakers == []

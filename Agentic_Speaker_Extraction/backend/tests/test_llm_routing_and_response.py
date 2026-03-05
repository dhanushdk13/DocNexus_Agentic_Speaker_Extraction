from app.config import Settings
from app.services.llm_response import extract_json_object, extract_json_payload, extract_message_text
from app.services.llm_routing import select_llm_model, select_llm_timeout


def _settings(**overrides):
    base = {
        "deepseek_model": "deepseek-chat",
        "deepseek_reasoning_enabled": True,
        "deepseek_reasoning_model": "deepseek-reasoner",
        "deepseek_reasoning_timeout_seconds": 240,
        "deepseek_reasoning_preflight": True,
        "deepseek_reasoning_identity": True,
        "deepseek_reasoning_triage": True,
        "deepseek_reasoning_extraction": True,
        "deepseek_reasoning_talk_brief": True,
        "deepseek_reasoning_navigation": True,
        "deepseek_reasoning_attribution": True,
        "deepseek_reasoning_enrichment": True,
    }
    base.update(overrides)
    return Settings(**base)


def test_select_llm_model_uses_reasoner_for_enabled_stage():
    settings = _settings()
    assert select_llm_model(settings, stage="extraction") == "deepseek-reasoner"


def test_select_llm_model_falls_back_when_stage_disabled():
    settings = _settings(deepseek_reasoning_extraction=False)
    assert select_llm_model(settings, stage="extraction") == "deepseek-chat"


def test_select_llm_timeout_uses_reasoning_timeout_floor():
    settings = _settings()
    # default is lower than reasoning timeout; routing should elevate it.
    assert select_llm_timeout(settings, stage="triage", default_timeout_seconds=30.0) == 240.0


def test_extract_message_text_falls_back_to_reasoning_content():
    body = {
        "choices": [
            {
                "message": {
                    "content": "",
                    "reasoning_content": '{"ok": true}',
                }
            }
        ]
    }
    assert extract_message_text(body) == '{"ok": true}'


def test_extract_json_object_from_fenced_block():
    text = "```json\n{\"a\": 1, \"b\": 2}\n```"
    assert extract_json_object(text) == {"a": 1, "b": 2}


def test_extract_json_payload_from_fenced_array():
    text = "```json\n[{\"name\": \"A\"}, {\"name\": \"B\"}]\n```"
    assert extract_json_payload(text) == [{"name": "A"}, {"name": "B"}]

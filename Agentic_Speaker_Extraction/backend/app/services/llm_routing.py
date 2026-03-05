from __future__ import annotations

from typing import Literal

from app.config import Settings

LlmStage = Literal[
    "preflight",
    "identity",
    "triage",
    "extraction",
    "talk_brief",
    "navigation",
    "attribution",
    "enrichment",
    "default",
]


def select_llm_model(settings: Settings, *, stage: LlmStage) -> str:
    reasoning_model = (settings.deepseek_reasoning_model or "").strip()
    if not settings.deepseek_reasoning_enabled or not reasoning_model:
        return settings.deepseek_model

    if stage == "preflight" and settings.deepseek_reasoning_preflight:
        return reasoning_model
    if stage == "identity" and settings.deepseek_reasoning_identity:
        return reasoning_model
    if stage == "triage" and settings.deepseek_reasoning_triage:
        return reasoning_model
    if stage == "extraction" and settings.deepseek_reasoning_extraction:
        return reasoning_model
    if stage == "talk_brief" and settings.deepseek_reasoning_talk_brief:
        return reasoning_model
    if stage == "navigation" and settings.deepseek_reasoning_navigation:
        return reasoning_model
    if stage == "attribution" and settings.deepseek_reasoning_attribution:
        return reasoning_model
    if stage == "enrichment" and settings.deepseek_reasoning_enrichment:
        return reasoning_model
    return settings.deepseek_model


def select_llm_timeout(settings: Settings, *, stage: LlmStage, default_timeout_seconds: float) -> float:
    timeout = float(default_timeout_seconds)
    model = select_llm_model(settings, stage=stage)
    reasoning_model = (settings.deepseek_reasoning_model or "").strip()
    if reasoning_model and model == reasoning_model:
        timeout = max(timeout, float(settings.deepseek_reasoning_timeout_seconds or timeout))
    return timeout

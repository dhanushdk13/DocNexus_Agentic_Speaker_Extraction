from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_url: str = "postgresql://dhanush:password@localhost:5432/test_scraper"
    deepseek_api_key: str = ""
    deepseek_model: str = "deepseek-chat"
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_reasoning_enabled: bool = False
    deepseek_reasoning_model: str = "deepseek-reasoner"
    deepseek_reasoning_timeout_seconds: int = 240
    deepseek_reasoning_preflight: bool = False
    deepseek_reasoning_identity: bool = False
    deepseek_reasoning_triage: bool = False
    deepseek_reasoning_extraction: bool = True
    deepseek_reasoning_talk_brief: bool = False
    deepseek_reasoning_navigation: bool = True
    deepseek_reasoning_attribution: bool = False
    deepseek_reasoning_enrichment: bool = False
    serpapi_api_key: str = ""
    serpapi_location: str = "India"
    serpapi_google_domain: str = "google.co.in"
    serpapi_hl: str = "hi"
    serpapi_gl: str = "in"
    app_env: str = "development"
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    seed_speaker_stop_threshold: int = 1
    additional_link_budget: int = 2
    domain_min_delay_seconds: float = 2.0
    domain_max_delay_seconds: float = 4.0
    domain_max_pages_per_minute: int = 10
    domain_max_urls: int = 8
    domain_block_threshold: int = 2
    domain_block_cooldown_min_seconds: int = 300
    domain_block_cooldown_max_seconds: int = 600
    playwright_stealth_enabled: bool = True
    selenium_bootstrap_enabled: bool = True
    selenium_bootstrap_timeout_seconds: int = 25
    crawl4ai_enabled: bool = True
    crawl4ai_timeout_seconds: int = 30
    nav_max_pages_per_domain: int = 16
    nav_max_pages_per_domain_hard: int = 120
    nav_budget_expand_step: int = 8
    nav_budget_expand_window: int = 5
    nav_budget_expand_candidate_threshold: int = 10
    nav_budget_expand_appearance_threshold: int = 3
    nav_consecutive_zero_window: int = 6
    nav_max_total_pages: int = 320
    nav_max_depth: int = 4
    nav_max_next_urls: int = 12
    nav_forced_explore_links: int = 10
    nav_pdf_enabled: bool = False
    nav_strict_conference_focus: bool = True
    nav_summary_text_chars: int = 2500
    nav_llm_retry_count: int = 2
    nav_no_progress_streak_limit: int = 6
    nav_deterministic_score_threshold: int = 12
    markdown_first_enabled: bool = True
    markdown_emergency_fallback_enabled: bool = False
    markdown_reasoner_max_chars: int = 12000
    markdown_segment_chars: int = 6000
    markdown_segment_overlap: int = 500
    markdown_segment_max: int = 8
    link_memory_enabled: bool = True
    link_memory_decay_days: int = 30
    link_memory_min_visits: int = 2
    pathfinder_enabled: bool = True
    pathfinder_llm_retry_count: int = 2
    pathfinder_max_next_urls: int = 12
    pathfinder_gatekeeper_min_conf: float = 0.60
    modal_breaker_enabled: bool = True
    modal_breaker_min_candidates: int = 3
    modal_breaker_max_attempts_per_page: int = 1
    modal_breaker_wait_for_selectors: str = ".speaker-name,.faculty-name,[data-speaker]"
    modal_breaker_magic_mode: bool = True
    novelty_window_size: int = 10
    novelty_zero_window_limit: int = 3
    max_total_pages_per_run: int = 500
    max_run_duration_minutes: int = 240
    interaction_explorer_enabled: bool = True
    interaction_min_internal_links: int = 60
    interaction_max_actions_per_page: int = 12
    interaction_no_novelty_limit: int = 3
    attribution_min_confidence: float = 0.55
    llm_page_context_max_chars: int = 60000
    llm_page_segment_chars: int = 2400
    llm_page_segment_overlap: int = 250
    llm_page_max_segments: int = 60
    llm_dom_candidate_cap: int = 20
    llm_pdf_candidate_cap: int = 60
    llm_candidate_cap: int = 800
    llm_normalize_batch_size: int = 4
    llm_request_timeout_seconds: int = 150
    llm_batch_timeout_buffer_seconds: int = 30
    watchdog_stall_seconds: int = 240
    watchdog_max_stalls_per_run: int = 2
    physician_enrichment_enabled: bool = True
    physician_enrichment_max_results: int = 8
    physician_enrichment_timeout_seconds: int = 20
    physician_enrichment_min_confidence: float = 0.7
    physician_enrichment_max_evidence_urls: int = 10
    physician_enrichment_source_timeout_seconds: float = 10.0
    physician_enrichment_llm_passes: int = 2
    physician_enrichment_enable_npi: bool = True
    physician_enrichment_enable_pubmed: bool = True
    physician_enrichment_enable_openalex: bool = True
    physician_enrichment_enable_duckduckgo: bool = True
    physician_enrichment_enable_serpapi_images: bool = True
    physician_enrichment_trusted_profile_domains: str = "nih.gov,pubmed.ncbi.nlm.nih.gov,orcid.org,edu,org,gov"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

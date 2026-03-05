from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl

from app.models.enums import ConferenceYearStatus, RunStatus


class ScrapeRunCreate(BaseModel):
    home_url: HttpUrl
    conference_name: str = Field(min_length=2, max_length=255)


class ScrapeRunCreateResponse(BaseModel):
    run_id: str
    status: RunStatus
    home_url: str
    conference_name: str | None = None


class ScrapeRunListItemOut(BaseModel):
    run_id: str
    status: RunStatus
    home_url: str
    conference_name: str | None = None
    created_at: datetime
    finished_at: datetime | None = None


class ScrapeRunListResponse(BaseModel):
    runs: list[ScrapeRunListItemOut]


class ScrapeRunCancelResponse(BaseModel):
    run_id: str
    status: RunStatus
    cancelled: bool
    message: str


class DiscoveredConferenceYearStatus(BaseModel):
    conference_year_id: int
    conference_name: str
    year: int
    status: ConferenceYearStatus
    notes: str | None = None


class RunMetricsOut(BaseModel):
    pages_visited: int = 0
    pages_enqueued: int = 0
    unique_url_states: int = 0
    frontier_size: int = 0
    branch_count: int = 0
    adaptive_budget_current: int = 0
    adaptive_budget_max: int = 0
    interaction_actions_total: int = 0
    high_yield_branches: int = 0
    pages_skipped_budget: int = 0
    template_clusters_discovered: int = 0
    speaker_candidates_found: int = 0
    speaker_candidates_new: int = 0
    normalized_speakers: int = 0
    physicians_linked: int = 0
    appearances_linked: int = 0
    unresolved_attributions: int = 0
    attribution_resolved_count: int = 0
    attribution_reconcile_resolved_count: int = 0
    attribution_final_unresolved_count: int = 0
    llm_calls: int = 0
    llm_failures: int = 0
    llm_calls_saved: int = 0
    llm_batches_started: int = 0
    llm_batches_completed: int = 0
    llm_batches_timed_out: int = 0
    stalls_recovered: int = 0
    stalls_terminal: int = 0
    repeated_state_skips: int = 0
    gatekeeper_links_found: int = 0
    modal_breaker_attempts: int = 0
    modal_breaker_successes: int = 0
    dynamic_pages_detected: int = 0
    pathfinder_llm_calls: int = 0
    pathfinder_llm_failures: int = 0
    novelty_windows_without_progress: int = 0
    markdown_pages_processed: int = 0
    markdown_chars_processed: int = 0
    markdown_segments_used: int = 0
    memory_templates_hit: int = 0
    memory_templates_promoted: int = 0
    legacy_fallback_pages: int = 0
    pages_with_zero_speakers_nonzero_links: int = 0
    branches_closed_no_links: int = 0
    nav_reask_attempts: int = 0
    nav_reask_successes: int = 0


class RunProgressStateOut(BaseModel):
    queue_estimate: int = 0
    no_progress_streak: int = 0
    last_stage: str = "unknown"
    last_update_at: datetime | None = None


class ScrapeRunStatusResponse(BaseModel):
    run_id: str
    home_url: str
    conference_name: str | None = None
    status: RunStatus
    created_at: datetime
    finished_at: datetime | None
    years: list[DiscoveredConferenceYearStatus] = Field(default_factory=list)
    metrics: RunMetricsOut = Field(default_factory=RunMetricsOut)
    progress_state: RunProgressStateOut = Field(default_factory=RunProgressStateOut)


class RunDashboardConferenceYearOut(BaseModel):
    conference_year_id: int
    conference_name: str
    year: int
    status: str
    linked_appearances: int = 0
    duplicate_links: int = 0
    notes: str | None = None


class RunDashboardSummaryOut(BaseModel):
    conferences_scraped: int = 0
    conference_year_entries: int = 0
    unique_years_scraped: int = 0
    speakers_discovered: int = 0
    normalized_speakers: int = 0
    profiles_enrichment_started: int = 0
    profiles_enriched: int = 0
    profiles_enrichment_skipped: int = 0
    physicians_linked: int = 0
    appearances_linked: int = 0
    attribution_unresolved: int = 0
    llm_calls: int = 0
    llm_failures: int = 0
    llm_calls_saved: int = 0
    nav_reask_attempts: int = 0
    nav_reask_successes: int = 0


class RunDashboardResponse(BaseModel):
    run_id: str
    conference_name: str | None = None
    summary: RunDashboardSummaryOut
    conference_years: list[RunDashboardConferenceYearOut] = Field(default_factory=list)


class DashboardOverviewYearOut(BaseModel):
    conference_year_id: int
    conference_name: str
    year: int
    unique_speakers_db: int = 0
    appearance_count_db: int = 0


class DashboardOverviewConferenceOut(BaseModel):
    conference_name: str
    years_scraped: int = 0
    unique_speakers_db: int = 0
    appearance_count_db: int = 0
    speakers_found_extracted: int = 0
    pages_visited: int = 0
    links_discovered_unique: int = 0
    good_pages_with_speakers: int = 0
    years: list[DashboardOverviewYearOut] = Field(default_factory=list)


class DashboardOverviewTotalsOut(BaseModel):
    complete_runs_considered: int = 0
    conferences_scraped: int = 0
    conference_years_scraped: int = 0
    speakers_found_extracted: int = 0
    unique_speakers_db: int = 0
    appearance_count_db: int = 0
    pages_visited: int = 0
    links_discovered_unique: int = 0
    good_pages_with_speakers: int = 0


class DashboardOverviewResponse(BaseModel):
    generated_at: datetime
    totals: DashboardOverviewTotalsOut
    conferences: list[DashboardOverviewConferenceOut] = Field(default_factory=list)


class RunEventOut(BaseModel):
    id: int
    run_id: str
    conference_year_id: int | None
    stage: str
    level: str
    message: str
    data_json: str | None
    created_at: datetime


class RunEventsResponse(BaseModel):
    next_cursor: int | None
    events: list[RunEventOut]

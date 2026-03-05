export type ConferenceYear = {
  id: number;
  year: number;
  status: string;
  notes: string | null;
  created_at: string;
};

export type Conference = {
  id: number;
  name: string;
  canonical_name: string | null;
  organizer_name?: string | null;
  event_series_name?: string | null;
  name_confidence?: number | null;
  created_at: string;
  years: ConferenceYear[];
};

export type ConferenceDetail = Conference & {
  total_physicians: number;
  total_appearances: number;
};

export type ConferencePhysicianCard = {
  physician_id: number;
  full_name: string;
  primary_designation: string | null;
  appearance_count: number;
  session_count: number;
};

export type ConferenceYearPhysicianGroup = {
  year: number;
  status: string;
  notes: string | null;
  physicians: ConferencePhysicianCard[];
};

export type Appearance = {
  id: number;
  conference_year_id: number;
  conference_id: number;
  conference_name: string;
  year: number;
  role: string | null;
  session_title: string | null;
  talk_brief_extracted: string | null;
  talk_brief_generated: string | null;
  confidence: number | null;
  source_url: string | null;
};

export type Physician = {
  id: number;
  full_name: string;
  primary_designation: string | null;
  primary_affiliation: string | null;
  primary_location: string | null;
  primary_specialty?: string | null;
  primary_education?: string | null;
  primary_profile_url?: string | null;
  specialty?: string | null;
  profile_url?: string | null;
  bio_short?: string | null;
  created_at: string;
  appearances: Appearance[];
  aliases?: string[];
  highlight_conference_id?: number | null;
  highlight_year?: number | null;
};

export type PhysicianCardLite = {
  id: number;
  full_name: string;
  primary_designation: string | null;
  primary_specialty?: string | null;
  primary_profile_url?: string | null;
  specialty?: string | null;
  profile_url?: string | null;
  bio_short?: string | null;
  conference_count: number;
  appearance_count: number;
};

export type RunCreateResponse = {
  run_id: string;
  status: string;
  home_url: string;
  conference_name: string | null;
};

export type RunCancelResponse = {
  run_id: string;
  status: string;
  cancelled: boolean;
  message: string;
};

export type RunMetrics = {
  pages_visited: number;
  pages_enqueued: number;
  unique_url_states: number;
  frontier_size: number;
  branch_count: number;
  adaptive_budget_current: number;
  adaptive_budget_max: number;
  interaction_actions_total: number;
  high_yield_branches: number;
  pages_skipped_budget: number;
  template_clusters_discovered: number;
  speaker_candidates_found: number;
  speaker_candidates_new: number;
  normalized_speakers: number;
  physicians_linked: number;
  appearances_linked: number;
  unresolved_attributions: number;
  attribution_resolved_count: number;
  attribution_reconcile_resolved_count: number;
  attribution_final_unresolved_count: number;
  llm_calls: number;
  llm_failures: number;
  llm_calls_saved: number;
  llm_batches_started: number;
  llm_batches_completed: number;
  llm_batches_timed_out: number;
  stalls_recovered: number;
  stalls_terminal: number;
  repeated_state_skips: number;
  gatekeeper_links_found: number;
  modal_breaker_attempts: number;
  modal_breaker_successes: number;
  dynamic_pages_detected: number;
  pathfinder_llm_calls: number;
  pathfinder_llm_failures: number;
  novelty_windows_without_progress: number;
  markdown_pages_processed: number;
  markdown_chars_processed: number;
  markdown_segments_used: number;
  memory_templates_hit: number;
  memory_templates_promoted: number;
  legacy_fallback_pages: number;
  pages_with_zero_speakers_nonzero_links: number;
  branches_closed_no_links: number;
  nav_reask_attempts: number;
  nav_reask_successes: number;
};

export type RunStatusResponse = {
  run_id: string;
  home_url: string;
  conference_name: string | null;
  status: string;
  created_at: string;
  finished_at: string | null;
  metrics: RunMetrics;
  progress_state: {
    queue_estimate: number;
    no_progress_streak: number;
    last_stage: string;
    last_update_at: string | null;
  };
  years: {
    conference_year_id: number;
    conference_name: string;
    year: number;
    status: string;
    notes: string | null;
  }[];
};

export type RunListItem = {
  run_id: string;
  status: string;
  home_url: string;
  conference_name: string | null;
  created_at: string;
  finished_at: string | null;
};

export type RunListResponse = {
  runs: RunListItem[];
};

export type RunDashboardConferenceYear = {
  conference_year_id: number;
  conference_name: string;
  year: number;
  status: string;
  linked_appearances: number;
  duplicate_links: number;
  notes: string | null;
};

export type RunDashboardSummary = {
  conferences_scraped: number;
  conference_year_entries: number;
  unique_years_scraped: number;
  speakers_discovered: number;
  normalized_speakers: number;
  profiles_enrichment_started: number;
  profiles_enriched: number;
  profiles_enrichment_skipped: number;
  physicians_linked: number;
  appearances_linked: number;
  attribution_unresolved: number;
  llm_calls: number;
  llm_failures: number;
  llm_calls_saved: number;
  nav_reask_attempts: number;
  nav_reask_successes: number;
};

export type RunDashboardResponse = {
  run_id: string;
  conference_name: string | null;
  summary: RunDashboardSummary;
  conference_years: RunDashboardConferenceYear[];
};

export type DashboardOverviewYear = {
  conference_year_id: number;
  conference_name: string;
  year: number;
  unique_speakers_db: number;
  appearance_count_db: number;
};

export type DashboardOverviewConference = {
  conference_name: string;
  years_scraped: number;
  unique_speakers_db: number;
  appearance_count_db: number;
  speakers_found_extracted: number;
  pages_visited: number;
  links_discovered_unique: number;
  good_pages_with_speakers: number;
  years: DashboardOverviewYear[];
};

export type DashboardOverviewTotals = {
  complete_runs_considered: number;
  conferences_scraped: number;
  conference_years_scraped: number;
  speakers_found_extracted: number;
  unique_speakers_db: number;
  appearance_count_db: number;
  pages_visited: number;
  links_discovered_unique: number;
  good_pages_with_speakers: number;
};

export type DashboardOverviewResponse = {
  generated_at: string;
  totals: DashboardOverviewTotals;
  conferences: DashboardOverviewConference[];
};

export type RunEvent = {
  id: number;
  run_id: string;
  conference_year_id: number | null;
  stage: string;
  level: string;
  message: string;
  data_json: string | null;
  created_at: string;
};

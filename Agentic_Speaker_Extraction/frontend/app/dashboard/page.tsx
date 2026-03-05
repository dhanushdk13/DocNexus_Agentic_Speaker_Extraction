"use client";

import { useEffect, useMemo, useState } from "react";

import { apiGet } from "../../components/api";
import { DashboardOverviewResponse } from "../../components/types";

type DashboardState = {
  overview: DashboardOverviewResponse | null;
  loading: boolean;
  error: string | null;
};

function formatCount(value: number): string {
  return Number.isFinite(value) ? value.toLocaleString() : "0";
}

export default function DashboardPage() {
  const [state, setState] = useState<DashboardState>({ overview: null, loading: true, error: null });
  const [selectedConference, setSelectedConference] = useState<string>("");
  const conferences = state.overview?.conferences ?? [];

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        const overview = await apiGet<DashboardOverviewResponse>("/dashboard/overview");
        if (!active) {
          return;
        }
        setState({ overview, loading: false, error: null });
        setSelectedConference((current) =>
          overview.conferences.some((item) => item.conference_name === current)
            ? current
            : (overview.conferences[0]?.conference_name ?? ""),
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to load dashboard";
        if (!active) {
          return;
        }
        setState({ overview: null, loading: false, error: message });
      }
    }

    void load();
    const timer = setInterval(() => {
      void load();
    }, 10000);

    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  const sortedConferences = useMemo(
    () =>
      [...conferences].sort(
        (a, b) =>
          b.speakers_found_extracted - a.speakers_found_extracted ||
          b.appearance_count_db - a.appearance_count_db ||
          a.conference_name.localeCompare(b.conference_name),
      ),
    [conferences],
  );
  const activeConference =
    sortedConferences.find((conference) => conference.conference_name === selectedConference) || sortedConferences[0] || null;

  if (state.loading) {
    return <section className="panel">Loading dashboard...</section>;
  }

  if (state.error) {
    return <section className="panel error">{state.error}</section>;
  }

  if (!state.overview) {
    return <section className="panel">No dashboard data is available yet.</section>;
  }

  const { totals } = state.overview;

  return (
    <div className="page-stack dashboard-single">
      <section className="panel intro-panel">
        <h2>Conference and Speaker Outcomes</h2>
        <p className="small">Dynamic run outcomes from conferences where speakers were extracted and linked crawl evidence is available.</p>
      </section>

      <section className="kpi-grid">
        <article className="panel kpi-card">
          <p className="kpi-label">Conferences Scraped</p>
          <p className="kpi-value">{formatCount(totals.conferences_scraped)}</p>
          <p className="small">Complete runs: {formatCount(totals.complete_runs_considered)}</p>
        </article>
        <article className="panel kpi-card">
          <p className="kpi-label">Conference Years</p>
          <p className="kpi-value">{formatCount(totals.conference_years_scraped)}</p>
          <p className="small">Year-level coverage from complete runs</p>
        </article>
        <article className="panel kpi-card">
          <p className="kpi-label">Speakers Found</p>
          <p className="kpi-value">{formatCount(totals.speakers_found_extracted)}</p>
          <p className="small">From markdown extraction events</p>
        </article>
        <article className="panel kpi-card">
          <p className="kpi-label">Unique Speakers in DB</p>
          <p className="kpi-value">{formatCount(totals.unique_speakers_db)}</p>
          <p className="small">Distinct linked physician records</p>
        </article>
        <article className="panel kpi-card">
          <p className="kpi-label">Appearances in DB</p>
          <p className="kpi-value">{formatCount(totals.appearance_count_db)}</p>
          <p className="small">Conference-year appearance rows</p>
        </article>
        <article className="panel kpi-card">
          <p className="kpi-label">Pages Visited</p>
          <p className="kpi-value">{formatCount(totals.pages_visited)}</p>
          <p className="small">Across all complete runs</p>
        </article>
        <article className="panel kpi-card">
          <p className="kpi-label">Unique Links Discovered</p>
          <p className="kpi-value">{formatCount(totals.links_discovered_unique)}</p>
          <p className="small">Unique conference-internal candidates</p>
        </article>
        <article className="panel kpi-card">
          <p className="kpi-label">Good Pages</p>
          <p className="kpi-value">{formatCount(totals.good_pages_with_speakers)}</p>
          <p className="small">Pages with extracted speakers, seed included</p>
        </article>
      </section>

      <section className="panel conference-overview-card">
        {sortedConferences.length === 0 ? (
          <p className="small">No conference-level data available yet. Finish a run with extracted speakers to populate this view.</p>
        ) : (
          <>
            <div className="panel-head">
              <h3>Conference Details</h3>
              <span className="chip">{sortedConferences.length} conference(s)</span>
            </div>
            <div style={{ maxWidth: "320px", marginBottom: "0.9rem" }}>
              <label className="label" htmlFor="dashboard_conference_select">
                Select conference
              </label>
              <select
                id="dashboard_conference_select"
                value={activeConference?.conference_name || ""}
                onChange={(event) => setSelectedConference(event.target.value)}
              >
                {sortedConferences.map((conference) => (
                  <option key={conference.conference_name} value={conference.conference_name}>
                    {conference.conference_name}
                  </option>
                ))}
              </select>
            </div>

            {activeConference ? (
              <>
                <div className="detail-grid conference-mini-grid">
                  <div className="info-block">
                    <span className="info-label">Speakers Found</span>
                    <p className="info-value">{formatCount(activeConference.speakers_found_extracted)}</p>
                  </div>
                  <div className="info-block">
                    <span className="info-label">Unique Speakers in DB</span>
                    <p className="info-value">{formatCount(activeConference.unique_speakers_db)}</p>
                  </div>
                  <div className="info-block">
                    <span className="info-label">Appearances in DB</span>
                    <p className="info-value">{formatCount(activeConference.appearance_count_db)}</p>
                  </div>
                  <div className="info-block">
                    <span className="info-label">Pages Visited</span>
                    <p className="info-value">{formatCount(activeConference.pages_visited)}</p>
                  </div>
                  <div className="info-block">
                    <span className="info-label">Unique Links Discovered</span>
                    <p className="info-value">{formatCount(activeConference.links_discovered_unique)}</p>
                  </div>
                  <div className="info-block">
                    <span className="info-label">Good Pages</span>
                    <p className="info-value">{formatCount(activeConference.good_pages_with_speakers)}</p>
                  </div>
                </div>

                <div className="conference-year-grid">
                  {activeConference.years.length === 0 ? (
                    <div className="conference-year-card">
                      <p className="small">No year entries were linked for this conference.</p>
                    </div>
                  ) : (
                    activeConference.years.map((year) => (
                      <div key={year.conference_year_id} className="conference-year-card">
                        <div className="panel-head" style={{ marginBottom: "0.2rem" }}>
                          <h4>{year.year}</h4>
                          <span className="chip">Year ID: {year.conference_year_id}</span>
                        </div>
                        <div className="conference-year-stats">
                          <div className="pipeline-row">
                            <span>Unique Speakers</span>
                            <strong>{formatCount(year.unique_speakers_db)}</strong>
                          </div>
                          <div className="pipeline-row">
                            <span>Appearances</span>
                            <strong>{formatCount(year.appearance_count_db)}</strong>
                          </div>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </>
            ) : null}
          </>
        )}
      </section>
    </div>
  );
}

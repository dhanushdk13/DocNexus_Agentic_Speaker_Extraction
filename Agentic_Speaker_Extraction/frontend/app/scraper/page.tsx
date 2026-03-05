"use client";

import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { apiGet, apiPost } from "../../components/api";
import {
  RunCancelResponse,
  RunCreateResponse,
  RunEvent,
  RunListItem,
  RunListResponse,
  RunStatusResponse,
} from "../../components/types";

const STORAGE_ACTIVE_RUN_ID = "scraper.active_run_id";
const storageCursorKey = (runId: string) => `scraper.cursor.${runId}`;
const storageEventsKey = (runId: string) => `scraper.events.${runId}`;
const storageStatusKey = (runId: string) => `scraper.status.${runId}`;

const TERMINAL_RUN_STATUSES = new Set(["complete", "partial", "error", "blocked"]);

const LOOP_GUARD_STAGES = new Set(["state_repeat_skip", "no_progress_stop"]);

type RunHistoryEntry = {
  run_id: RunListItem["run_id"];
  home_url: RunListItem["home_url"];
  conference_name?: RunListItem["conference_name"];
  status: RunListItem["status"];
  created_at: RunListItem["created_at"];
  finished_at: RunListItem["finished_at"];
};

function safeParse<T>(value: string | null, fallback: T): T {
  if (!value) {
    return fallback;
  }
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function parseEventData(event: RunEvent): Record<string, unknown> {
  if (!event.data_json) {
    return {};
  }
  try {
    return JSON.parse(event.data_json) as Record<string, unknown>;
  } catch {
    return {};
  }
}

function mergeUniqueEvents(existing: RunEvent[], incoming: RunEvent[]): RunEvent[] {
  if (incoming.length === 0) {
    return existing;
  }
  const merged = new Map<number, RunEvent>();
  for (const event of existing) {
    merged.set(event.id, event);
  }
  for (const event of incoming) {
    merged.set(event.id, event);
  }
  return Array.from(merged.values()).sort((a, b) => a.id - b.id);
}

export default function ScraperPage() {
  const [homeUrl, setHomeUrl] = useState("");
  const [conferenceName, setConferenceName] = useState("");
  const [runId, setRunId] = useState<string | null>(null);
  const [runStatus, setRunStatus] = useState<RunStatusResponse | null>(null);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [cursor, setCursor] = useState<number | null>(null);
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [detailRunId, setDetailRunId] = useState<string | null>(null);
  const [detailRunStatus, setDetailRunStatus] = useState<RunStatusResponse | null>(null);
  const [detailEvents, setDetailEvents] = useState<RunEvent[]>([]);
  const [detailLoading, setDetailLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const cursorRef = useRef<number | null>(null);
  const pollInFlightRef = useRef(false);

  const clearRunState = useCallback(
    (targetRunId: string) => {
      localStorage.removeItem(STORAGE_ACTIVE_RUN_ID);
      localStorage.removeItem(storageCursorKey(targetRunId));
      localStorage.removeItem(storageEventsKey(targetRunId));
      localStorage.removeItem(storageStatusKey(targetRunId));

      const clearingActive = runId === targetRunId;
      if (clearingActive) {
        setRunId(null);
        setRunStatus(null);
        setEvents([]);
        setCursor(null);
        cursorRef.current = null;
      }
    },
    [runId],
  );

  const refreshRunHistory = useCallback(async () => {
    try {
      const response = await apiGet<RunListResponse>("/scrape-runs?limit=20");
      setRunHistory(response.runs);
    } catch {
      // Keep the existing list if history refresh fails.
    }
  }, []);

  useEffect(() => {
    const activeRunId = localStorage.getItem(STORAGE_ACTIVE_RUN_ID);
    if (!activeRunId) {
      void refreshRunHistory();
      return;
    }
    setRunId(activeRunId);

    const persistedCursor = safeParse<number | null>(localStorage.getItem(storageCursorKey(activeRunId)), null);
    const persistedEvents = safeParse<RunEvent[]>(localStorage.getItem(storageEventsKey(activeRunId)), []);
    const persistedStatus = safeParse<RunStatusResponse | null>(localStorage.getItem(storageStatusKey(activeRunId)), null);

    cursorRef.current = persistedCursor;
    setCursor(persistedCursor);
    setEvents(mergeUniqueEvents([], persistedEvents));
    setRunStatus(persistedStatus);
    void refreshRunHistory();
  }, [refreshRunHistory]);

  useEffect(() => {
    if (!runId) {
      localStorage.removeItem(STORAGE_ACTIVE_RUN_ID);
      return;
    }
    localStorage.setItem(STORAGE_ACTIVE_RUN_ID, runId);
  }, [runId]);

  useEffect(() => {
    if (!runId) {
      return;
    }
    localStorage.setItem(storageCursorKey(runId), JSON.stringify(cursor));
  }, [cursor, runId]);

  useEffect(() => {
    if (!runId) {
      return;
    }
    localStorage.setItem(storageEventsKey(runId), JSON.stringify(events.slice(-500)));
  }, [events, runId]);

  useEffect(() => {
    if (!runId || !runStatus) {
      return;
    }
    localStorage.setItem(storageStatusKey(runId), JSON.stringify(runStatus));
  }, [runId, runStatus]);

  useEffect(() => {
    if (!runId) {
      return;
    }

    let timer: NodeJS.Timeout | null = null;

    const tick = async () => {
      if (pollInFlightRef.current) {
        return;
      }
      pollInFlightRef.current = true;
      try {
        const status = await apiGet<RunStatusResponse>(`/scrape-runs/${runId}`);
        setRunStatus(status);
        if (TERMINAL_RUN_STATUSES.has(status.status) && timer) {
          clearInterval(timer);
          timer = null;
          localStorage.removeItem(STORAGE_ACTIVE_RUN_ID);
        }

        const params = cursorRef.current ? `?cursor=${cursorRef.current}` : "";
        const eventResp = await apiGet<{ next_cursor: number | null; events: RunEvent[] }>(
          `/scrape-runs/${runId}/events${params}`,
        );

        if (eventResp.events.length > 0) {
          setEvents((prev) => mergeUniqueEvents(prev, eventResp.events));
        }
        cursorRef.current = eventResp.next_cursor;
        setCursor(eventResp.next_cursor);
        await refreshRunHistory();
      } catch (err) {
        const message = err instanceof Error ? err.message : "Unable to refresh run status";
        if (message.includes("failed: 404")) {
          if (timer) {
            clearInterval(timer);
            timer = null;
          }
          clearRunState(runId);
          setError(null);
          return;
        }
        setError(message);
      } finally {
        pollInFlightRef.current = false;
      }
    };

    void tick();
    timer = setInterval(() => {
      void tick();
    }, 2000);

    return () => {
      if (timer) {
        clearInterval(timer);
      }
    };
  }, [runId, clearRunState, refreshRunHistory]);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    setEvents([]);
    setRunStatus(null);
    setCursor(null);
    cursorRef.current = null;

    try {
      const response = await apiPost<RunCreateResponse>("/scrape-runs", {
        home_url: homeUrl,
        conference_name: conferenceName,
      });
      setRunId(response.run_id);
      localStorage.setItem(STORAGE_ACTIVE_RUN_ID, response.run_id);
      await refreshRunHistory();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Run creation failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function onCancelRunById(targetRunId: string) {
    if (!targetRunId || cancelling) {
      return;
    }

    setCancelling(true);
    setError(null);
    try {
      await apiPost<RunCancelResponse>(`/scrape-runs/${targetRunId}/cancel`, {});
      const refreshed = await apiGet<RunStatusResponse>(`/scrape-runs/${targetRunId}`);
      setRunStatus((current) => (current?.run_id === targetRunId ? refreshed : current));
      await refreshRunHistory();

      if (TERMINAL_RUN_STATUSES.has(refreshed.status) && runId === targetRunId) {
        localStorage.removeItem(STORAGE_ACTIVE_RUN_ID);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Run cancellation failed");
    } finally {
      setCancelling(false);
    }
  }

  async function onCancelRun() {
    if (!activeRunStatus) {
      return;
    }
    await onCancelRunById(activeRunStatus.run_id);
  }

  async function onViewRunDetails(targetRunId: string) {
    if (!targetRunId || detailLoading) {
      return;
    }
    if (detailRunId === targetRunId) {
      setDetailRunId(null);
      setDetailRunStatus(null);
      setDetailEvents([]);
      return;
    }
    setDetailRunId(targetRunId);
    setDetailLoading(true);
    setError(null);
    try {
      const [status, eventResp] = await Promise.all([
        apiGet<RunStatusResponse>(`/scrape-runs/${targetRunId}`),
        apiGet<{ next_cursor: number | null; events: RunEvent[] }>(`/scrape-runs/${targetRunId}/events?limit=500`),
      ]);
      setDetailRunStatus(status);
      if (TERMINAL_RUN_STATUSES.has(status.status)) {
        setDetailEvents([]);
      } else {
        setDetailEvents(mergeUniqueEvents([], eventResp.events));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load run details");
    } finally {
      setDetailLoading(false);
    }
  }

  const groupedEvents = useMemo(() => {
    const groups: Record<string, RunEvent[]> = {};
    for (const event of events) {
      const payload = parseEventData(event);
      const url =
        typeof payload.url === "string"
          ? payload.url
          : typeof payload.from_url === "string"
            ? payload.from_url
            : "general";
      if (!groups[url]) {
        groups[url] = [];
      }
      groups[url].push(event);
    }
    return groups;
  }, [events]);

  const activeRunStatus = runStatus && !TERMINAL_RUN_STATUSES.has(runStatus.status) ? runStatus : null;
  const otherRuns = useMemo(
    () => runHistory.filter((item) => item.run_id !== activeRunStatus?.run_id),
    [runHistory, activeRunStatus],
  );

  return (
    <div className="page-stack">
      <section className="panel intro-panel">
        <h2>Scrape Runner</h2>
        <p className="small">Launch runs, monitor progress, and inspect real-time event streams.</p>
        <form onSubmit={onSubmit} style={{ marginTop: "0.75rem" }}>
          <div className="row">
            <div>
              <label className="label" htmlFor="conference_name">
                Conference Name
              </label>
              <input
                id="conference_name"
                placeholder="Continuum"
                value={conferenceName}
                onChange={(e) => setConferenceName(e.target.value)}
                required
              />
            </div>
            <div>
              <label className="label" htmlFor="home_url">
                Home URL
              </label>
              <input
                id="home_url"
                placeholder="https://www.iapac.org/conferences/continuum-2025/"
                value={homeUrl}
                onChange={(e) => setHomeUrl(e.target.value)}
                required
              />
            </div>
            <button type="submit" disabled={submitting || !homeUrl.trim() || !conferenceName.trim()}>
              {submitting ? "Starting..." : "Start Run"}
            </button>
          </div>
        </form>
        {error ? <p className="error" style={{ marginTop: "0.75rem" }}>{error}</p> : null}
      </section>

      {runStatus ? (
        <section className="panel">
          <div className="panel-head">
            <h3>Current Run</h3>
            <span className={`badge status-${runStatus.status}`}>{runStatus.status}</span>
          </div>
          <p className="small">Run ID: {runStatus.run_id}</p>
          <p className="small">Conference: {runStatus.conference_name || "n/a"}</p>
          <p className="small">Seed URL: {runStatus.home_url}</p>

          <div className="run-history-actions" style={{ marginTop: "0.7rem", justifyContent: "flex-start" }}>
            <button type="button" onClick={() => void onViewRunDetails(runStatus.run_id)} disabled={detailLoading} className="btn-ghost">
              {detailLoading && detailRunId === runStatus.run_id
                ? "Loading..."
                : detailRunId === runStatus.run_id
                  ? "Hide details"
                  : "See details"}
            </button>
            {activeRunStatus ? (
              <button type="button" onClick={onCancelRun} disabled={cancelling} className="btn-danger">
                {cancelling ? "Cancelling..." : "Cancel Run"}
              </button>
            ) : null}
          </div>

          <ul className="clean" style={{ marginTop: "0.8rem" }}>
            {runStatus.years.map((year) => (
              <li key={year.conference_year_id}>
                <strong>
                  {year.conference_name} {year.year}
                </strong>{" "}
                <span className={`badge status-${year.status}`}>{year.status}</span>
                {year.notes ? <div className="small">{year.notes}</div> : null}
              </li>
            ))}
          </ul>

          {detailRunStatus && detailRunId === runStatus.run_id ? (
            <div className="run-history-details" style={{ marginTop: "0.8rem" }}>
              {TERMINAL_RUN_STATUSES.has(detailRunStatus.status) ? (
                <ul className="clean">
                  <li className="small">Pages visited: {detailRunStatus.metrics.pages_visited.toLocaleString()}</li>
                  <li className="small">Pages enqueued: {detailRunStatus.metrics.pages_enqueued.toLocaleString()}</li>
                  <li className="small">Candidates found: {detailRunStatus.metrics.speaker_candidates_found.toLocaleString()}</li>
                  <li className="small">Normalized speakers: {detailRunStatus.metrics.normalized_speakers.toLocaleString()}</li>
                  <li className="small">Appearances linked: {detailRunStatus.metrics.appearances_linked.toLocaleString()}</li>
                  <li className="small">LLM calls/failures: {detailRunStatus.metrics.llm_calls}/{detailRunStatus.metrics.llm_failures}</li>
                </ul>
              ) : (
                <div className="run-events-feed">
                  {detailEvents.slice(-120).map((event) => (
                    <div className="event-item" key={event.id}>
                      <div className="event-item-head">
                        <span className="badge event-stage">{event.stage}</span>
                        <span className="event-time">{new Date(event.created_at).toLocaleString()}</span>
                      </div>
                      <div>{event.message}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ) : null}
        </section>
      ) : null}

      {activeRunStatus ? (
        <section className="panel">
          <div className="panel-head">
            <h3>Live Run Logs</h3>
            {runId ? <p className="small">{events.length.toLocaleString()} event(s)</p> : null}
          </div>
          <div className="run-events-feed">
            {Object.entries(groupedEvents).map(([groupKey, group]) => (
              <div key={groupKey} className="event-group">
                <div className="event-group-head">
                  <strong>{groupKey === "general" ? "General" : groupKey}</strong>
                  <span className="small">{group.length} item(s)</span>
                </div>
                {group.map((event) => (
                  <div className="event-item" key={event.id}>
                    <div className="event-item-head">
                      <div style={{ display: "flex", gap: "0.4rem", alignItems: "center", flexWrap: "wrap" }}>
                        <span className="badge event-stage">{event.stage}</span>
                        {LOOP_GUARD_STAGES.has(event.stage) ? <span className="badge status-error">loop-guard</span> : null}
                      </div>
                      <span className="event-time">{new Date(event.created_at).toLocaleString()}</span>
                    </div>
                    <div>{event.message}</div>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </section>
      ) : null}

      <section className="panel">
        <div className="panel-head">
          <h3>Run History</h3>
          <p className="small">Latest runs from backend</p>
        </div>
        {otherRuns.length === 0 ? <p className="small">No previous runs yet.</p> : null}

        <ul className="run-history-list">
          {otherRuns.map((item) => (
            <li key={item.run_id} className="run-history-row">
              <div className="run-history-meta">
                <div style={{ display: "flex", gap: "0.45rem", alignItems: "center", flexWrap: "wrap" }}>
                  <span className={`badge status-${item.status}`}>{item.status}</span>
                  <strong>{item.home_url}</strong>
                </div>
                <div className="small">Conference: {item.conference_name || "n/a"}</div>
                <div className="small">Run ID: {item.run_id}</div>
                <div className="small">
                  Updated: {new Date(item.finished_at || item.created_at).toLocaleString()}
                </div>
              </div>
              <div className="run-history-actions">
                <button type="button" onClick={() => void onViewRunDetails(item.run_id)} disabled={detailLoading} className="btn-ghost">
                  {detailLoading && detailRunId === item.run_id
                    ? "Loading..."
                    : detailRunId === item.run_id
                      ? "Hide details"
                      : "See details"}
                </button>
                {!TERMINAL_RUN_STATUSES.has(item.status) ? (
                  <button type="button" onClick={() => void onCancelRunById(item.run_id)} disabled={cancelling} className="btn-danger">
                    {cancelling ? "Cancelling..." : "Cancel Run"}
                  </button>
                ) : null}
              </div>
              {detailRunStatus && detailRunId === item.run_id ? (
                <div className="run-history-details">
                  {TERMINAL_RUN_STATUSES.has(detailRunStatus.status) ? (
                    <ul className="clean">
                      <li className="small">Pages visited: {detailRunStatus.metrics.pages_visited.toLocaleString()}</li>
                      <li className="small">Pages enqueued: {detailRunStatus.metrics.pages_enqueued.toLocaleString()}</li>
                      <li className="small">Candidates found: {detailRunStatus.metrics.speaker_candidates_found.toLocaleString()}</li>
                      <li className="small">Normalized speakers: {detailRunStatus.metrics.normalized_speakers.toLocaleString()}</li>
                      <li className="small">Physicians linked: {detailRunStatus.metrics.physicians_linked.toLocaleString()}</li>
                      <li className="small">Appearances linked: {detailRunStatus.metrics.appearances_linked.toLocaleString()}</li>
                      <li className="small">LLM calls/failures: {detailRunStatus.metrics.llm_calls}/{detailRunStatus.metrics.llm_failures}</li>
                    </ul>
                  ) : (
                    <div className="run-events-feed">
                      {detailEvents.slice(-80).map((event) => (
                        <div className="event-item" key={event.id}>
                          <div className="event-item-head">
                            <span className="badge event-stage">{event.stage}</span>
                            <span className="event-time">{new Date(event.created_at).toLocaleString()}</span>
                          </div>
                          <div>{event.message}</div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : null}
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}

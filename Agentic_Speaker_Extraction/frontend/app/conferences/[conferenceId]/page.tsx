"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import { apiGet } from "../../../components/api";
import { ConferenceDetail, ConferenceYearPhysicianGroup } from "../../../components/types";

export default function ConferenceDetailPage() {
  const params = useParams<{ conferenceId: string }>();
  const conferenceId = Number(params.conferenceId);
  const [detail, setDetail] = useState<ConferenceDetail | null>(null);
  const [groups, setGroups] = useState<ConferenceYearPhysicianGroup[]>([]);
  const [selectedYear, setSelectedYear] = useState<string>("all");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      if (!conferenceId || Number.isNaN(conferenceId)) {
        setError("Invalid conference id");
        setLoading(false);
        return;
      }
      try {
        const detailResp = await apiGet<ConferenceDetail>(`/conferences/${conferenceId}`);
        setDetail(detailResp);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load conference detail");
      } finally {
        setLoading(false);
      }
    }
    void load();
  }, [conferenceId]);

  useEffect(() => {
    async function loadGroups() {
      if (!conferenceId || Number.isNaN(conferenceId)) {
        return;
      }
      try {
        const yearQuery = selectedYear !== "all" ? `?year=${selectedYear}` : "";
        const groupsResp = await apiGet<ConferenceYearPhysicianGroup[]>(`/conferences/${conferenceId}/physicians${yearQuery}`);
        setGroups(groupsResp);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load conference physicians");
      }
    }
    void loadGroups();
  }, [conferenceId, selectedYear]);

  const nonEmptyGroups = useMemo(() => groups.filter((group) => group.physicians.length > 0), [groups]);

  if (loading) {
    return <section className="panel">Loading conference detail...</section>;
  }
  if (error) {
    return <section className="panel error">{error}</section>;
  }
  if (!detail) {
    return <section className="panel">Conference not found.</section>;
  }

  return (
    <div className="page-stack">
      <section className="panel intro-panel">
        <h2>{detail.name}</h2>
        <p className="small">
          {detail.total_physicians} physician card(s) • {detail.total_appearances} linked appearance(s)
        </p>
        <div style={{ marginTop: "0.8rem", maxWidth: "280px" }}>
          <label className="label" htmlFor="conference_year_filter">
            Filter by year
          </label>
          <select
            id="conference_year_filter"
            value={selectedYear}
            onChange={(event) => setSelectedYear(event.target.value)}
          >
            <option value="all">All years</option>
            {detail.years
              .slice()
              .sort((a, b) => b.year - a.year)
              .map((year) => (
                <option key={year.id} value={String(year.year)}>
                  {year.year}
                </option>
              ))}
          </select>
        </div>
      </section>

      {nonEmptyGroups.length === 0 ? (
        <section className="panel small">No linked physicians for this conference yet.</section>
      ) : null}

      {nonEmptyGroups.map((group) => (
        <section className="panel" key={group.year}>
          <div className="panel-head">
            <h3>
              {group.year} <span className={`badge status-${group.status}`}>{group.status}</span>
            </h3>
            <p className="small">{group.physicians.length} physician(s)</p>
          </div>
          {group.notes ? <p className="small" style={{ marginBottom: "0.7rem" }}>{group.notes}</p> : null}
          <div className="grid">
            {group.physicians.map((physician) => (
              <Link
                key={`${group.year}-${physician.physician_id}`}
                href={`/physicians/${physician.physician_id}?fromConferenceId=${conferenceId}&fromYear=${group.year}`}
                className="card-link"
              >
                <article className="panel interactive-card">
                  <h4>{physician.full_name}</h4>
                  {physician.primary_designation ? <p className="small">{physician.primary_designation}</p> : null}
                  <p className="small">
                    {physician.session_count} session(s) • {physician.appearance_count} appearance(s)
                  </p>
                </article>
              </Link>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

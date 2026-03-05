"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { apiGet } from "../../components/api";
import { PhysicianCardLite } from "../../components/types";

export default function PhysiciansPage() {
  const [query, setQuery] = useState("");
  const [data, setData] = useState<PhysicianCardLite[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const response = await apiGet<PhysicianCardLite[]>("/physicians/cards");
        setData(response);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load physicians");
      } finally {
        setLoading(false);
      }
    }

    void load();
  }, []);

  const filtered = useMemo(() => {
    if (!query.trim()) {
      return data;
    }
    const q = query.toLowerCase();
    return data.filter((entry) => entry.full_name.toLowerCase().includes(q));
  }, [data, query]);

  if (loading) {
    return <section className="panel">Loading physicians...</section>;
  }

  if (error) {
    return <section className="panel error">{error}</section>;
  }

  return (
    <div className="page-stack">
      <section className="panel intro-panel">
        <h2>Physicians</h2>
        <p className="small">Deduplicated physician cards with conference-linked appearances.</p>
        <label className="label" htmlFor="q" style={{ marginTop: "0.7rem" }}>
          Search by name
        </label>
        <input id="q" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search physician" />
      </section>

      <section className="physician-grid">
        {filtered.map((physician) => (
          <Link key={physician.id} href={`/physicians/${physician.id}`} className="card-link">
            <article className="physician-card interactive-card">
              <div className="physician-main">
                <h3>{physician.full_name}</h3>
                {physician.primary_designation ? (
                  <div className="info-block">
                    <span className="info-label">Designation</span>
                    <span className="info-value">{physician.primary_designation}</span>
                  </div>
                ) : null}
                {physician.primary_specialty ? (
                  <div className="info-block">
                    <span className="info-label">Specialty</span>
                    <span className="info-value">{physician.primary_specialty}</span>
                  </div>
                ) : null}
                {physician.bio_short ? (
                  <div className="info-block">
                    <span className="info-label">Summary</span>
                    <span className="info-value">{physician.bio_short}</span>
                  </div>
                ) : null}
                <p className="small">
                  {physician.conference_count} conference(s) • {physician.appearance_count} appearance(s)
                </p>
              </div>
            </article>
          </Link>
        ))}
      </section>
    </div>
  );
}

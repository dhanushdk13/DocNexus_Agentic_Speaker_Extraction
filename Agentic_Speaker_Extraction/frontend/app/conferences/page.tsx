"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { apiGet } from "../../components/api";
import { Conference } from "../../components/types";

export default function ConferencesPage() {
  const [data, setData] = useState<Conference[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const response = await apiGet<Conference[]>("/conferences");
        setData(response);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load conferences");
      } finally {
        setLoading(false);
      }
    }

    void load();
  }, []);

  if (loading) {
    return <section className="panel">Loading conferences...</section>;
  }

  if (error) {
    return <section className="panel error">{error}</section>;
  }

  return (
    <div className="page-stack">
      <section className="panel intro-panel">
        <h2>Conferences</h2>
        <p className="small">Open a conference to review year-by-year physician participation and linked session appearances.</p>
      </section>

      {data.length === 0 ? <section className="panel small">No conference data found yet.</section> : null}

      <section className="grid">
        {data.map((conference) => (
          <Link key={conference.id} href={`/conferences/${conference.id}`} className="card-link">
            <article className="panel interactive-card">
              <h3>{conference.name}</h3>
              <p className="small">{conference.years.length} conference year(s)</p>
              <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap" }}>
                {conference.years.slice(0, 8).map((year) => (
                  <span key={year.id} className={`badge status-${year.status}`}>
                    {year.year}
                  </span>
                ))}
              </div>
            </article>
          </Link>
        ))}
      </section>
    </div>
  );
}

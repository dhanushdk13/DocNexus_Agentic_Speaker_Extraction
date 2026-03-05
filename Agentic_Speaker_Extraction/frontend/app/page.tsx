import Link from "next/link";

export default function HomePage() {
  return (
    <div className="page-stack">
      <section className="panel intro-panel">
        <h2>DocNexus Control Center</h2>
        <p className="small">Track scraping runs, monitor extraction quality, and review physician intelligence with conference-level provenance.</p>
      </section>

      <section className="grid">
        <Link href="/dashboard" className="card-link">
          <article className="panel interactive-card">
            <h3>Dashboard</h3>
            <p className="small">Compare the latest successful runs with KPI cards, charts, and full metric deltas.</p>
          </article>
        </Link>

        <Link href="/scraper" className="card-link">
          <article className="panel interactive-card">
            <h3>Scraper</h3>
            <p className="small">Launch and monitor crawl runs in real time, including logs, run history, and cancellation controls.</p>
          </article>
        </Link>

        <Link href="/conferences" className="card-link">
          <article className="panel interactive-card">
            <h3>Conferences</h3>
            <p className="small">Browse conference-year rollups and see linked physicians by event chronology.</p>
          </article>
        </Link>

        <Link href="/physicians" className="card-link">
          <article className="panel interactive-card">
            <h3>Physicians</h3>
            <p className="small">Review deduplicated physician cards, profiles, aliases, and cross-conference appearances.</p>
          </article>
        </Link>
      </section>
    </div>
  );
}

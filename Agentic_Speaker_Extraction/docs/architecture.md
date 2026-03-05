# Architecture

## Components
- `backend`: FastAPI + Postgres + Alembic + in-process run worker.
- `frontend`: Next.js UI with three pages (`/scraper`, `/conferences`, `/physicians`).

## Pipeline
1. Search (DuckDuckGo HTML)
2. URL triage (DeepSeek)
3. Fetch (HTTP/Playwright/PDF)
4. Candidate extraction (DOM/PDF/network)
5. Structured extraction (DeepSeek)
6. Dedupe/link physicians globally

## Blocked-site policy
If anti-bot pages are detected (Cloudflare/captcha/verify-human patterns), source is marked `blocked`, retries are not aggressive, and pipeline continues to alternate URLs and source types.

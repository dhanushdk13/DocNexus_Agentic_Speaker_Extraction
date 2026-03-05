# DocNexus_Agentic_Speaker_Extraction

The agentic speaker extraction is capable of extracting the speakers along with session info's from medical conferences around the world with just a URL

## Stack

- Backend: FastAPI, SQLAlchemy, PostgreSQL, Alembic
- Frontend: Next.js 15, React 19, TypeScript
- Crawling/fetching: HTTP + Playwright/Selenium/Crawl4AI + optional PDF extraction
- LLM: DeepSeek (`deepseek-chat` / optional `deepseek-reasoner` depending on env toggles)

## Repository Structure

- `backend/`: API, scraper pipeline, models, migrations, run/event services
- `frontend/`: UI for scraper runs, conferences, physicians, dashboard
- `docs/architecture.md`: high-level architecture and pipeline
- `docs/runbook.md`: quick operational commands
- `backend/run_logs/`: run artifacts and logs
- `scripts/`: utility scripts

## Core Pipeline

1. Search / seed intake
2. URL triage and navigation reasoning
3. Page fetch (HTML / browser / optional PDF path)
4. Markdown-first extraction flow
5. LLM normalization of extracted speaker records
6. Attribution/linking to conference + year
7. Physician dedupe + appearance creation
8. Run/event metrics persisted for UI

## Prerequisites

- Python 3.11+
- Node.js 18+ (recommended: current LTS)
- PostgreSQL 14+

## Backend Setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -e .[test]
cp .env.example .env
```

Set required values in `backend/.env`:

- `POSTGRES_URL`
- `DEEPSEEK_API_KEY`
- `DEEPSEEK_MODEL` (default in example: `deepseek-chat`)
- `DEEPSEEK_BASE_URL`
- `CORS_ORIGINS`

Run migrations and start server:

```bash
cd backend
source .venv/bin/activate
alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

Backend API base: `http://localhost:8000/api/v1`

## Frontend Setup

```bash
cd frontend
npm install
NEXT_PUBLIC_API_BASE=http://localhost:8000/api/v1 npm run dev
```

Frontend URL: `http://localhost:3000`

## Tests

Backend:

```bash
cd backend
source .venv/bin/activate
TEST_POSTGRES_URL=postgresql://<user>:<pass>@localhost:5432/<test_db> pytest -q
```

Frontend:

```bash
cd frontend
npm run lint
npm run build
```

## Key Runtime Configuration

All runtime settings are documented in `backend/.env.example`, including:

- Navigation scope and limits (`NAV_*`)
- Markdown-first extraction controls (`MARKDOWN_*`)
- Link memory/pathfinding controls
- LLM timeouts/chunking (`LLM_*`)
- Watchdog and stall thresholds
- Optional enrichment toggles (`PHYSICIAN_ENRICHMENT_*`)

Use conservative values in production to avoid runaway crawls.

## API Overview

Primary endpoints are under `/api/v1`:

- Scrape runs: create/list/status/events/cancel/dashboard-related endpoints
- Conferences and conference years
- Physicians and linked appearances

Use the running app’s OpenAPI docs for exact request/response shapes:

- `http://localhost:8000/docs`

## Notes on PDFs

PDF support exists in the backend dependency/runtime path (`pdfplumber`) and can be controlled through environment flags (for example `NAV_PDF_ENABLED`).
Whether PDFs are actively used in a run depends on current configuration and link selection.

## Deployment

Recommended split:

- Frontend: Vercel
- Backend: Render/Railway/Fly.io/other container-friendly host
- Database: managed PostgreSQL (Render Postgres / Supabase / Neon / RDS, etc.)

For temporary demos, tunneling a local backend (for example with ngrok) works, but it is not ideal for stable production usage.

## Security

- Do not commit `.env` files or secrets.
- Keep API keys only in host secret managers/environment variables.
- Ensure CORS is restricted to your frontend domain(s).
- Rotate keys if they were ever exposed.



## Operations Tips

- Cancel active runs from scraper UI or cancel API endpoint.
- Keep run logs for debugging extraction/attribution behavior.
- Tune `NAV_*` + `LLM_*` together; increasing one without the other can degrade stability.

## License

Internal project (no open-source license specified in repository at this time).

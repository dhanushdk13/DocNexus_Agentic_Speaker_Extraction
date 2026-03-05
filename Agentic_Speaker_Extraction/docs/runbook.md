# Runbook

## Backend setup
1. Create `backend/.env` from `.env.example`.
2. Install dependencies:
   - `cd backend`
   - `python -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -e .[test]`
3. Run migrations:
   - `alembic upgrade head`
4. Start API:
   - `uvicorn app.main:app --reload --port 8000`

## Frontend setup
1. `cd frontend`
2. `npm install`
3. `NEXT_PUBLIC_API_BASE=http://localhost:8000/api/v1 npm run dev`

## Test
- `cd backend && TEST_POSTGRES_URL=postgresql://dhanush:password@localhost:5432/test_scraper_tests pytest -q`

## Required env values
- `POSTGRES_URL=postgresql://dhanush:password@localhost:5432/test_scraper`
- `DEEPSEEK_API_KEY=...`
- `DEEPSEEK_MODEL=deepseek-chat`
- `DEEPSEEK_BASE_URL=https://api.deepseek.com`

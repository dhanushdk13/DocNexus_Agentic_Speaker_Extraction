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
3. `cp .env.example .env.local`
4. `npm run dev`

## Vercel frontend deployment
1. Import the repository in Vercel.
2. Set **Root Directory** to `Agentic_Speaker_Extraction/frontend`.
3. Keep **Framework Preset** as `Next.js`.
4. Add project environment variables (Preview + Production):
   - `NEXT_PUBLIC_API_BASE=/api/v1`
   - `API_PROXY_TARGET=https://b599-171-76-80-80.ngrok-free.app/api/v1`
5. Deploy.
6. If ngrok URL changes, update `API_PROXY_TARGET` in Vercel and trigger a redeploy.
7. If UI shows `Unexpected token '<'`, your API call is receiving HTML (not JSON). Verify:
   - `NEXT_PUBLIC_API_BASE` is exactly `/api/v1` (not an ngrok URL)
   - `API_PROXY_TARGET` points to the live ngrok backend with `/api/v1`
   - redeploy with build cache disabled

## Test
- `cd backend && TEST_POSTGRES_URL=postgresql://dhanush:password@localhost:5432/test_scraper_tests pytest -q`

## Required env values
- `POSTGRES_URL=postgresql://dhanush:password@localhost:5432/test_scraper`
- `DEEPSEEK_API_KEY=...`
- `DEEPSEEK_MODEL=deepseek-chat`
- `DEEPSEEK_BASE_URL=https://api.deepseek.com`

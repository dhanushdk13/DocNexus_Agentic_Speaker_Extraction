from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.conferences import router as conferences_router
from app.api.physicians import router as physicians_router
from app.api.scrape_runs import router as scrape_runs_router
from app.config import get_settings
from app.services.runs import run_manager


def _cors_origins_from_settings() -> list[str]:
    settings = get_settings()
    return [origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_manager.start()
    try:
        yield
    finally:
        await run_manager.stop()


app = FastAPI(title="Light Conference Speaker Harvester", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins_from_settings(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(scrape_runs_router, prefix="/api/v1")
app.include_router(conferences_router, prefix="/api/v1")
app.include_router(physicians_router, prefix="/api/v1")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import psycopg
from fastapi.testclient import TestClient
import pytest
from psycopg import sql
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

TEST_POSTGRES_URL = os.environ.get(
    "TEST_POSTGRES_URL",
    "postgresql://dhanush:password@localhost:5432/test_scraper_tests",
)

# Force tests to use an isolated DB and never the app runtime DB.
os.environ["POSTGRES_URL"] = TEST_POSTGRES_URL
os.environ.setdefault("DEEPSEEK_API_KEY", "")
os.environ.setdefault("DEEPSEEK_MODEL", "deepseek-chat")
os.environ.setdefault("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("SEED_SPEAKER_STOP_THRESHOLD", "99")
os.environ.setdefault("DOMAIN_MIN_DELAY_SECONDS", "0")
os.environ.setdefault("DOMAIN_MAX_DELAY_SECONDS", "0")
os.environ.setdefault("DOMAIN_BLOCK_COOLDOWN_MIN_SECONDS", "0")
os.environ.setdefault("DOMAIN_BLOCK_COOLDOWN_MAX_SECONDS", "0")

from app.db import Base, engine  # noqa: E402
from app.main import app  # noqa: E402


def _database_name_from_url(url: str) -> str:
    return urlparse(url).path.lstrip("/")


def _admin_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.path == "/postgres":
        return url
    return parsed._replace(path="/postgres").geturl()


def _ensure_test_database_exists(url: str) -> None:
    db_name = _database_name_from_url(url)
    if not db_name or "test" not in db_name:
        raise RuntimeError(
            f"Refusing to run tests against non-test database '{db_name}'. "
            "Set TEST_POSTGRES_URL to a dedicated test DB."
        )

    admin_url = _admin_url(url)
    with psycopg.connect(admin_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(db_name)))


@pytest.fixture(scope="session", autouse=True)
def ensure_postgres_available() -> None:
    try:
        _ensure_test_database_exists(TEST_POSTGRES_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            conn.commit()
    except (OperationalError, psycopg.Error, RuntimeError) as exc:
        pytest.skip(f"Postgres unavailable for tests: {exc}")


@pytest.fixture(scope="session", autouse=True)
def create_schema(ensure_postgres_available: None) -> None:
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(autouse=True)
def clean_db() -> None:
    table_names = [table.name for table in Base.metadata.sorted_tables]
    if not table_names:
        return

    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {', '.join(table_names)} RESTART IDENTITY CASCADE"))
        conn.commit()


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def fixture_text() -> dict[str, str]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    return {
        "html": (fixtures_dir / "sample_speakers.html").read_text(encoding="utf-8"),
        "pdf_text": (fixtures_dir / "sample_pdf_text.txt").read_text(encoding="utf-8"),
        "network_json": (fixtures_dir / "sample_network.json").read_text(encoding="utf-8"),
    }

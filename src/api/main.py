"""FastAPI application entry point.

Startup sequence (lifespan):
  1. Try Snowflake BRONZE — load up to 100K rows via Arrow fetch.
  2. Fall back to fixture CSV if Snowflake is unavailable.
  3. Store the DataFrame in app.state.df for all route handlers.

Run locally:
    uv run uvicorn src.api.main:app --reload --port 8000
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.api.routes import router

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent


# ── App settings ───────────────────────────────────────────────────────────────

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    cors_origins: str = (
        "http://localhost:3000,"
        "http://localhost:5173,"
        "https://rajlakkakula.github.io"
    )
    app_env: str = "development"


# ── Data loading ───────────────────────────────────────────────────────────────

def _load_snowflake() -> pd.DataFrame:
    from src.snowflake.connection import get_snowflake_manager
    mgr = get_snowflake_manager()
    df = mgr.execute_query(
        "SELECT * FROM PRICING_DB.BRONZE.RAW_TRANSACTIONS LIMIT 100000"
    )
    if df.empty:
        raise ValueError("BRONZE table returned 0 rows — has data been loaded?")
    return df


def _load_csv() -> pd.DataFrame:
    csv_path = _ROOT / "tests" / "fixtures" / "sample_transactions.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Fixture CSV not found at {csv_path}. "
            "Run: uv run python scripts/seed_sample_data.py"
        )
    return pd.read_csv(csv_path)


def _load_data() -> tuple[pd.DataFrame, str]:
    try:
        df = _load_snowflake()
        return df, "Snowflake BRONZE"
    except Exception as exc:
        logger.warning("Snowflake unavailable (%s) — falling back to fixture CSV.", exc)

    df = _load_csv()
    return df, "fixture CSV"


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading transaction data...")
    df, source = _load_data()
    app.state.df = df
    app.state.data_source = source
    logger.info("Loaded %d rows from %s.", len(df), source)

    from src.agent.sql_agent import SqlAgent
    if source.startswith("Snowflake"):
        from src.snowflake.connection import get_snowflake_manager
        app.state.sql_agent = SqlAgent(snowflake_mgr=get_snowflake_manager())
        logger.info("SQL agent: Snowflake GOLD backend.")
    else:
        app.state.sql_agent = SqlAgent(df=df)
        logger.info("SQL agent: DuckDB offline backend.")

    yield
    logger.info("Shutting down.")


# ── Application factory ────────────────────────────────────────────────────────

def create_app() -> FastAPI:
    settings = AppSettings()

    application = FastAPI(
        title="Pricing Waterfall Agent",
        description=(
            "AI-powered pricing intelligence for the filtration industry. "
            "Decomposes Blue Price → Deductions → Invoice → Bonuses → "
            "Pocket Price → Standard Cost → Contribution Margin."
        ),
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins.split(","),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(router)

    @application.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled error on %s", request.url)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(exc)},
        )

    return application


app = create_app()

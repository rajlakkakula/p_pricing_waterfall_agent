"""FastAPI route handlers for the waterfall agent API.

Five endpoints:
  GET  /api/health      — liveness + data status
  POST /api/chat        — NL query → full agent pipeline
  GET  /api/waterfall   — direct waterfall with filter query params
  GET  /api/outliers    — direct outlier detection with filter query params
  GET  /api/trends      — YoY margin bridge with base_year + current_year
"""

from __future__ import annotations

import time

import pandas as pd
from fastapi import APIRouter, Query, Request

from src.agent.intent_parser import ParsedIntent
from src.agent.orchestrator import AnalysisResult, run_analysis
from src.analytics.outliers import OutlierFlag
from src.analytics.trends import MarginBridge, PeriodMetrics
from src.analytics.waterfall import WaterfallFilters, WaterfallResult
from src.api.models import (
    AnalysisResponse,
    BridgeModel,
    ChatRequest,
    HealthResponse,
    OutlierModel,
    PeriodModel,
    SqlChatResponse,
    WaterfallModel,
)

router = APIRouter(prefix="/api")


# ── Conversion helpers ─────────────────────────────────────────────────────────

def _wf_model(wf: WaterfallResult) -> WaterfallModel:
    return WaterfallModel(**wf.__dict__)


def _outlier_model(f: OutlierFlag) -> OutlierModel:
    return OutlierModel(
        sold_to=f.sold_to, corporate_group=f.corporate_group,
        country=f.country, pso=f.pso, material=f.material,
        year=f.year, sales_qty=f.sales_qty, volume_band=f.volume_band,
        peer_group=f.peer_group, metric=f.metric, value=f.value,
        peer_mean=f.peer_mean, z_score=f.z_score,
        severity=f.severity, direction=f.direction,
    )


def _period_model(p: PeriodMetrics) -> PeriodModel:
    return PeriodModel(**p.__dict__)


def _bridge_model(b: MarginBridge) -> BridgeModel:
    return BridgeModel(
        base_year=b.base_year, current_year=b.current_year,
        base=_period_model(b.base), current=_period_model(b.current),
        price_effect=b.price_effect, deduction_effect=b.deduction_effect,
        bonus_effect=b.bonus_effect, cost_effect=b.cost_effect,
        volume_effect=b.volume_effect, mix_effect=b.mix_effect,
        total_margin_change=b.total_margin_change,
    )


def _to_response(
    result: AnalysisResult,
    elapsed_ms: int,
    query: str | None = None,
) -> AnalysisResponse:
    return AnalysisResponse(
        status="error" if result.error else "ok",
        action=result.intent.action,
        query=query,
        waterfall=_wf_model(result.waterfall) if result.waterfall else None,
        outliers=[_outlier_model(f) for f in result.outliers] if result.outliers is not None else None,
        bridge=_bridge_model(result.bridge) if result.bridge else None,
        narrative=result.narrative,
        error=result.error,
        elapsed_ms=elapsed_ms,
    )


def _df(request: Request) -> pd.DataFrame:
    return request.app.state.df


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse)
def health(request: Request) -> HealthResponse:
    """Liveness probe — returns data source and row count."""
    return HealthResponse(
        status="ok",
        data_source=getattr(request.app.state, "data_source", "unknown"),
        row_count=len(_df(request)),
    )


@router.post("/chat", response_model=SqlChatResponse)
def chat(body: ChatRequest, request: Request) -> SqlChatResponse:
    """Natural language query → SQL agent → plain-English answer.

    Converts the question to SQL, executes it against DuckDB (offline) or
    Snowflake GOLD (live), and returns Claude's interpretation as markdown.
    Expect 3–10 s for most questions.
    """
    t0 = time.perf_counter()
    agent = getattr(request.app.state, "sql_agent", None)
    if agent is None:
        return SqlChatResponse(
            status="error",
            error="SQL agent not available. Check ANTHROPIC_API_KEY in .env.",
            elapsed_ms=0,
        )
    result = agent.ask(body.query)
    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return SqlChatResponse(
        status="ok" if not result.error else "error",
        answer=result.answer,
        sql_queries=result.sql_calls or None,
        error=result.error,
        elapsed_ms=elapsed_ms,
    )


@router.get("/waterfall", response_model=AnalysisResponse)
def waterfall(
    request: Request,
    country: str | None = Query(None),
    year: int | None = Query(None),
    material: str | None = Query(None),
    pso: str | None = Query(None),
    corporate_group: str | None = Query(None),
    sold_to: str | None = Query(None),
) -> AnalysisResponse:
    """Direct waterfall computation — no NL parsing, no Claude API call."""
    t0 = time.perf_counter()
    filters = WaterfallFilters(
        country=country, year=year, material=material,
        pso=pso, corporate_group=corporate_group, sold_to=sold_to,
    )
    intent = ParsedIntent(action="waterfall", filters=filters,
                          base_year=None, current_year=None, raw_query="")
    result = run_analysis(_df(request), intent)
    return _to_response(result, int((time.perf_counter() - t0) * 1000))


@router.get("/outliers", response_model=AnalysisResponse)
def outliers(
    request: Request,
    country: str | None = Query(None),
    year: int | None = Query(None),
    material: str | None = Query(None),
    pso: str | None = Query(None),
    corporate_group: str | None = Query(None),
    sold_to: str | None = Query(None),
) -> AnalysisResponse:
    """Direct outlier detection — no NL parsing, no Claude API call.

    Runs z-score detection within the filtered slice.
    Groups with fewer than 3 peers are skipped automatically.
    """
    t0 = time.perf_counter()
    filters = WaterfallFilters(
        country=country, year=year, material=material,
        pso=pso, corporate_group=corporate_group, sold_to=sold_to,
    )
    intent = ParsedIntent(action="outliers", filters=filters,
                          base_year=None, current_year=None, raw_query="")
    result = run_analysis(_df(request), intent)
    return _to_response(result, int((time.perf_counter() - t0) * 1000))


@router.get("/trends", response_model=AnalysisResponse)
def trends(
    request: Request,
    base_year: int = Query(..., description="Prior-year reference period."),
    current_year: int = Query(..., description="Current year being evaluated."),
    country: str | None = Query(None),
    material: str | None = Query(None),
    pso: str | None = Query(None),
) -> AnalysisResponse:
    """YoY margin bridge — no NL parsing, no Claude API call.

    Decomposes the margin change between base_year and current_year into
    price / deduction / bonus / cost / volume / mix effects.
    """
    t0 = time.perf_counter()
    filters = WaterfallFilters(country=country, material=material, pso=pso)
    intent = ParsedIntent(action="trends", filters=filters,
                          base_year=base_year, current_year=current_year, raw_query="")
    result = run_analysis(_df(request), intent)
    return _to_response(result, int((time.perf_counter() - t0) * 1000))

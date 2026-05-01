"""Pydantic request and response schemas for the waterfall API.

These models form the contract between the backend and any consumer
(frontend, external clients). All monetary values are per-unit averages
unless suffixed with _dollars or _revenue.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ── Nested component models ────────────────────────────────────────────────────

class WaterfallModel(BaseModel):
    blue_price: float
    deductions: float
    invoice_price: float
    bonuses: float
    pocket_price: float
    standard_cost: float
    material_cost: float
    contribution_margin: float
    margin_pct: float
    deduction_pct: float
    bonus_pct: float
    realization_pct: float
    leakage_pct: float
    conversion_cost: float
    total_qty: int
    transaction_count: int
    total_pocket_revenue: float
    total_margin_dollars: float


class OutlierModel(BaseModel):
    sold_to: str
    corporate_group: str
    country: str
    pso: str
    material: str
    year: int
    sales_qty: float
    volume_band: str
    peer_group: str
    metric: str
    value: float
    peer_mean: float
    z_score: float
    severity: str    # "HIGH" | "MEDIUM"
    direction: str   # "low_is_bad" | "high_is_bad"


class PeriodModel(BaseModel):
    year: int
    wavg_blue_price: float
    wavg_deductions: float
    wavg_invoice_price: float
    wavg_bonuses: float
    wavg_pocket_price: float
    wavg_standard_cost: float
    wavg_margin_pct: float
    total_qty: float
    total_pocket_revenue: float
    total_margin_dollars: float
    transaction_count: int


class BridgeModel(BaseModel):
    base_year: int
    current_year: int
    base: PeriodModel
    current: PeriodModel
    price_effect: float
    deduction_effect: float
    bonus_effect: float
    cost_effect: float
    volume_effect: float
    mix_effect: float
    total_margin_change: float


# ── Request models ─────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500,
                       description="Natural language pricing question.")


# ── Unified response model ─────────────────────────────────────────────────────

class AnalysisResponse(BaseModel):
    """Single response envelope used by every endpoint."""

    status: str                          # "ok" | "error"
    action: str | None = None            # intent action that was executed
    query: str | None = None             # original NL query (chat endpoint only)
    waterfall: WaterfallModel | None = None
    outliers: list[OutlierModel] | None = None
    bridge: BridgeModel | None = None
    narrative: str | None = None
    error: str | None = None
    elapsed_ms: int | None = None        # total wall-clock time


# ── SQL agent chat response ────────────────────────────────────────────────────

class SqlChatResponse(BaseModel):
    """Response from the SQL-agent-powered /api/chat endpoint."""

    status: str                           # "ok" | "error"
    answer: str | None = None             # narrative + tables in markdown
    sql_queries: list[str] | None = None  # SQL statements that were executed
    error: str | None = None
    elapsed_ms: int | None = None


# ── Health response ────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    data_source: str
    row_count: int

"""Integration tests for the FastAPI layer.

Uses FastAPI TestClient with:
  - app.state.df injected with a deterministic synthetic DataFrame
  - parse_intent patched so /api/chat never calls the real Claude API
  - generate_narrative patched so full_analysis tests don't call the real API
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.agent.intent_parser import ParsedIntent
from src.analytics.waterfall import WaterfallFilters


# ── Synthetic test data ────────────────────────────────────────────────────────

def _txn(year: int, qty: float, blue: float, ded: float, bon: float,
         cost: float, country: str = "USA", material: str = "HYD-001") -> dict:
    inv = blue - ded
    pocket = inv - bon
    return {
        "country": country, "material": material, "pso": "Americas",
        "corporate_group": "Corp", "sold_to": "C-001",
        "year": year, "sales_qty": qty, "sales_designation": "STD",
        "blue_jobber_price": blue, "deductions": ded,
        "invoice_price": inv, "bonuses": bon,
        "pocket_price": pocket, "standard_cost": cost,
        "material_cost": cost * 0.6,
    }


@pytest.fixture
def test_df() -> pd.DataFrame:
    rows = []
    for year in (2024, 2025):
        for i in range(6):
            rows.append(_txn(year=year, qty=float(100 + i * 10),
                             blue=100.0, ded=10.0, bon=5.0, cost=50.0))
    return pd.DataFrame(rows)


@pytest.fixture
def client(test_df: pd.DataFrame):
    """TestClient with data pre-loaded into app.state — no Snowflake needed."""
    from src.api.main import app

    with TestClient(app, raise_server_exceptions=True) as c:
        app.state.df = test_df
        app.state.data_source = "test fixture"
        yield c


def _mock_intent(action: str, **filter_kwargs) -> ParsedIntent:
    return ParsedIntent(
        action=action,
        filters=WaterfallFilters(**filter_kwargs),
        base_year=None,
        current_year=None,
        raw_query="test query",
    )


def _mock_trend_intent() -> ParsedIntent:
    return ParsedIntent(
        action="trends",
        filters=WaterfallFilters(),
        base_year=2024,
        current_year=2025,
        raw_query="test trend query",
    )


# ── GET /api/health ────────────────────────────────────────────────────────────

def test_health_returns_200(client) -> None:
    r = client.get("/api/health")
    assert r.status_code == 200


def test_health_status_ok(client) -> None:
    assert r.json()["status"] == "ok" if (r := client.get("/api/health")) else True
    r = client.get("/api/health")
    assert r.json()["status"] == "ok"


def test_health_includes_row_count(client, test_df) -> None:
    r = client.get("/api/health")
    assert r.json()["row_count"] == len(test_df)


def test_health_includes_data_source(client) -> None:
    r = client.get("/api/health")
    assert r.json()["data_source"] == "test fixture"


# ── POST /api/chat ─────────────────────────────────────────────────────────────

@patch("src.api.routes.parse_intent")
def test_chat_returns_200(mock_parse, client) -> None:
    mock_parse.return_value = _mock_intent("waterfall")
    r = client.post("/api/chat", json={"query": "Show me the waterfall"})
    assert r.status_code == 200


@patch("src.api.routes.parse_intent")
def test_chat_status_ok(mock_parse, client) -> None:
    mock_parse.return_value = _mock_intent("waterfall")
    r = client.post("/api/chat", json={"query": "Show me the waterfall"})
    assert r.json()["status"] == "ok"


@patch("src.api.routes.parse_intent")
def test_chat_returns_waterfall(mock_parse, client) -> None:
    mock_parse.return_value = _mock_intent("waterfall")
    r = client.post("/api/chat", json={"query": "waterfall"})
    body = r.json()
    assert body["waterfall"] is not None
    assert "margin_pct" in body["waterfall"]


@patch("src.api.routes.parse_intent")
def test_chat_returns_outliers(mock_parse, client) -> None:
    mock_parse.return_value = _mock_intent("outliers")
    r = client.post("/api/chat", json={"query": "show outliers"})
    body = r.json()
    assert body["outliers"] is not None
    assert isinstance(body["outliers"], list)


@patch("src.api.routes.parse_intent")
def test_chat_preserves_query(mock_parse, client) -> None:
    mock_parse.return_value = _mock_intent("waterfall")
    r = client.post("/api/chat", json={"query": "What is the margin?"})
    assert r.json()["query"] == "What is the margin?"


@patch("src.api.routes.parse_intent")
def test_chat_includes_elapsed_ms(mock_parse, client) -> None:
    mock_parse.return_value = _mock_intent("waterfall")
    r = client.post("/api/chat", json={"query": "test"})
    assert isinstance(r.json()["elapsed_ms"], int)
    assert r.json()["elapsed_ms"] >= 0


@patch("src.api.routes.parse_intent")
def test_chat_no_matching_data_returns_error(mock_parse, client) -> None:
    mock_parse.return_value = _mock_intent("waterfall", country="Narnia")
    r = client.post("/api/chat", json={"query": "Narnia waterfall"})
    body = r.json()
    assert body["status"] == "error"
    assert body["error"] is not None


@patch("src.api.routes.parse_intent")
def test_chat_trends_action(mock_parse, client) -> None:
    intent = _mock_trend_intent()
    mock_parse.return_value = intent
    r = client.post("/api/chat", json={"query": "YoY trend"})
    body = r.json()
    assert body["status"] == "ok"
    assert body["bridge"] is not None
    assert body["bridge"]["base_year"] == 2024
    assert body["bridge"]["current_year"] == 2025


@patch("src.api.routes.parse_intent")
@patch("src.api.routes.run_analysis")
def test_chat_full_analysis_with_narrative(mock_run, mock_parse, client) -> None:
    from src.agent.orchestrator import AnalysisResult
    from src.analytics.waterfall import WaterfallResult

    mock_parse.return_value = _mock_intent("full_analysis")
    wf = WaterfallResult(
        blue_price=100.0, deductions=10.0, invoice_price=90.0,
        bonuses=5.0, pocket_price=85.0, standard_cost=50.0,
        material_cost=30.0, contribution_margin=35.0,
        margin_pct=41.18, deduction_pct=10.0, bonus_pct=5.56,
        realization_pct=85.0, leakage_pct=15.0, conversion_cost=20.0,
        total_qty=1000, transaction_count=10,
        total_pocket_revenue=85000.0, total_margin_dollars=35000.0,
    )
    mock_run.return_value = AnalysisResult(
        intent=mock_parse.return_value,
        waterfall=wf,
        outliers=[],
        narrative="The margin is healthy.",
    )
    r = client.post("/api/chat", json={"query": "full analysis"})
    body = r.json()
    assert body["narrative"] == "The margin is healthy."


def test_chat_empty_query_returns_422(client) -> None:
    r = client.post("/api/chat", json={"query": ""})
    assert r.status_code == 422


def test_chat_missing_query_returns_422(client) -> None:
    r = client.post("/api/chat", json={})
    assert r.status_code == 422


# ── GET /api/waterfall ─────────────────────────────────────────────────────────

def test_waterfall_no_filters(client) -> None:
    r = client.get("/api/waterfall")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["waterfall"] is not None


def test_waterfall_country_filter(client) -> None:
    r = client.get("/api/waterfall?country=USA")
    assert r.status_code == 200
    assert r.json()["waterfall"] is not None


def test_waterfall_unknown_country_returns_error(client) -> None:
    r = client.get("/api/waterfall?country=Atlantis")
    assert r.json()["status"] == "error"


def test_waterfall_no_outliers_or_bridge(client) -> None:
    r = client.get("/api/waterfall")
    body = r.json()
    assert body["outliers"] is None
    assert body["bridge"] is None
    assert body["narrative"] is None


def test_waterfall_action_in_response(client) -> None:
    r = client.get("/api/waterfall")
    assert r.json()["action"] == "waterfall"


def test_waterfall_year_filter(client) -> None:
    r = client.get("/api/waterfall?year=2024")
    assert r.status_code == 200
    assert r.json()["waterfall"] is not None


# ── GET /api/outliers ──────────────────────────────────────────────────────────

def test_outliers_no_filters(client) -> None:
    r = client.get("/api/outliers")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert isinstance(body["outliers"], list)


def test_outliers_action_in_response(client) -> None:
    r = client.get("/api/outliers")
    assert r.json()["action"] == "outliers"


def test_outliers_no_waterfall_or_bridge(client) -> None:
    r = client.get("/api/outliers")
    body = r.json()
    assert body["waterfall"] is None
    assert body["bridge"] is None


def test_outliers_pso_filter(client) -> None:
    r = client.get("/api/outliers?pso=Americas")
    assert r.status_code == 200
    assert isinstance(r.json()["outliers"], list)


# ── GET /api/trends ────────────────────────────────────────────────────────────

def test_trends_returns_bridge(client) -> None:
    r = client.get("/api/trends?base_year=2024&current_year=2025")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["bridge"] is not None


def test_trends_bridge_years_correct(client) -> None:
    r = client.get("/api/trends?base_year=2024&current_year=2025")
    bridge = r.json()["bridge"]
    assert bridge["base_year"] == 2024
    assert bridge["current_year"] == 2025


def test_trends_bridge_effects_present(client) -> None:
    r = client.get("/api/trends?base_year=2024&current_year=2025")
    bridge = r.json()["bridge"]
    for field in ("price_effect", "deduction_effect", "cost_effect",
                  "volume_effect", "mix_effect", "total_margin_change"):
        assert field in bridge


def test_trends_missing_base_year_returns_422(client) -> None:
    r = client.get("/api/trends?current_year=2025")
    assert r.status_code == 422


def test_trends_missing_both_years_returns_422(client) -> None:
    r = client.get("/api/trends")
    assert r.status_code == 422


def test_trends_unknown_years_bridge_is_none(client) -> None:
    r = client.get("/api/trends?base_year=2020&current_year=2021")
    body = r.json()
    assert body["status"] == "ok"
    assert body["bridge"] is None

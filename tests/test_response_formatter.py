"""Unit tests for the response formatter."""

import pytest

from src.agent.intent_parser import ParsedIntent
from src.agent.orchestrator import AnalysisResult
from src.agent.response_formatter import format_error, format_response
from src.analytics.waterfall import WaterfallFilters, WaterfallResult


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_intent(action: str = "waterfall") -> ParsedIntent:
    return ParsedIntent(
        action=action,
        filters=WaterfallFilters(),
        base_year=None,
        current_year=None,
        raw_query="test query",
    )


def _make_waterfall() -> WaterfallResult:
    return WaterfallResult(
        blue_price=100.0, deductions=10.0, invoice_price=90.0,
        bonuses=5.0, pocket_price=85.0, standard_cost=50.0,
        material_cost=30.0, contribution_margin=35.0,
        margin_pct=41.18, deduction_pct=10.0, bonus_pct=5.56,
        realization_pct=85.0, leakage_pct=15.0, conversion_cost=20.0,
        total_qty=1000, transaction_count=50,
        total_pocket_revenue=85000.0, total_margin_dollars=35000.0,
    )


# ── format_response ────────────────────────────────────────────────────────────

def test_format_response_returns_dict() -> None:
    result = AnalysisResult(intent=_make_intent())
    assert isinstance(format_response(result), dict)


def test_format_response_ok_status_when_no_error() -> None:
    result = AnalysisResult(intent=_make_intent())
    assert format_response(result)["status"] == "ok"


def test_format_response_error_status_when_error_set() -> None:
    result = AnalysisResult(intent=_make_intent(), error="No data found.")
    assert format_response(result)["status"] == "error"


def test_format_response_contains_intent() -> None:
    intent = _make_intent("outliers")
    result = AnalysisResult(intent=intent)
    response = format_response(result)
    assert "intent" in response
    assert response["intent"]["action"] == "outliers"
    assert response["intent"]["raw_query"] == "test query"


def test_format_response_waterfall_serialized() -> None:
    result = AnalysisResult(intent=_make_intent(), waterfall=_make_waterfall())
    response = format_response(result)
    assert response["waterfall"] is not None
    assert response["waterfall"]["margin_pct"] == 41.18


def test_format_response_none_fields_present() -> None:
    result = AnalysisResult(intent=_make_intent())
    response = format_response(result)
    assert "waterfall" in response
    assert response["waterfall"] is None
    assert response["outliers"] is None
    assert response["bridge"] is None
    assert response["narrative"] is None


def test_format_response_narrative_included() -> None:
    result = AnalysisResult(
        intent=_make_intent(),
        waterfall=_make_waterfall(),
        narrative="The margin is healthy.",
    )
    response = format_response(result)
    assert response["narrative"] == "The margin is healthy."


def test_format_response_is_json_serializable() -> None:
    import json
    result = AnalysisResult(
        intent=_make_intent(),
        waterfall=_make_waterfall(),
        outliers=[],
        narrative="Good margin.",
    )
    response = format_response(result)
    # Should not raise
    json.dumps(response)


def test_format_response_filters_serialized() -> None:
    intent = ParsedIntent(
        action="waterfall",
        filters=WaterfallFilters(country="Germany", year=2025),
        base_year=None,
        current_year=None,
        raw_query="Germany 2025",
    )
    result = AnalysisResult(intent=intent)
    response = format_response(result)
    assert response["intent"]["filters"]["country"] == "Germany"
    assert response["intent"]["filters"]["year"] == 2025


# ── format_error ───────────────────────────────────────────────────────────────

def test_format_error_returns_dict() -> None:
    assert isinstance(format_error("Something went wrong."), dict)


def test_format_error_status_is_error() -> None:
    assert format_error("Oops")["status"] == "error"


def test_format_error_message_included() -> None:
    assert format_error("No data found.")["error"] == "No data found."

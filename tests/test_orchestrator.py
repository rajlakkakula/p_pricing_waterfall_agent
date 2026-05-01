"""Unit tests for the analysis orchestrator."""

from unittest.mock import patch

import pandas as pd
import pytest

from src.agent.intent_parser import ParsedIntent
from src.agent.orchestrator import AnalysisResult, run_analysis
from src.analytics.waterfall import WaterfallFilters


# ── Fixtures ───────────────────────────────────────────────────────────────────

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


def _make_intent(
    action: str,
    *,
    country: str | None = None,
    year: int | None = None,
    material: str | None = None,
    pso: str | None = None,
    base_year: int | None = None,
    current_year: int | None = None,
) -> ParsedIntent:
    return ParsedIntent(
        action=action,
        filters=WaterfallFilters(country=country, year=year, material=material, pso=pso),
        base_year=base_year,
        current_year=current_year,
        raw_query="",
    )


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Minimal two-year dataset large enough for peer-group detection."""
    rows = []
    for year in (2024, 2025):
        for i in range(6):
            rows.append(_txn(year=year, qty=float(100 + i * 10), blue=100.0,
                             ded=10.0, bon=5.0, cost=50.0))
    return pd.DataFrame(rows)


@pytest.fixture
def two_year_df() -> pd.DataFrame:
    rows = [
        _txn(year=2024, qty=100.0, blue=100.0, ded=10.0, bon=5.0, cost=50.0),
        _txn(year=2025, qty=100.0, blue=110.0, ded=10.0, bon=5.0, cost=50.0),
    ]
    return pd.DataFrame(rows)


# ── AnalysisResult type ────────────────────────────────────────────────────────

def test_run_analysis_returns_analysis_result_type(sample_df: pd.DataFrame) -> None:
    intent = _make_intent("waterfall")
    result = run_analysis(sample_df, intent)
    assert isinstance(result, AnalysisResult)


# ── waterfall action ──────────────────────────────────────────────────────────

def test_waterfall_action_populates_waterfall(sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("waterfall"))
    assert result.waterfall is not None
    assert result.outliers is None
    assert result.bridge is None
    assert result.narrative is None
    assert result.error is None


def test_waterfall_action_with_country_filter(sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("waterfall", country="USA"))
    assert result.waterfall is not None
    assert result.error is None


def test_waterfall_action_no_matching_data_sets_error(sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("waterfall", country="Narnia"))
    assert result.error is not None
    assert result.waterfall is None


# ── outliers action ───────────────────────────────────────────────────────────

def test_outliers_action_populates_outliers(sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("outliers"))
    assert result.outliers is not None
    assert isinstance(result.outliers, list)
    assert result.waterfall is None
    assert result.narrative is None


def test_outliers_action_homogeneous_data_returns_empty_list(sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("outliers"))
    # Homogeneous data → no outliers
    assert result.outliers == []


# ── trends action ─────────────────────────────────────────────────────────────

def test_trends_action_populates_bridge(two_year_df: pd.DataFrame) -> None:
    result = run_analysis(two_year_df, _make_intent("trends", base_year=2024, current_year=2025))
    assert result.bridge is not None
    assert result.waterfall is None
    assert result.narrative is None


def test_trends_action_no_years_skips_bridge(sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("trends"))
    assert result.bridge is None


def test_trends_action_missing_year_returns_none_bridge(sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("trends", base_year=2020, current_year=2021))
    assert result.bridge is None


# ── full_analysis action ──────────────────────────────────────────────────────

@patch("src.agent.orchestrator.generate_narrative", return_value="Mock narrative.")
def test_full_analysis_populates_all_fields(mock_gen, sample_df: pd.DataFrame) -> None:
    intent = _make_intent("full_analysis", base_year=2024, current_year=2025)
    result = run_analysis(sample_df, intent)
    assert result.waterfall is not None
    assert result.outliers is not None
    assert result.narrative == "Mock narrative."
    assert result.error is None


@patch("src.agent.orchestrator.generate_narrative", return_value="Narrative.")
def test_full_analysis_no_years_skips_bridge(mock_gen, sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("full_analysis"))
    assert result.waterfall is not None
    assert result.bridge is None
    assert result.narrative == "Narrative."


@patch("src.agent.orchestrator.generate_narrative", return_value="Narrative.")
def test_full_analysis_no_matching_data_sets_error(mock_gen, sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("full_analysis", country="Nowhere"))
    assert result.error is not None
    assert result.narrative is None
    mock_gen.assert_not_called()


# ── narrative action ──────────────────────────────────────────────────────────

@patch("src.agent.orchestrator.generate_narrative", return_value="The narrative.")
def test_narrative_action_computes_waterfall_and_generates(mock_gen, sample_df: pd.DataFrame) -> None:
    result = run_analysis(sample_df, _make_intent("narrative"))
    assert result.waterfall is not None
    assert result.narrative == "The narrative."
    mock_gen.assert_called_once()


# ── intent preserved in result ────────────────────────────────────────────────

def test_intent_preserved_in_result(sample_df: pd.DataFrame) -> None:
    intent = _make_intent("waterfall", country="USA")
    result = run_analysis(sample_df, intent)
    assert result.intent is intent

"""Unit tests for YoY trend analysis and margin bridge decomposition."""

import pandas as pd
import pytest

from src.analytics.trends import (
    MarginBridge,
    PeriodMetrics,
    compute_margin_bridge,
    compute_period_metrics,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

def _txn(year: int, qty: float, blue: float, ded: float, bon: float, cost: float) -> dict:
    inv = blue - ded
    pocket = inv - bon
    return {
        "country": "USA", "material": "HYD-001", "pso": "Americas",
        "corporate_group": "Corp", "sold_to": "C-001",
        "year": year, "sales_qty": qty,
        "blue_jobber_price": blue, "deductions": ded,
        "invoice_price": inv, "bonuses": bon,
        "pocket_price": pocket, "standard_cost": cost,
        "material_cost": cost * 0.6,
    }


@pytest.fixture
def two_year_df() -> pd.DataFrame:
    """Two-year dataset with fully deterministic known values."""
    rows = [
        # Base year 2024: blue=100, ded=10, bon=5, cost=50 → pocket=85, margin=35, qty=100
        _txn(year=2024, qty=100.0, blue=100.0, ded=10.0, bon=5.0, cost=50.0),
        # Current year 2025: blue=110, ded=10, bon=5, cost=50 → pocket=95, margin=45, qty=100
        _txn(year=2025, qty=100.0, blue=110.0, ded=10.0, bon=5.0, cost=50.0),
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def two_year_volume_change_df() -> pd.DataFrame:
    """Dataset where only volume changes between years (all per-unit metrics identical)."""
    rows = [
        _txn(year=2024, qty=100.0, blue=100.0, ded=10.0, bon=5.0, cost=50.0),
        _txn(year=2025, qty=200.0, blue=100.0, ded=10.0, bon=5.0, cost=50.0),
    ]
    return pd.DataFrame(rows)


# ── compute_period_metrics ─────────────────────────────────────────────────────

def test_period_metrics_returns_correct_type(two_year_df: pd.DataFrame) -> None:
    result = compute_period_metrics(two_year_df, 2024)
    assert isinstance(result, PeriodMetrics)


def test_period_metrics_known_values(two_year_df: pd.DataFrame) -> None:
    result = compute_period_metrics(two_year_df, 2024)
    assert result is not None
    assert result.year == 2024
    assert result.wavg_blue_price == 100.0
    assert result.wavg_pocket_price == 85.0       # 100 - 10 - 5
    assert result.wavg_standard_cost == 50.0
    # margin = 85 - 50 = 35; margin_pct = 35/85*100 ≈ 41.18%
    assert abs(result.wavg_margin_pct - (35 / 85 * 100)) < 0.01
    assert result.total_qty == 100.0
    assert result.total_pocket_revenue == 100.0 * 85.0   # qty * pocket
    assert result.total_margin_dollars == 100.0 * 35.0   # qty * margin


def test_period_metrics_missing_year_returns_none(two_year_df: pd.DataFrame) -> None:
    result = compute_period_metrics(two_year_df, 2022)
    assert result is None


def test_period_metrics_missing_column_raises() -> None:
    df = pd.DataFrame([{"year": 2024, "sales_qty": 100}])
    with pytest.raises(ValueError, match="Missing required columns"):
        compute_period_metrics(df, 2024)


def test_period_metrics_zero_qty_excluded() -> None:
    rows = [
        _txn(year=2024, qty=0.0,   blue=100.0, ded=10.0, bon=5.0, cost=50.0),
        _txn(year=2024, qty=100.0, blue=100.0, ded=10.0, bon=5.0, cost=50.0),
    ]
    df = pd.DataFrame(rows)
    result = compute_period_metrics(df, 2024)
    assert result is not None
    assert result.total_qty == 100.0   # zero-qty row excluded


def test_period_metrics_transaction_count(two_year_df: pd.DataFrame) -> None:
    result = compute_period_metrics(two_year_df, 2024)
    assert result is not None
    assert result.transaction_count == 1


# ── compute_margin_bridge ──────────────────────────────────────────────────────

def test_margin_bridge_returns_correct_type(two_year_df: pd.DataFrame) -> None:
    bridge = compute_margin_bridge(two_year_df, base_year=2024, current_year=2025)
    assert isinstance(bridge, MarginBridge)


def test_margin_bridge_total_change_known(two_year_df: pd.DataFrame) -> None:
    """Base margin=3500, current margin=4500 → total_change=1000."""
    bridge = compute_margin_bridge(two_year_df, 2024, 2025)
    assert bridge is not None
    # base:    pocket=85, cost=50, qty=100 → margin_dollars=3500
    # current: pocket=95, cost=50, qty=100 → margin_dollars=4500
    assert bridge.total_margin_change == 1000.0


def test_margin_bridge_price_effect_only(two_year_df: pd.DataFrame) -> None:
    """Only blue price changed (+10); all other levers unchanged."""
    bridge = compute_margin_bridge(two_year_df, 2024, 2025)
    assert bridge is not None
    # price_effect = (110 - 100) * 100 = 1000
    assert bridge.price_effect == 1000.0
    assert bridge.deduction_effect == 0.0
    assert bridge.bonus_effect == 0.0
    assert bridge.cost_effect == 0.0
    assert bridge.volume_effect == 0.0


def test_margin_bridge_effects_sum_to_total(two_year_df: pd.DataFrame) -> None:
    """All effects + mix_effect must sum to total_margin_change."""
    bridge = compute_margin_bridge(two_year_df, 2024, 2025)
    assert bridge is not None
    effect_sum = (
        bridge.price_effect
        + bridge.deduction_effect
        + bridge.bonus_effect
        + bridge.cost_effect
        + bridge.volume_effect
        + bridge.mix_effect
    )
    assert abs(effect_sum - bridge.total_margin_change) < 0.02   # floating-point tolerance


def test_margin_bridge_volume_effect(two_year_volume_change_df: pd.DataFrame) -> None:
    """When only volume changes, volume_effect should equal total_margin_change."""
    bridge = compute_margin_bridge(two_year_volume_change_df, 2024, 2025)
    assert bridge is not None
    # base margin per unit = 85 - 50 = 35; delta_qty = 100; volume_effect = 3500
    assert bridge.volume_effect == 3500.0
    assert bridge.price_effect == 0.0
    assert bridge.total_margin_change == 3500.0


def test_margin_bridge_missing_year_returns_none(two_year_df: pd.DataFrame) -> None:
    bridge = compute_margin_bridge(two_year_df, base_year=2020, current_year=2025)
    assert bridge is None


def test_margin_bridge_negative_effects() -> None:
    """Cost increase and deduction increase should produce negative effects."""
    rows = [
        _txn(year=2024, qty=100.0, blue=100.0, ded=10.0, bon=5.0, cost=50.0),
        _txn(year=2025, qty=100.0, blue=100.0, ded=20.0, bon=5.0, cost=60.0),
    ]
    df = pd.DataFrame(rows)
    bridge = compute_margin_bridge(df, 2024, 2025)
    assert bridge is not None
    assert bridge.deduction_effect < 0   # more deductions = worse
    assert bridge.cost_effect < 0        # higher cost = worse
    assert bridge.total_margin_change < 0

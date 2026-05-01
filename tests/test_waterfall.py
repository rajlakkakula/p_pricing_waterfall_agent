"""Unit tests for waterfall computation with known-answer test cases."""

import pandas as pd
import pytest

from src.analytics.waterfall import WaterfallFilters, WaterfallResult, compute_waterfall


@pytest.fixture
def sample_transactions() -> pd.DataFrame:
    """Create a small DataFrame with known values for deterministic testing."""
    return pd.DataFrame([
        {
            "country": "USA", "year": 2025, "material": "HYD-001",
            "pso": "Americas", "corporate_group": "Komatsu", "sold_to": "C-100",
            "sales_qty": 100, "blue_jobber_price": 100.00, "deductions": 15.00,
            "invoice_price": 85.00, "bonuses": 8.00, "pocket_price": 77.00,
            "standard_cost": 40.00, "material_cost": 25.00,
        },
        {
            "country": "USA", "year": 2025, "material": "AIR-042",
            "pso": "Americas", "corporate_group": "Caterpillar", "sold_to": "C-200",
            "sales_qty": 200, "blue_jobber_price": 50.00, "deductions": 5.00,
            "invoice_price": 45.00, "bonuses": 3.00, "pocket_price": 42.00,
            "standard_cost": 20.00, "material_cost": 12.00,
        },
        {
            "country": "Germany", "year": 2025, "material": "HYD-001",
            "pso": "EMEA", "corporate_group": "Siemens", "sold_to": "C-300",
            "sales_qty": 50, "blue_jobber_price": 120.00, "deductions": 20.00,
            "invoice_price": 100.00, "bonuses": 10.00, "pocket_price": 90.00,
            "standard_cost": 45.00, "material_cost": 28.00,
        },
    ])


def test_compute_waterfall_unfiltered(sample_transactions: pd.DataFrame) -> None:
    """Test waterfall computation across all transactions."""
    result = compute_waterfall(sample_transactions)

    assert result is not None
    assert result.transaction_count == 3
    assert result.total_qty == 350

    # Volume-weighted blue price: (100*100 + 200*50 + 50*120) / 350 = 22000/350 ≈ 62.86
    expected_blue = (100 * 100 + 200 * 50 + 50 * 120) / 350
    assert abs(result.blue_price - expected_blue) < 0.01


def test_compute_waterfall_with_country_filter(sample_transactions: pd.DataFrame) -> None:
    """Test waterfall filtered to a single country."""
    filters = WaterfallFilters(country="USA")
    result = compute_waterfall(sample_transactions, filters)

    assert result is not None
    assert result.transaction_count == 2
    assert result.total_qty == 300


def test_compute_waterfall_with_pso_filter(sample_transactions: pd.DataFrame) -> None:
    """Test waterfall filtered by PSO."""
    filters = WaterfallFilters(pso="EMEA")
    result = compute_waterfall(sample_transactions, filters)

    assert result is not None
    assert result.transaction_count == 1
    assert result.blue_price == 120.00
    assert result.pocket_price == 90.00
    assert result.contribution_margin == 45.00
    assert result.margin_pct == 50.00


def test_compute_waterfall_known_margin(sample_transactions: pd.DataFrame) -> None:
    """Test that margin calculation is correct for a single known transaction."""
    filters = WaterfallFilters(sold_to="C-100")
    result = compute_waterfall(sample_transactions, filters)

    assert result is not None
    # Pocket price 77 - standard cost 40 = margin 37
    assert result.contribution_margin == 37.00
    # Margin % = 37/77 * 100 ≈ 48.05
    assert abs(result.margin_pct - 48.05) < 0.1
    # Realization = 77/100 * 100 = 77%
    assert result.realization_pct == 77.00


def test_compute_waterfall_empty_result(sample_transactions: pd.DataFrame) -> None:
    """Test that filtering to no data returns None."""
    filters = WaterfallFilters(country="Japan")
    result = compute_waterfall(sample_transactions, filters)

    assert result is None


def test_compute_waterfall_zero_quantity() -> None:
    """Test that zero-quantity transactions are excluded."""
    df = pd.DataFrame([
        {
            "country": "USA", "year": 2025, "material": "X",
            "pso": "A", "corporate_group": "B", "sold_to": "C",
            "sales_qty": 0, "blue_jobber_price": 100.00, "deductions": 10.00,
            "invoice_price": 90.00, "bonuses": 5.00, "pocket_price": 85.00,
            "standard_cost": 40.00, "material_cost": 25.00,
        },
    ])
    result = compute_waterfall(df)
    assert result is None


def test_compute_waterfall_missing_columns() -> None:
    """Test that missing required columns raise ValueError."""
    df = pd.DataFrame([{"country": "USA", "sales_qty": 100}])

    with pytest.raises(ValueError, match="Missing required columns"):
        compute_waterfall(df)


def test_compute_waterfall_revenue_calculation(sample_transactions: pd.DataFrame) -> None:
    """Test total pocket revenue calculation."""
    result = compute_waterfall(sample_transactions)

    assert result is not None
    # Revenue = 100*77 + 200*42 + 50*90 = 7700 + 8400 + 4500 = 20600
    assert result.total_pocket_revenue == 20600.00

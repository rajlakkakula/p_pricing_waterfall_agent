"""Unit tests for outlier detection with known-answer inputs."""

import pandas as pd
import pytest

from src.analytics.outliers import (
    OutlierFlag,
    assign_volume_bands,
    detect_outliers,
    summarize_outliers,
)

# ── Shared fixture ─────────────────────────────────────────────────────────────

@pytest.fixture
def base_row() -> dict:
    """Prototype for a normal transaction in peer group HYD-001|USA|*."""
    return {
        "country": "USA", "year": 2025, "material": "HYD-001",
        "pso": "Americas", "corporate_group": "Normal Corp", "sold_to": "C-NORM",
        "sales_qty": 100.0,
        "blue_jobber_price": 100.0, "deductions": 10.0,   # 10% deductions (normal)
        "invoice_price": 90.0, "bonuses": 5.0,
        "pocket_price": 85.0,
        "standard_cost": 50.0,   # margin ~41% (healthy)
        "material_cost": 30.0,
    }


def _make_peer_group(base: dict, n: int = 6) -> list[dict]:
    """Return n copies of base row (forms a stable peer group)."""
    return [dict(base, sold_to=f"C-PEER{i}") for i in range(n)]


# ── assign_volume_bands ────────────────────────────────────────────────────────

def test_volume_band_low():
    df = pd.DataFrame({"sales_qty": [10.0, 20.0, 30.0, 100.0, 200.0, 500.0]})
    bands = assign_volume_bands(df)
    assert bands.iloc[0] == "LOW"
    assert bands.iloc[-1] == "HIGH"


def test_volume_band_all_same():
    df = pd.DataFrame({"sales_qty": [100.0] * 10})
    bands = assign_volume_bands(df)
    # All identical values: all fall into same band — no crash
    assert len(bands) == 10


# ── detect_outliers ────────────────────────────────────────────────────────────

def test_detect_outliers_clean_data_no_flags(base_row: dict):
    """Homogeneous peer group should produce zero flags."""
    rows = _make_peer_group(base_row, n=8)
    df = pd.DataFrame(rows)
    flags = detect_outliers(df)
    assert flags == []


def test_detect_outliers_high_deduction_flagged(base_row: dict):
    """A transaction with very high deductions should be flagged."""
    rows = _make_peer_group(base_row, n=8)
    # Inject one outlier with deduction_pct ~45% vs normal ~10%
    outlier = dict(base_row, sold_to="C-BAD", deductions=45.0, invoice_price=55.0,
                   bonuses=2.0, pocket_price=53.0, standard_cost=50.0)
    rows.append(outlier)
    df = pd.DataFrame(rows)

    flags = detect_outliers(df)
    deduction_flags = [f for f in flags if f.metric == "deduction_pct" and f.sold_to == "C-BAD"]
    assert len(deduction_flags) >= 1
    assert deduction_flags[0].z_score > 0   # high deductions → positive z
    assert deduction_flags[0].severity in ("HIGH", "MEDIUM")


def test_detect_outliers_low_margin_flagged(base_row: dict):
    """A transaction with near-zero margin should be flagged on margin_pct."""
    rows = _make_peer_group(base_row, n=8)
    # Normal margin ~41%; inject one at ~2%
    outlier = dict(base_row, sold_to="C-LOW",
                   pocket_price=85.0, standard_cost=83.0)  # margin ≈ 2.4%
    rows.append(outlier)
    df = pd.DataFrame(rows)

    flags = detect_outliers(df)
    margin_flags = [f for f in flags if f.metric == "margin_pct" and f.sold_to == "C-LOW"]
    assert len(margin_flags) >= 1
    assert margin_flags[0].z_score < 0   # low margin → negative z


def test_detect_outliers_missing_columns_raises():
    df = pd.DataFrame([{"country": "USA", "sales_qty": 100}])
    with pytest.raises(ValueError, match="Missing required columns"):
        detect_outliers(df)


def test_detect_outliers_zero_qty_excluded(base_row: dict):
    """Zero-quantity rows should be silently excluded, not crash."""
    rows = _make_peer_group(base_row, n=5)
    rows.append(dict(base_row, sold_to="C-ZERO", sales_qty=0.0))
    df = pd.DataFrame(rows)
    flags = detect_outliers(df)
    assert all(f.sold_to != "C-ZERO" for f in flags)


def test_detect_outliers_small_peer_group_skipped(base_row: dict):
    """Groups with fewer than MIN_PEER_SIZE members should be skipped (no crash)."""
    # Only 2 rows — below MIN_PEER_SIZE=3, so no flags expected
    rows = [dict(base_row, sold_to="C-A"), dict(base_row, sold_to="C-B")]
    df = pd.DataFrame(rows)
    flags = detect_outliers(df)
    assert flags == []


def test_detect_outliers_severity_levels(base_row: dict):
    """Extreme outlier should be HIGH; moderate should be MEDIUM."""
    rows = _make_peer_group(base_row, n=10)
    # Extreme: deduction_pct far above normal
    extreme = dict(base_row, sold_to="C-EXTREME", deductions=60.0,
                   invoice_price=40.0, bonuses=2.0, pocket_price=38.0, standard_cost=35.0)
    rows.append(extreme)
    df = pd.DataFrame(rows)

    flags = detect_outliers(df)
    severities = {f.severity for f in flags if f.sold_to == "C-EXTREME"}
    assert "HIGH" in severities


def test_detect_outliers_returns_outlier_flag_type(base_row: dict):
    """Each result must be an OutlierFlag instance with expected attributes."""
    rows = _make_peer_group(base_row, n=8)
    outlier = dict(base_row, sold_to="C-BAD", deductions=50.0, invoice_price=50.0,
                   bonuses=2.0, pocket_price=48.0, standard_cost=45.0)
    rows.append(outlier)
    df = pd.DataFrame(rows)

    flags = detect_outliers(df)
    assert all(isinstance(f, OutlierFlag) for f in flags)
    for f in flags:
        assert f.severity in ("HIGH", "MEDIUM")
        assert f.metric in ("margin_pct", "deduction_pct", "bonus_pct", "realization_pct")


# ── summarize_outliers ─────────────────────────────────────────────────────────

def test_summarize_outliers_empty():
    df = summarize_outliers([])
    assert df.empty


def test_summarize_outliers_sorted_by_abs_z(base_row: dict):
    """summarize_outliers should return rows sorted by |z_score| descending."""
    rows = _make_peer_group(base_row, n=8)
    rows.append(dict(base_row, sold_to="C-BAD", deductions=50.0, invoice_price=50.0,
                     bonuses=2.0, pocket_price=48.0, standard_cost=46.0))
    df_input = pd.DataFrame(rows)

    flags = detect_outliers(df_input)
    summary = summarize_outliers(flags)

    if not summary.empty:
        abs_z = summary["z_score"].abs()
        assert (abs_z.diff().dropna() <= 0).all(), "Rows not sorted by |z_score| descending"

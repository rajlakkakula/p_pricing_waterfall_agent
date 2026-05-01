"""Period-over-period trend analysis and margin bridge decomposition.

Decomposes the year-over-year change in total margin dollars into five
first-order effects:

    price_effect      — change in avg blue price × base volume
    deduction_effect  — change in avg deductions × base volume  (negative = more leakage)
    bonus_effect      — change in avg bonuses × base volume     (negative = more leakage)
    cost_effect       — change in avg standard cost × base volume (negative = cost inflation)
    volume_effect     — change in volume × base margin-per-unit
    mix_effect        — residual (accounts for interaction terms and channel/product mix shift)

All effects sum to total_margin_change (within floating-point tolerance).
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class PeriodMetrics:
    """Volume-weighted averages and totals for a single year."""

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


@dataclass
class MarginBridge:
    """YoY margin bridge between base_year and current_year."""

    base_year: int
    current_year: int
    base: PeriodMetrics
    current: PeriodMetrics

    # First-order bridge effects (in margin-dollar terms)
    price_effect: float       # higher blue price → positive
    deduction_effect: float   # higher deductions → negative
    bonus_effect: float       # higher bonuses → negative
    cost_effect: float        # higher std cost → negative
    volume_effect: float      # more volume at base margin → positive/negative
    mix_effect: float         # residual interaction term

    total_margin_change: float  # current.total_margin_dollars - base.total_margin_dollars


def _wavg(df: pd.DataFrame, col: str) -> float:
    """Volume-weighted average of col using sales_qty as weights."""
    total_qty = df["sales_qty"].sum()
    if total_qty == 0:
        return 0.0
    return float(np.average(df[col], weights=df["sales_qty"]))


def compute_period_metrics(df: pd.DataFrame, year: int) -> PeriodMetrics | None:
    """Compute volume-weighted metrics for a single year.

    Args:
        df: Transaction-level DataFrame with 15 source fields.
        year: The year to filter to.

    Returns:
        PeriodMetrics, or None if no data exists for that year.
    """
    required = [
        "year", "sales_qty", "blue_jobber_price", "deductions",
        "invoice_price", "bonuses", "pocket_price", "standard_cost",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    period = df[(df["year"] == year) & (df["sales_qty"] > 0)].copy()
    if period.empty:
        return None

    pocket = _wavg(period, "pocket_price")
    std_cost = _wavg(period, "standard_cost")
    margin_per_unit = pocket - std_cost
    margin_pct = (margin_per_unit / pocket * 100) if pocket != 0 else 0.0

    total_qty = float(period["sales_qty"].sum())
    total_revenue = float((period["pocket_price"] * period["sales_qty"]).sum())
    total_margin = float(
        ((period["pocket_price"] - period["standard_cost"]) * period["sales_qty"]).sum()
    )

    return PeriodMetrics(
        year=year,
        wavg_blue_price=round(_wavg(period, "blue_jobber_price"), 4),
        wavg_deductions=round(_wavg(period, "deductions"), 4),
        wavg_invoice_price=round(_wavg(period, "invoice_price"), 4),
        wavg_bonuses=round(_wavg(period, "bonuses"), 4),
        wavg_pocket_price=round(pocket, 4),
        wavg_standard_cost=round(std_cost, 4),
        wavg_margin_pct=round(margin_pct, 2),
        total_qty=round(total_qty, 4),
        total_pocket_revenue=round(total_revenue, 2),
        total_margin_dollars=round(total_margin, 2),
        transaction_count=len(period),
    )


def compute_margin_bridge(
    df: pd.DataFrame,
    base_year: int,
    current_year: int,
) -> MarginBridge | None:
    """Decompose the YoY margin change into first-order price / cost / volume / mix effects.

    Args:
        df: Transaction-level DataFrame with 15 source fields (may span multiple years).
        base_year: The prior-year reference period.
        current_year: The current year being evaluated.

    Returns:
        MarginBridge, or None if data is missing for either year.
    """
    base = compute_period_metrics(df, base_year)
    current = compute_period_metrics(df, current_year)

    if base is None or current is None:
        return None

    total_margin_change = current.total_margin_dollars - base.total_margin_dollars

    # Each effect = (delta in that lever) × base_qty, expressed in total margin dollars.
    # base_margin_per_unit is the per-unit margin in the base year.
    base_margin_per_unit = base.wavg_pocket_price - base.wavg_standard_cost
    delta_qty = current.total_qty - base.total_qty

    price_effect      = (current.wavg_blue_price   - base.wavg_blue_price)   * base.total_qty
    deduction_effect  = -(current.wavg_deductions   - base.wavg_deductions)   * base.total_qty
    bonus_effect      = -(current.wavg_bonuses       - base.wavg_bonuses)       * base.total_qty
    cost_effect       = -(current.wavg_standard_cost - base.wavg_standard_cost) * base.total_qty
    volume_effect     = delta_qty * base_margin_per_unit

    explained = price_effect + deduction_effect + bonus_effect + cost_effect + volume_effect
    mix_effect = total_margin_change - explained

    return MarginBridge(
        base_year=base_year,
        current_year=current_year,
        base=base,
        current=current,
        price_effect=round(price_effect, 2),
        deduction_effect=round(deduction_effect, 2),
        bonus_effect=round(bonus_effect, 2),
        cost_effect=round(cost_effect, 2),
        volume_effect=round(volume_effect, 2),
        mix_effect=round(mix_effect, 2),
        total_margin_change=round(total_margin_change, 2),
    )

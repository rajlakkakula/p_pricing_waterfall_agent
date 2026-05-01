"""Core waterfall computation engine.

Computes volume-weighted average waterfall metrics for any combination
of dimension filters. All calculations use sales_qty as the weighting factor.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class WaterfallFilters:
    """Filter criteria for waterfall computation."""

    country: str | None = None
    year: int | None = None
    material: str | None = None
    category: str | None = None
    pso: str | None = None
    corporate_group: str | None = None
    sold_to: str | None = None


@dataclass
class WaterfallResult:
    """Volume-weighted waterfall computation result."""

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


def apply_filters(df: pd.DataFrame, filters: WaterfallFilters) -> pd.DataFrame:
    """Apply dimension filters to the transaction DataFrame."""
    filtered = df.copy()

    filter_map = {
        "country": filters.country,
        "year": filters.year,
        "material": filters.material,
        "pso": filters.pso,
        "corporate_group": filters.corporate_group,
        "sold_to": filters.sold_to,
    }

    for col, value in filter_map.items():
        if value is not None and col in filtered.columns:
            filtered = filtered[filtered[col] == value]

    return filtered


def compute_waterfall(df: pd.DataFrame, filters: WaterfallFilters | None = None) -> WaterfallResult | None:
    """Compute volume-weighted waterfall metrics.

    Args:
        df: Transaction-level DataFrame with all 15 source fields.
        filters: Optional dimension filters to apply.

    Returns:
        WaterfallResult with weighted averages, or None if no data matches.

    Raises:
        ValueError: If required columns are missing from the DataFrame.
    """
    required_cols = [
        "sales_qty", "blue_jobber_price", "deductions", "invoice_price",
        "bonuses", "pocket_price", "standard_cost", "material_cost",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if filters:
        df = apply_filters(df, filters)

    # Exclude zero-quantity transactions
    df = df[df["sales_qty"] > 0]

    if df.empty:
        return None

    total_qty = df["sales_qty"].sum()
    if total_qty == 0:
        return None

    def wavg(col: str) -> float:
        """Compute volume-weighted average for a column."""
        return float(np.average(df[col], weights=df["sales_qty"]))

    blue = wavg("blue_jobber_price")
    ded = wavg("deductions")
    inv = wavg("invoice_price")
    bon = wavg("bonuses")
    pocket = wavg("pocket_price")
    std_cost = wavg("standard_cost")
    mat_cost = wavg("material_cost")

    margin = pocket - std_cost
    margin_pct = (margin / pocket * 100) if pocket != 0 else 0.0
    ded_pct = (ded / blue * 100) if blue != 0 else 0.0
    bon_pct = (bon / inv * 100) if inv != 0 else 0.0
    real_pct = (pocket / blue * 100) if blue != 0 else 0.0
    leak_pct = ((blue - pocket) / blue * 100) if blue != 0 else 0.0

    return WaterfallResult(
        blue_price=round(blue, 4),
        deductions=round(ded, 4),
        invoice_price=round(inv, 4),
        bonuses=round(bon, 4),
        pocket_price=round(pocket, 4),
        standard_cost=round(std_cost, 4),
        material_cost=round(mat_cost, 4),
        contribution_margin=round(margin, 4),
        margin_pct=round(margin_pct, 2),
        deduction_pct=round(ded_pct, 2),
        bonus_pct=round(bon_pct, 2),
        realization_pct=round(real_pct, 2),
        leakage_pct=round(leak_pct, 2),
        conversion_cost=round(std_cost - mat_cost, 4),
        total_qty=int(total_qty),
        transaction_count=len(df),
        total_pocket_revenue=round(float((df["pocket_price"] * df["sales_qty"]).sum()), 2),
        total_margin_dollars=round(
            float(((df["pocket_price"] - df["standard_cost"]) * df["sales_qty"]).sum()), 2
        ),
    )

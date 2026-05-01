"""Statistical outlier detection for pricing anomalies.

Detects outlier transactions using z-scores within peer groups defined by
material × country × volume band. Each metric is scored independently so
a customer can be flagged for excessive deductions without being flagged
for margin issues.

Peer group logic:
  - volume_band: LOW (<p33), MEDIUM (p33–p67), HIGH (>p67) of sales_qty
  - Groups with fewer than MIN_PEER_SIZE members are skipped (too sparse)
  - |z_score| >= HIGH_Z  → severity HIGH
  - |z_score| >= MED_Z   → severity MEDIUM
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

MIN_PEER_SIZE = 3
HIGH_Z = 2.5
MED_Z = 1.8

METRICS = {
    "margin_pct":     "low_is_bad",   # flag unusually LOW margin
    "deduction_pct":  "high_is_bad",  # flag unusually HIGH deductions
    "bonus_pct":      "high_is_bad",
    "realization_pct": "low_is_bad",
}


@dataclass
class OutlierFlag:
    """A single outlier detection result for one transaction × metric."""

    row_idx: int
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
    peer_std: float
    z_score: float
    severity: str   # "HIGH" | "MEDIUM"
    direction: str  # "low_is_bad" | "high_is_bad"


def _compute_metric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived percentage columns if not already present."""
    df = df.copy()
    if "margin_pct" not in df.columns:
        df["margin_pct"] = np.where(
            df["pocket_price"] != 0,
            (df["pocket_price"] - df["standard_cost"]) / df["pocket_price"] * 100,
            0.0,
        )
    if "deduction_pct" not in df.columns:
        df["deduction_pct"] = np.where(
            df["blue_jobber_price"] != 0,
            df["deductions"] / df["blue_jobber_price"] * 100,
            0.0,
        )
    if "bonus_pct" not in df.columns:
        df["bonus_pct"] = np.where(
            df["invoice_price"] != 0,
            df["bonuses"] / df["invoice_price"] * 100,
            0.0,
        )
    if "realization_pct" not in df.columns:
        df["realization_pct"] = np.where(
            df["blue_jobber_price"] != 0,
            df["pocket_price"] / df["blue_jobber_price"] * 100,
            0.0,
        )
    return df


def assign_volume_bands(df: pd.DataFrame) -> pd.Series:
    """Assign LOW / MEDIUM / HIGH volume bands from sales_qty percentiles.

    Falls back to "MEDIUM" for all rows when percentiles are non-distinct
    (e.g., all quantities are identical).
    """
    p33 = df["sales_qty"].quantile(0.33)
    p67 = df["sales_qty"].quantile(0.67)

    if p33 == p67:
        return pd.Series(["MEDIUM"] * len(df), index=df.index)

    return pd.cut(
        df["sales_qty"],
        bins=[-np.inf, p33, p67, np.inf],
        labels=["LOW", "MEDIUM", "HIGH"],
        right=True,
        duplicates="drop",
    ).astype(str)


def detect_outliers(
    df: pd.DataFrame,
    z_threshold_high: float = HIGH_Z,
    z_threshold_med: float = MED_Z,
) -> list[OutlierFlag]:
    """Detect outlier transactions using z-scores within material × country × volume_band peers.

    Args:
        df: Transaction-level DataFrame with 15 source fields.
        z_threshold_high: |z| threshold for HIGH severity (default 2.5).
        z_threshold_med:  |z| threshold for MEDIUM severity (default 1.8).

    Returns:
        List of OutlierFlag, one per (row, metric) pair that breaches a threshold.
        Empty list if no outliers found.
    """
    required = [
        "sold_to", "corporate_group", "country", "pso", "material", "year",
        "sales_qty", "blue_jobber_price", "deductions", "invoice_price",
        "bonuses", "pocket_price", "standard_cost",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[df["sales_qty"] > 0].copy().reset_index(drop=True)
    if df.empty:
        return []

    df = _compute_metric_columns(df)
    df["volume_band"] = assign_volume_bands(df)
    df["peer_group"] = df["material"] + "|" + df["country"] + "|" + df["volume_band"]

    flags: list[OutlierFlag] = []

    for metric, direction in METRICS.items():
        group_stats = (
            df.groupby("peer_group")[metric]
            .agg(["mean", "std", "count"])
            .rename(columns={"mean": "peer_mean", "std": "peer_std", "count": "n"})
        )

        for row_idx, row in df.iterrows():
            pg = row["peer_group"]
            stats = group_stats.loc[pg]

            if stats["n"] < MIN_PEER_SIZE or stats["peer_std"] == 0 or np.isnan(stats["peer_std"]):
                continue

            z = (row[metric] - stats["peer_mean"]) / stats["peer_std"]

            # Direction filter: only flag the meaningful tail
            if direction == "low_is_bad" and z > -z_threshold_med:
                continue
            if direction == "high_is_bad" and z < z_threshold_med:
                continue

            abs_z = abs(z)
            if abs_z >= z_threshold_high:
                severity = "HIGH"
            elif abs_z >= z_threshold_med:
                severity = "MEDIUM"
            else:
                continue

            flags.append(
                OutlierFlag(
                    row_idx=int(row_idx),
                    sold_to=row["sold_to"],
                    corporate_group=row["corporate_group"],
                    country=row["country"],
                    pso=row["pso"],
                    material=row["material"],
                    year=int(row["year"]),
                    sales_qty=float(row["sales_qty"]),
                    volume_band=row["volume_band"],
                    peer_group=pg,
                    metric=metric,
                    value=round(float(row[metric]), 4),
                    peer_mean=round(float(stats["peer_mean"]), 4),
                    peer_std=round(float(stats["peer_std"]), 4),
                    z_score=round(float(z), 4),
                    severity=severity,
                    direction=direction,
                )
            )

    return flags


def summarize_outliers(flags: list[OutlierFlag]) -> pd.DataFrame:
    """Convert a list of OutlierFlag into a summary DataFrame sorted by |z_score| desc."""
    if not flags:
        return pd.DataFrame()

    rows = [
        {
            "sold_to": f.sold_to,
            "corporate_group": f.corporate_group,
            "country": f.country,
            "pso": f.pso,
            "material": f.material,
            "year": f.year,
            "metric": f.metric,
            "value": f.value,
            "peer_mean": f.peer_mean,
            "z_score": f.z_score,
            "severity": f.severity,
            "peer_group": f.peer_group,
        }
        for f in flags
    ]
    return (
        pd.DataFrame(rows)
        .assign(abs_z=lambda d: d["z_score"].abs())
        .sort_values("abs_z", ascending=False)
        .drop(columns="abs_z")
        .reset_index(drop=True)
    )

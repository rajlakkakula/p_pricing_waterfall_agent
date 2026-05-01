"""Generate realistic synthetic data for the waterfall agent (development / demo).

Covers all 15 source fields from PRICING_DB.BRONZE.RAW_TRANSACTIONS plus
the _loaded_at ETL timestamp. Business logic follows CLAUDE.md formulas:

    invoice_price  = blue_jobber_price - deductions
    pocket_price   = invoice_price - bonuses
    (contribution_margin and derived % fields are computed in the Gold layer)

Design decisions for realistic analytics:
  - PSO / country alignment: Americas → USA, Brazil; EMEA → Germany; APAC → China, India
  - Four customer margin archetypes: Premium, Healthy, Low-margin, Destructive
    so that customer_profitability tiers (Tier 1-5) have representation in every PSO
  - ~5% deliberate outlier rows with very high deductions (alert trigger)
  - ~3% rows with near-zero margin (CRITICAL_MARGIN alert trigger)
  - 1% rows with material_cost > standard_cost (data quality flag)
  - Reproducible with a fixed seed (default 42)

Usage:
    python scripts/seed_sample_data.py              # 10 000 rows (default)
    python scripts/seed_sample_data.py --n 50000    # custom count
"""

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# ─── Dimension tables ────────────────────────────────────────────────────────

MATERIALS = [
    {"id": "HYD-001", "category": "Mobile Hydraulics",  "base_price": 145.0},
    {"id": "HYD-002", "category": "Mobile Hydraulics",  "base_price": 220.0},
    {"id": "AIR-042", "category": "Industrial Air",     "base_price":  68.0},
    {"id": "AIR-091", "category": "Industrial Air",     "base_price": 112.0},
    {"id": "PRC-118", "category": "Process Filtration", "base_price": 190.0},
    {"id": "PRC-204", "category": "Process Filtration", "base_price": 340.0},
    {"id": "DST-007", "category": "Dust Collection",    "base_price":  42.0},
    {"id": "OIL-033", "category": "Engine Filtration",  "base_price":  88.0},
]

# PSO → allowed countries (keeps data coherent for GROUP BY in analytics)
PSO_COUNTRIES = {
    "Americas": ["USA", "Brazil"],
    "EMEA":     ["Germany"],
    "APAC":     ["China", "India"],
}

# Country price multiplier (reflects local cost/market differences)
COUNTRY_MULT = {
    "USA":     1.00,
    "Germany": 1.15,
    "Brazil":  0.88,
    "China":   0.75,
    "India":   0.72,
}

SALES_DESIGNATIONS = ["OEM Direct", "Distribution", "Aftermarket", "Service Center"]

YEARS = [2023, 2024, 2025]

# Customer archetypes drive realistic margin distribution across all five tiers.
# margin_archetype controls cost_pct range so Gold-layer tier logic fires correctly:
#   Tier 1 (>35%) → premium; Tier 5 (<5%) → destructive
CUSTOMERS = [
    # ── Americas ──────────────────────────────────────────────────────────────
    {"sold_to": "C-10042", "corp": "Komatsu Group",     "pso": "Americas", "archetype": "premium"},
    {"sold_to": "C-10445", "corp": "Caterpillar Inc",   "pso": "Americas", "archetype": "healthy"},
    {"sold_to": "C-10678", "corp": "Deere & Company",   "pso": "Americas", "archetype": "healthy"},
    {"sold_to": "C-10156", "corp": "Vale S.A.",         "pso": "Americas", "archetype": "low"},
    {"sold_to": "C-10901", "corp": "Freeport Mining",   "pso": "Americas", "archetype": "destructive"},
    # ── EMEA ──────────────────────────────────────────────────────────────────
    {"sold_to": "C-10089", "corp": "Siemens AG",        "pso": "EMEA",     "archetype": "premium"},
    {"sold_to": "C-10512", "corp": "Volvo Group",       "pso": "EMEA",     "archetype": "healthy"},
    {"sold_to": "C-10891", "corp": "Bosch Group",       "pso": "EMEA",     "archetype": "low"},
    {"sold_to": "C-10955", "corp": "ThyssenKrupp AG",   "pso": "EMEA",     "archetype": "destructive"},
    # ── APAC ──────────────────────────────────────────────────────────────────
    {"sold_to": "C-10201", "corp": "SANY Group",        "pso": "APAC",     "archetype": "premium"},
    {"sold_to": "C-10330", "corp": "Tata Group",        "pso": "APAC",     "archetype": "healthy"},
    {"sold_to": "C-10724", "corp": "XCMG Group",        "pso": "APAC",     "archetype": "low"},
    {"sold_to": "C-10820", "corp": "CRRC Corporation",  "pso": "APAC",     "archetype": "destructive"},
]

# Archetype → (cost_pct_min, cost_pct_max) relative to pocket_price.
# These ranges produce margin_pct that land in the correct Tier buckets.
ARCHETYPE_COST_RANGE = {
    "premium":     (0.50, 0.60),   # margin ~40-50% → Tier 1
    "healthy":     (0.65, 0.72),   # margin ~28-35% → Tier 2
    "low":         (0.78, 0.84),   # margin ~16-22% → Tier 3-4
    "destructive": (0.94, 1.02),   # margin < 6%    → Tier 4-5
}


# ─── Transaction generator ────────────────────────────────────────────────────

def _loaded_at_ts(rng: random.Random, year: int) -> str:
    """Return a realistic ETL load timestamp within the given year."""
    start = datetime(year, 1, 1)
    offset_days = rng.randint(0, 364)
    return (start + timedelta(days=offset_days)).strftime("%Y-%m-%d %H:%M:%S")


def generate_transaction(
    country: str,
    year: int,
    material: dict,
    customer: dict,
    rng: random.Random,
    outlier_type: str = "none",
) -> dict:
    """Generate one synthetic transaction row.

    outlier_type controls deliberate anomalies:
      'high_deduction' → deduction_pct > 25% (triggers HIGH_DEDUCTIONS alert)
      'critical_margin' → cost eats almost all pocket price
      'dq_material_cost' → material_cost > standard_cost (data quality flag)
      'none'            → normal row
    """
    mult = COUNTRY_MULT[country]
    base = material["base_price"] * mult

    # Blue/Jobber price: base ± 15%
    blue_price = round(base * (0.85 + rng.random() * 0.30), 4)

    # ── Deductions ───────────────────────────────────────────────────────────
    if outlier_type == "high_deduction":
        ded_pct = 0.25 + rng.random() * 0.10   # 25-35% — triggers alert
    else:
        ded_pct = 0.05 + rng.random() * 0.15   # 5-20% normal range

    deductions = round(blue_price * ded_pct, 4)
    invoice_price = round(blue_price - deductions, 4)

    # ── Bonuses ──────────────────────────────────────────────────────────────
    bon_pct = 0.02 + rng.random() * 0.07   # 2-9%
    bonuses = round(invoice_price * bon_pct, 4)
    pocket_price = round(invoice_price - bonuses, 4)

    # ── Standard cost (archetype-driven) ─────────────────────────────────────
    if outlier_type == "critical_margin":
        cost_pct = 0.93 + rng.random() * 0.08  # cost ≈ 93-101% of pocket → margin < 7%
    else:
        lo, hi = ARCHETYPE_COST_RANGE[customer["archetype"]]
        cost_pct = lo + rng.random() * (hi - lo)

    standard_cost = round(pocket_price * cost_pct, 4)
    # Guard: keep standard_cost positive
    standard_cost = max(standard_cost, 0.01)

    # ── Material cost ─────────────────────────────────────────────────────────
    if outlier_type == "dq_material_cost":
        # Intentional DQ error: material_cost > standard_cost
        material_cost = round(standard_cost * (1.05 + rng.random() * 0.10), 4)
    else:
        mat_pct = 0.55 + rng.random() * 0.20   # 55-75% of standard cost
        material_cost = round(standard_cost * mat_pct, 4)

    qty = round(10 + rng.random() * 490, 4)
    sales_designation = rng.choice(SALES_DESIGNATIONS)

    return {
        "country":           country,
        "year":              year,
        "material":          material["id"],
        "sales_designation": sales_designation,
        "sold_to":           customer["sold_to"],
        "corporate_group":   customer["corp"],
        "pso":               customer["pso"],
        "sales_qty":         qty,
        "blue_jobber_price": blue_price,
        "deductions":        deductions,
        "invoice_price":     invoice_price,
        "bonuses":           bonuses,
        "pocket_price":      pocket_price,
        "standard_cost":     standard_cost,
        "material_cost":     material_cost,
        "_loaded_at":        _loaded_at_ts(rng, year),
    }


# ─── Dataset builder ──────────────────────────────────────────────────────────

def generate_dataset(n_records: int = 10_000, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic transaction dataset aligned with CLAUDE.md schema.

    Outlier mix (approximate):
      ~5%  high-deduction rows  → triggers HIGH_DEDUCTIONS / LOW_REALIZATION alerts
      ~3%  critical-margin rows → triggers CRITICAL_MARGIN alert
      ~1%  DQ material-cost rows → flagged by silver layer as MATERIAL_COST_EXCEEDS_STANDARD
      ~91% normal rows with archetype-driven margin distribution

    Args:
        n_records: Number of rows to generate.
        seed: Random seed for reproducibility.

    Returns:
        DataFrame with 16 columns (15 source fields + _loaded_at).
    """
    rng = random.Random(seed)
    records: list[dict] = []

    # Pre-compute outlier row indices
    n_high_ded = int(n_records * 0.05)
    n_critical  = int(n_records * 0.03)
    n_dq        = int(n_records * 0.01)

    outlier_pool = (
        ["high_deduction"] * n_high_ded
        + ["critical_margin"] * n_critical
        + ["dq_material_cost"] * n_dq
        + ["none"] * (n_records - n_high_ded - n_critical - n_dq)
    )
    rng.shuffle(outlier_pool)

    for i in range(n_records):
        customer = rng.choice(CUSTOMERS)
        country  = rng.choice(PSO_COUNTRIES[customer["pso"]])
        year     = rng.choice(YEARS)
        material = rng.choice(MATERIALS)
        txn = generate_transaction(country, year, material, customer, rng, outlier_pool[i])
        records.append(txn)

    df = pd.DataFrame(records)

    # ── Validation assertions (catch logic bugs before saving) ────────────────
    tol = 0.02  # allow up to 2 cents rounding gap
    inv_check = (df["invoice_price"] - (df["blue_jobber_price"] - df["deductions"])).abs()
    pkt_check = (df["pocket_price"]  - (df["invoice_price"]     - df["bonuses"])).abs()
    assert (inv_check <= tol).all(), f"invoice_price formula violated: max gap={inv_check.max():.4f}"
    assert (pkt_check <= tol).all(), f"pocket_price formula violated:  max gap={pkt_check.max():.4f}"
    assert (df["sales_qty"] > 0).all(),            "sales_qty must be positive"
    assert (df["blue_jobber_price"] > 0).all(),    "blue_jobber_price must be positive"

    # ── Summary stats ─────────────────────────────────────────────────────────
    df["_margin_pct"] = (df["pocket_price"] - df["standard_cost"]) / df["pocket_price"] * 100
    df["_real_pct"]   = df["pocket_price"] / df["blue_jobber_price"] * 100
    df["_ded_pct"]    = df["deductions"]   / df["blue_jobber_price"] * 100

    print(f"\nGenerated {len(df):,} transactions  (seed={seed})")
    print(f"  Avg margin %:       {df['_margin_pct'].mean():6.1f}%  (target: mix of all tiers)")
    print(f"  Avg realization %:  {df['_real_pct'].mean():6.1f}%")
    print(f"  Avg deduction %:    {df['_ded_pct'].mean():6.1f}%")
    print(f"\n  Margin tier distribution (approx):")
    bins   = [-999, 5, 15, 25, 35, 999]
    labels = ["Tier 5 Destructive", "Tier 4 Low", "Tier 3 Acceptable", "Tier 2 Healthy", "Tier 1 Premium"]
    tier_counts = pd.cut(df["_margin_pct"], bins=bins, labels=labels).value_counts().sort_index(ascending=False)
    for tier, cnt in tier_counts.items():
        print(f"    {tier:<25} {cnt:>6,} rows  ({cnt/len(df)*100:.1f}%)")

    dq_rows = (df["material_cost"] > df["standard_cost"]).sum()
    print(f"\n  DQ rows (mat_cost > std_cost): {dq_rows:,} ({dq_rows/len(df)*100:.1f}%)")
    print(f"  High-deduction rows (>25%):    {(df['_ded_pct'] > 25).sum():,}")
    print(f"  Critical-margin rows (<10%):   {(df['_margin_pct'] < 10).sum():,}")

    # Drop internal helper columns before saving
    df = df.drop(columns=["_margin_pct", "_real_pct", "_ded_pct"])

    return df


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic waterfall transactions")
    parser.add_argument("--n",    type=int, default=10_000, help="Number of rows (default: 10 000)")
    parser.add_argument("--seed", type=int, default=42,     help="Random seed (default: 42)")
    parser.add_argument("--out",  type=str, default="tests/fixtures/sample_transactions.csv",
                        help="Output CSV path")
    args = parser.parse_args()

    df = generate_dataset(n_records=args.n, seed=args.seed)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"\nSaved → {out}  ({out.stat().st_size / 1024:.0f} KB)")
    print("\nSample row:")
    print(df.iloc[0].to_dict())

#!/usr/bin/env python3
"""End-to-end agent test runner — Snowflake or fixture CSV.

Usage:
    uv run python scripts/run_agent.py                      # 4 canned queries
    uv run python scripts/run_agent.py "Show EMEA margin"   # custom query
    uv run python scripts/run_agent.py --csv                # force fixture CSV

Tries Snowflake (BRONZE layer) first; falls back to fixture CSV automatically.
All queries run the full pipeline: parse_intent → run_analysis → print result.
Narrative generation (Claude API) only fires on 'full_analysis' or 'narrative' actions.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ── Data loading ───────────────────────────────────────────────────────────────

def _load_from_snowflake() -> pd.DataFrame:
    from src.snowflake.connection import get_snowflake_manager

    mgr = get_snowflake_manager()
    df = mgr.execute_query(
        "SELECT * FROM PRICING_DB.BRONZE.RAW_TRANSACTIONS LIMIT 100000"
    )
    if df.empty:
        raise ValueError("BRONZE table returned 0 rows — has data been loaded?")
    return df


def _load_fixture_csv() -> pd.DataFrame:
    csv_path = ROOT / "tests" / "fixtures" / "sample_transactions.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Fixture CSV not found at {csv_path}. "
            "Run: uv run python scripts/seed_sample_data.py"
        )
    return pd.read_csv(csv_path)


def load_data(force_csv: bool = False) -> tuple[pd.DataFrame, str]:
    if not force_csv:
        try:
            df = _load_from_snowflake()
            return df, "Snowflake BRONZE"
        except Exception as exc:
            print(f"  [Snowflake unavailable: {exc}]")
            print("  Falling back to fixture CSV...")

    df = _load_fixture_csv()
    return df, "fixture CSV"


# ── Result display ─────────────────────────────────────────────────────────────

_DIVIDER = "─" * 62


def _print_waterfall(wf) -> None:
    print(f"\n  WATERFALL")
    print(f"    Blue price       {wf.blue_price:>10.2f}")
    print(f"    Deductions       {wf.deductions:>10.2f}  ({wf.deduction_pct:.1f}%)")
    print(f"    Invoice price    {wf.invoice_price:>10.2f}")
    print(f"    Bonuses          {wf.bonuses:>10.2f}  ({wf.bonus_pct:.1f}%)")
    print(f"    Pocket price     {wf.pocket_price:>10.2f}")
    print(f"    Standard cost    {wf.standard_cost:>10.2f}")
    print(f"    Contribution     {wf.contribution_margin:>10.2f}  (margin {wf.margin_pct:.1f}%)")
    print(f"    Leakage          {wf.leakage_pct:.1f}%  |  Realization {wf.realization_pct:.1f}%")
    print(f"    Volume           {wf.total_qty:>10,} units  |  {wf.transaction_count:,} transactions")
    print(f"    Pocket revenue   ${wf.total_pocket_revenue:>12,.0f}")
    print(f"    Margin dollars   ${wf.total_margin_dollars:>12,.0f}")


def _print_outliers(outliers: list) -> None:
    if not outliers:
        print("\n  OUTLIERS  (none detected in this segment)")
        return

    top = sorted(outliers, key=lambda f: abs(f.z_score), reverse=True)[:5]
    print(f"\n  OUTLIERS  ({len(outliers)} total — showing top {len(top)} by |z|)")
    for f in top:
        print(
            f"    [{f.severity:<6}] {f.sold_to:<12}  {f.metric:<18}"
            f"  value={f.value:.1f}%  mean={f.peer_mean:.1f}%  z={f.z_score:+.2f}"
        )


def _print_bridge(b) -> None:
    print(f"\n  MARGIN BRIDGE  {b.base_year} → {b.current_year}")
    print(f"    Total change     ${b.total_margin_change:>+12,.0f}")
    print(f"      Price effect   ${b.price_effect:>+12,.0f}")
    print(f"      Deduct effect  ${b.deduction_effect:>+12,.0f}")
    print(f"      Bonus effect   ${b.bonus_effect:>+12,.0f}")
    print(f"      Cost effect    ${b.cost_effect:>+12,.0f}")
    print(f"      Volume effect  ${b.volume_effect:>+12,.0f}")
    print(f"      Mix effect     ${b.mix_effect:>+12,.0f}")
    print(f"    Base margin      {b.base.wavg_margin_pct:.1f}%  →  Current {b.current.wavg_margin_pct:.1f}%")


def _print_narrative(text: str) -> None:
    print(f"\n  NARRATIVE")
    for para in text.strip().split("\n\n"):
        print()
        for line in para.strip().splitlines():
            print(f"    {line}")


# ── Single query runner ────────────────────────────────────────────────────────

def run_query(df: pd.DataFrame, query: str) -> None:
    from src.agent.intent_parser import parse_intent
    from src.agent.orchestrator import run_analysis

    print(f"\n{_DIVIDER}")
    print(f"  QUERY  {query}")
    print(_DIVIDER)

    print("  Parsing intent via Claude...", end=" ", flush=True)
    t0 = time.perf_counter()
    try:
        intent = parse_intent(query)
    except Exception as exc:
        print(f"FAILED ({exc})")
        return
    t_parse = time.perf_counter() - t0

    filter_parts = {
        k: v for k, v in vars(intent.filters).items() if v is not None
    }
    print(f"action={intent.action}", end="")
    if filter_parts:
        print(f"  filters={filter_parts}", end="")
    if intent.base_year:
        print(f"  years={intent.base_year}→{intent.current_year}", end="")
    print(f"  [{t_parse:.2f}s]")

    print("  Running analysis...", end=" ", flush=True)
    t1 = time.perf_counter()
    result = run_analysis(df, intent)
    t_analysis = time.perf_counter() - t1
    t_total = time.perf_counter() - t0
    print(f"done.  [{t_analysis:.2f}s analytics  |  {t_total:.2f}s total]")

    if result.error:
        print(f"\n  ERROR: {result.error}")
        return

    if result.waterfall:
        _print_waterfall(result.waterfall)

    if result.outliers is not None:
        _print_outliers(result.outliers)

    if result.bridge:
        _print_bridge(result.bridge)

    if result.narrative:
        _print_narrative(result.narrative)


# ── Entry point ────────────────────────────────────────────────────────────────

SAMPLE_QUERIES = [
    "Show me the overall pricing waterfall for all data",
    "How did margin change year over year from 2024 to 2025?",
    "Which customers have unusually high deductions or low margins?",
    "Give me a full analysis of the Americas PSO",
]


def main() -> None:
    args = sys.argv[1:]
    force_csv = "--csv" in args
    queries = [a for a in args if not a.startswith("--")] or SAMPLE_QUERIES

    print("Loading transaction data...")
    df, source = load_data(force_csv)
    print(f"Loaded {len(df):,} rows from {source}.\n")

    for query in queries:
        run_query(df, query)

    print(f"\n{_DIVIDER}")
    print("  Done.")
    print(_DIVIDER)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Interactive terminal chatbot — SQL-based Q&A for the pricing waterfall agent.

The agent converts every natural language question into SQL, executes it
against the pricing database, and interprets the results in plain English.
No fixed analytics pipeline — any question the data can answer, it will answer.

Run:
    uv run python scripts/chat.py          # Snowflake GOLD layer (CSV fallback)
    uv run python scripts/chat.py --csv    # force offline CSV via DuckDB
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from textwrap import fill

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ── ANSI colours ───────────────────────────────────────────────────────────────

RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
CYAN    = "\033[36m"
GREEN   = "\033[32m"
YELLOW  = "\033[33m"
RED     = "\033[31m"
BLUE    = "\033[34m"
MAGENTA = "\033[35m"


def _c(text: str, *codes: str) -> str:
    return "".join(codes) + str(text) + RESET


# ── Data loading ───────────────────────────────────────────────────────────────

def load_data(force_csv: bool = False):
    """Load transaction data and return (df_or_None, source_label, mgr_or_None).

    Returns:
        df:     Pandas DataFrame (always populated — from Snowflake or CSV).
        source: Human-readable data source label.
        mgr:    SnowflakeConnectionManager if Snowflake is live, else None.
                When mgr is not None the SQL agent queries Snowflake GOLD directly;
                otherwise DuckDB queries the in-memory DataFrame.
    """
    import pandas as pd

    if not force_csv:
        try:
            from src.snowflake.connection import get_snowflake_manager
            print(_c("  Connecting to Snowflake...", DIM), end=" ", flush=True)
            mgr = get_snowflake_manager()
            # Load a sample to confirm connectivity; agent will query GOLD via mgr
            df = mgr.execute_query(
                "SELECT * FROM PRICING_DB.BRONZE.RAW_TRANSACTIONS LIMIT 1"
            )
            if df.empty:
                raise ValueError("Bronze table is empty.")
            print(_c("✓  Connected to Snowflake — agent will query GOLD layer", GREEN))
            return None, "Snowflake GOLD", mgr
        except Exception as exc:
            print(_c(f"✗  {exc}", RED))
            print(_c("  Falling back to fixture CSV via DuckDB...", DIM))

    csv_path = ROOT / "tests" / "fixtures" / "sample_transactions.csv"
    df = pd.read_csv(csv_path)
    print(_c(f"✓  {len(df):,} rows loaded from fixture CSV (DuckDB offline mode)", GREEN))
    return df, "CSV (DuckDB)", None


# ── Rendering ──────────────────────────────────────────────────────────────────

W = 72  # display width


def _line(char: str = "─") -> str:
    return _c(char * W, DIM)


def _section(title: str) -> None:
    print(f"\n  {_c(title, BOLD, CYAN)}")
    print(f"  {_c('─' * (len(title) + 2), DIM)}")


def render_sql(sql_calls: list[str]) -> None:
    for i, sql in enumerate(sql_calls, 1):
        label = f"SQL {i}" if len(sql_calls) > 1 else "SQL"
        _section(label)
        for line in sql.strip().splitlines()[:12]:
            print(f"    {_c(line, DIM, CYAN)}")
        if sql.strip().count("\n") >= 12:
            extra = sql.strip().count("\n") - 11
            print(f"    {_c(f'… ({extra} more lines)', DIM)}")


def render_answer(text: str) -> None:
    _section("ANSWER")
    for para in text.strip().split("\n\n"):
        print()
        for line in fill(para.strip(), width=W - 4).splitlines():
            print(f"    {line}")


# ── SQL agent runner ───────────────────────────────────────────────────────────

def run_query(agent, question: str) -> None:
    from src.agent.sql_agent import SqlAgent  # type hint only

    t0 = time.perf_counter()
    print(f"\n  {_c('Querying...', DIM)}", end=" ", flush=True)

    try:
        result = agent.ask(question)
    except Exception as exc:
        print(_c(f"failed — {exc}", RED))
        return

    t_total = time.perf_counter() - t0
    n_sql = len(result.sql_calls)
    status = _c(
        f"done  [{n_sql} SQL quer{'y' if n_sql == 1 else 'ies'}  |  {t_total:.1f}s]",
        DIM,
    )
    print(status)

    if result.error:
        print(f"\n  {_c('Warning:', YELLOW, BOLD)} {result.error}")

    if result.sql_calls:
        render_sql(result.sql_calls)

    render_answer(result.answer)


# ── Help / banner ──────────────────────────────────────────────────────────────

EXAMPLES = [
    "Show me the overall pricing waterfall",
    "What is the volume-weighted margin % for each country?",
    "Which customers in EMEA have margin below 15%?",
    "How did margin change from 2024 to 2025?",
    "Which material has the highest deduction rate?",
    "Show me the top 5 customers by total pocket revenue",
    "Are there any active pricing alerts?",
    "Compare realization % across PSOs for 2025",
    "Which corporate groups are in Tier 4 or Tier 5?",
    "Full analysis of the Americas PSO",
]


def print_banner(source: str) -> None:
    print()
    print(_c("┌" + "─" * (W - 2) + "┐", DIM))
    print(_c("│", DIM) + _c("  Pricing Waterfall SQL Agent  ".center(W - 2), BOLD, CYAN) + _c("│", DIM))
    print(_c("│", DIM) + _c(f"  Data source: {source}".center(W - 2), DIM) + _c("│", DIM))
    print(_c("└" + "─" * (W - 2) + "┘", DIM))
    print()
    print(f"  {_c('Ask any pricing question in plain English. Commands:', DIM)}")
    print(f"  {_c('  help', CYAN)}  — show example questions")
    print(f"  {_c('  quit', CYAN)}  — exit  (or Ctrl+C)")
    print()


def print_help() -> None:
    print()
    print(f"  {_c('Example questions:', BOLD)}")
    for q in EXAMPLES:
        print(f"    {q}")
    print()


# ── Main loop ──────────────────────────────────────────────────────────────────

def main() -> None:
    force_csv = "--csv" in sys.argv

    print(f"\n  {_c('Loading data...', DIM)}")
    try:
        df, source, mgr = load_data(force_csv)
    except Exception as exc:
        print(_c(f"  Fatal: {exc}", RED))
        sys.exit(1)

    # Initialise the SQL agent once — reused for every question in the session
    from src.agent.sql_agent import SqlAgent

    if mgr is not None:
        agent = SqlAgent(snowflake_mgr=mgr)
    else:
        agent = SqlAgent(df=df)

    print_banner(source)

    while True:
        try:
            raw = input(_c("You: ", BOLD, CYAN)).strip()
        except (EOFError, KeyboardInterrupt):
            print(f"\n  {_c('Goodbye.', DIM)}")
            break

        if not raw:
            continue
        if raw.lower() in ("quit", "exit", "q", ":q"):
            print(f"  {_c('Goodbye.', DIM)}")
            break
        if raw.lower() in ("help", "?", "h"):
            print_help()
            continue

        print(_line())
        run_query(agent, raw)
        print(f"\n{_line()}")


if __name__ == "__main__":
    main()

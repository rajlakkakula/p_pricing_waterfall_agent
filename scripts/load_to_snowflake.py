"""Load the synthetic CSV into Snowflake PRICING_DB.BRONZE.RAW_TRANSACTIONS.

Uses snowflake-connector-python write_pandas() — no SnowSQL required.
Reads credentials from .env (same variables as src/snowflake/connection.py).

Pre-requisite: run scripts/setup_snowflake.sql first to create the table.

Usage:
    python scripts/load_to_snowflake.py
    python scripts/load_to_snowflake.py --csv tests/fixtures/sample_transactions.csv
    python scripts/load_to_snowflake.py --truncate   # wipe table before loading
"""

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_env(key: str, default: str | None = None) -> str:
    val = os.getenv(key, default)
    if val is None:
        print(f"ERROR: missing environment variable {key}. Add it to .env")
        sys.exit(1)
    return val


def load_csv(csv_path: Path) -> pd.DataFrame:
    print(f"Reading {csv_path} …")
    df = pd.read_csv(csv_path)

    # Enforce column dtypes to match the Snowflake DDL
    df["year"]     = df["year"].astype(int)
    df["sales_qty"]          = df["sales_qty"].astype(float)
    df["blue_jobber_price"]  = df["blue_jobber_price"].astype(float)
    df["deductions"]         = df["deductions"].astype(float)
    df["invoice_price"]      = df["invoice_price"].astype(float)
    df["bonuses"]            = df["bonuses"].astype(float)
    df["pocket_price"]       = df["pocket_price"].astype(float)
    df["standard_cost"]      = df["standard_cost"].astype(float)
    df["material_cost"]      = df["material_cost"].astype(float)
    df["_loaded_at"]         = pd.to_datetime(df["_loaded_at"])

    # Snowflake write_pandas expects UPPER-CASE column names to match DDL
    df.columns = [c.upper() for c in df.columns]

    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")
    return df


def main(csv_path: Path, truncate: bool) -> None:
    # Lazy import — only needed at load time, not in the API/agent
    from snowflake.connector import connect
    from snowflake.connector.pandas_tools import write_pandas

    account  = get_env("SNOWFLAKE_ACCOUNT")
    user     = get_env("SNOWFLAKE_USER")
    password = get_env("SNOWFLAKE_PASSWORD")
    warehouse = get_env("SNOWFLAKE_WAREHOUSE", "PRICING_WH")
    role     = get_env("SNOWFLAKE_ROLE", "SYSADMIN")

    print(f"\nConnecting to Snowflake account: {account}")
    conn = connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database="PRICING_DB",
        schema="BRONZE",
        role=role,
    )

    try:
        if truncate:
            print("Truncating RAW_TRANSACTIONS …")
            conn.cursor().execute("TRUNCATE TABLE PRICING_DB.BRONZE.RAW_TRANSACTIONS")

        df = load_csv(csv_path)

        print("\nUploading via write_pandas (internal stage) …")
        t0 = time.time()
        success, n_chunks, n_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="RAW_TRANSACTIONS",
            database="PRICING_DB",
            schema="BRONZE",
            chunk_size=5_000,
            auto_create_table=False,  # table already created by setup_snowflake.sql
            quote_identifiers=False,
        )
        elapsed = time.time() - t0

        if success:
            print(f"\nLoad complete in {elapsed:.1f}s")
            print(f"  Chunks uploaded : {n_chunks}")
            print(f"  Rows loaded     : {n_rows:,}")
        else:
            print("Load FAILED. Output:")
            print(output)
            sys.exit(1)

        # Quick row-count verification
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM PRICING_DB.BRONZE.RAW_TRANSACTIONS")
        total = cur.fetchone()[0]
        print(f"  Table row count : {total:,}  ✓")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load synthetic CSV into Snowflake")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("tests/fixtures/sample_transactions.csv"),
        help="Path to the CSV file (default: tests/fixtures/sample_transactions.csv)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate RAW_TRANSACTIONS before loading (idempotent re-run)",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"CSV not found: {args.csv}")
        print("Run:  python scripts/seed_sample_data.py")
        sys.exit(1)

    main(args.csv, args.truncate)

"""Try common Snowflake account identifier formats until one connects.

For a personal Standard Edition account the Python connector needs a slightly
different format than what appears in the Snowsight URL bar.

Run:  uv run python scripts/find_account_id.py
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

load_dotenv(Path(__file__).parent.parent / ".env")

user     = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
raw      = os.environ["SNOWFLAKE_ACCOUNT"]   # e.g. "AAC53253.us-east-1"

base = raw.split(".")[0]   # e.g. "AAC53253" or "myorg-myaccount"

# Candidates in order of likelihood.
# Modern Snowflake accounts use "orgname-accountname" format visible in the
# Snowsight browser URL:  https://app.snowflake.com/<region>/<identifier>/
candidates = [
    raw,                             # exactly as in .env
    raw.lower(),
    base,                            # strip region suffix
    base.lower(),
    f"{base}.us-east-1",
    f"{base.lower()}.us-east-1",
    f"{base}.us-east-1.aws",
    f"{base.lower()}.us-east-1.aws",
]

print(f"Testing {len(candidates)} account identifier formats for user={user}\n")

for candidate in candidates:
    try:
        conn = snowflake.connector.connect(
            account=candidate, user=user, password=password,
            login_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER()")
        acct, region, usr = cur.fetchone()
        conn.close()
        print(f"SUCCESS → account identifier: '{candidate}'")
        print(f"  CURRENT_ACCOUNT() = {acct}")
        print(f"  CURRENT_REGION()  = {region}")
        print(f"  CURRENT_USER()    = {usr}")
        print(f"\nUpdate your .env:\n  SNOWFLAKE_ACCOUNT={candidate}")
        break
    except Exception as e:
        msg = str(e)[:120]
        print(f"  FAIL  '{candidate}' → {msg}")
else:
    print("\nAll formats failed.")
    print("\nTo find the exact identifier, run this SQL in your Snowsight worksheet:")
    print("  SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_ORGANIZATION_NAME();")
    print("\nOR look at your browser URL when logged into Snowsight:")
    print("  https://app.snowflake.com/<region>/<YOUR_IDENTIFIER>/")
    print("\nThen update .env:  SNOWFLAKE_ACCOUNT=<YOUR_IDENTIFIER>")
    print("and re-run:  uv run python scripts/find_account_id.py")

"""Execute setup_snowflake.sql against the Snowflake account in .env."""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

load_dotenv(Path(__file__).parent.parent / ".env")

account  = os.environ["SNOWFLAKE_ACCOUNT"]
user     = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
role     = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

print(f"Connecting → account={account}  user={user}  role={role}")

conn = snowflake.connector.connect(
    account=account, user=user, password=password, role=role,
    login_timeout=30,
)
cur = conn.cursor()

sql = (Path(__file__).parent / "setup_snowflake.sql").read_text()

errors = []
for raw in sql.split(";"):
    lines = [l for l in raw.splitlines() if l.strip() and not l.strip().startswith("--")]
    stmt = "\n".join(lines).strip()
    if not stmt or stmt.upper().startswith("SHOW"):
        continue
    try:
        cur.execute(stmt)
        print(f"  OK  {stmt.splitlines()[0][:80]}")
    except Exception as e:
        msg = str(e)
        if "already exists" in msg.lower():
            print(f"  --  (already exists) {stmt.splitlines()[0][:60]}")
        else:
            print(f"  ERR {stmt.splitlines()[0][:60]}\n      {msg}")
            errors.append(msg)

conn.close()

if errors:
    print(f"\n{len(errors)} error(s) above.")
    sys.exit(1)
print("\nSetup complete.")

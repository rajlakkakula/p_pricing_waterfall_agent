# Pricing Waterfall Agent — Local Setup & Run Guide

AI-powered pricing analytics for the filtration industry. Decomposes the full price journey — Blue/Jobber Price → Deductions → Invoice Price → Bonuses → Pocket Price → Standard Cost → Contribution Margin — with natural language diagnostics via Claude AI.

---

## Build Status

| Layer | Component | Status |
|-------|-----------|--------|
| Data | Synthetic data generator (`scripts/seed_sample_data.py`) | Done |
| Data | Snowflake DDL (`scripts/setup_snowflake.sql`) | Done |
| Data | CSV → Snowflake loader (`scripts/load_to_snowflake.py`) | Done |
| dbt | Bronze passthrough | Done |
| dbt | Silver (dedup + validation) | Done |
| dbt | Gold (waterfall_fact, customer_profitability, alerts) | Done |
| Python | Snowflake connection manager | Done |
| Python | Waterfall computation engine | Done |
| Python | Outlier detection (z-score, peer groups) | Done |
| Python | YoY margin bridge decomposition | Done |
| Python | AI narrative generation (Claude API) | Done |
| Python | Agent layer (intent parser, orchestrator, formatter) | Done |
| Python | FastAPI app + routes | Done |
| Python | Unit + API tests (116 tests) | Done |
| Frontend | React + Vite + Recharts + Tailwind | Done |

---

## Prerequisites

Install these before starting:

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.12+ | [python.org](https://python.org) |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Node.js | 18+ | [nodejs.org](https://nodejs.org) |
| npm | 9+ | Bundled with Node.js |
| Snowflake account | any tier | Free trial at snowflake.com *(optional — CSV fallback works offline)* |
| Anthropic API key | — | [console.anthropic.com](https://console.anthropic.com) |

---

## One-Time Setup

### 1. Install Python dependencies

```bash
cd p_pricing_waterfall_agent
uv sync
```

### 2. Install frontend dependencies

```bash
cd frontend
npm install
cd ..
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your credentials:

```env
# Snowflake (required for live data; skip if using CSV fallback)
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=PRICING_WH
SNOWFLAKE_DATABASE=PRICING_DB
SNOWFLAKE_SCHEMA=GOLD
SNOWFLAKE_ROLE=SYSADMIN

# Anthropic (required for AI chat)
ANTHROPIC_API_KEY=sk-ant-...
```

> **Finding your Snowflake account identifier:**
> ```bash
> uv run python scripts/find_account_id.py
> ```
> It looks like `abc12345.us-east-1` or `orgname-accountname`.

---

## Data Pipeline (First Time Only)

Skip this section if you only want the offline CSV mode — the fixture file (`tests/fixtures/sample_transactions.csv`) is already committed.

### Step 1 — Generate synthetic transactions

```bash
uv run python scripts/seed_sample_data.py         # 10,000 rows, seed=42
uv run python scripts/seed_sample_data.py --n 50000  # larger dataset
```

Output: `tests/fixtures/sample_transactions.csv`

### Step 2 — Provision Snowflake

Run `scripts/setup_snowflake.sql` in Snowsight (Snowflake Web UI). This creates:
- Warehouse: `PRICING_WH` (XS, auto-suspend 60s)
- Database: `PRICING_DB`
- Schemas: `BRONZE`, `SILVER`, `GOLD`
- Table: `PRICING_DB.BRONZE.RAW_TRANSACTIONS`

### Step 3 — Load CSV into Snowflake

```bash
uv run python scripts/load_to_snowflake.py
uv run python scripts/load_to_snowflake.py --truncate   # wipe and reload
```

### Step 4 — Run dbt transforms

dbt is managed through `uv` and does not load `.env` automatically. Always run it with `uv run` after sourcing the environment:

```bash
cd dbt

# 1. Source credentials into the shell (dbt reads them as env vars)
set -a && source ../.env && set +a

# 2. Verify the connection before running models
uv run dbt debug

# 3. Build all models: Bronze → Silver → Gold
uv run dbt run

# 4. Validate the Gold layer schema tests
uv run dbt test

cd ..
```

Expected output from `dbt run` (all 5 models should pass):

```
1 of 5 OK  sql view model  BRONZE.src_raw_transactions
2 of 5 OK  sql table model SILVER.stg_clean_transactions
3 of 5 OK  sql table model GOLD.waterfall_fact
4 of 5 OK  sql table model GOLD.customer_profitability
5 of 5 OK  sql table model GOLD.waterfall_alerts
Completed successfully — PASS=5 WARN=0 ERROR=0
```

> **Re-running dbt:** Models are idempotent — re-run `uv run dbt run` at any time to refresh the Gold tables after loading new data.

---

## Running the App in the Browser

You need two terminals running simultaneously — the FastAPI backend and the Vite frontend dev server.

### Terminal 1 — Start the FastAPI backend

```bash
cd p_pricing_waterfall_agent
uv run uvicorn src.api.main:app --reload --port 8000
```

Expected output:

```
INFO:     Started server process [...]
INFO:     Waiting for application startup.
INFO:     Data loaded: 10000 rows from Snowflake BRONZE   ← live Snowflake
  — or —
INFO:     Data loaded: 10000 rows from CSV fallback       ← offline mode
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000
```

> **Snowflake vs. CSV fallback:** The backend tries Snowflake first. If the connection fails (wrong credentials, no internet, Snowflake suspended), it automatically falls back to `tests/fixtures/sample_transactions.csv`. All features work in both modes.

Verify the backend is healthy:

```bash
curl http://localhost:8000/api/health
# → {"status":"ok","data_source":"Snowflake BRONZE","row_count":10000}
```

### Terminal 2 — Start the Vite frontend

```bash
cd p_pricing_waterfall_agent/frontend
npm run dev
```

Expected output:

```
  VITE v5.x.x  ready in 200 ms

  ➜  Local:   http://localhost:5173/
  ➜  Network: use --host to expose
```

### Open the app

Navigate to **[http://localhost:5173](http://localhost:5173)** in your browser.

You should see:
- **Left panel** — Price Waterfall chart, Year-over-Year Margin Bridge, Outlier Customer table
- **Right panel** — AI Pricing Agent chat

---

## Using the App

### Dashboard filters (left panel)

| Filter | Options |
|--------|---------|
| Country | USA, Germany, Brazil, China, India |
| PSO | Americas, EMEA, APAC |
| Year | 2024, 2025 |
| Material | HYD-001, IND-AIR-001, PROC-001, DUST-001 |

Select any combination and click **Apply**. Click **Reset** to return to the full dataset.

### AI chat (right panel)

Type a natural language question or click one of the suggestion chips:

```
Show me the overall pricing waterfall
Which customers have unusually high deductions?
How did margin change from 2024 to 2025?
Full analysis of the Americas PSO
```

The browser agent uses a structured pipeline (intent → waterfall/outliers/trends) and returns typed data for the charts. Waterfall data from chat responses also updates the left dashboard panel.

---

## Terminal Chat (SQL Agent)

An alternative to the browser UI — run directly in the terminal. Every question is answered by generating and running SQL, then interpreting the results in plain English.

```bash
# With Snowflake (queries GOLD layer directly)
uv run python scripts/chat.py

# Offline mode (no Snowflake needed — uses DuckDB + CSV fixture)
uv run python scripts/chat.py --csv
```

Example questions you can ask:

```
What is the volume-weighted margin % for each country?
Which customers in EMEA have margin below 15%?
How did margin change from 2024 to 2025?
Which material has the highest deduction rate?
Show me the top 5 customers by total pocket revenue
Are there any active pricing alerts?
Compare realization % across PSOs for 2025
Which corporate groups are in Tier 4 or Tier 5?
```

The agent shows the SQL it generated and executed, then gives a business-focused interpretation of the results.

**How it works (agentic loop):**
1. You ask a question in natural language
2. Claude calls `list_tables` → `describe_table` → `run_sql` (as many times as needed)
3. SQL executes against DuckDB (offline) or Snowflake GOLD (live)
4. Claude interprets the result rows and responds in plain English

---

## Available API Endpoints

The FastAPI backend exposes these routes directly if you want to call them from curl or Postman:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Liveness check + data source info |
| POST | `/api/chat` | Natural language query → full analysis |
| GET | `/api/waterfall` | Waterfall metrics (filter params optional) |
| GET | `/api/outliers` | Outlier flags (filter params optional) |
| GET | `/api/trends` | YoY margin bridge (requires `base_year` + `current_year`) |

Filter query parameters for `/api/waterfall` and `/api/outliers`:
`?country=USA&pso=Americas&year=2024&material=HYD-001`

Example:

```bash
curl "http://localhost:8000/api/waterfall?country=USA&year=2025"
curl "http://localhost:8000/api/outliers?pso=Americas"
curl "http://localhost:8000/api/trends?base_year=2024&current_year=2025"
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Which customers in EMEA have margin below 15%?"}'
```

---

## Running Tests

```bash
uv run pytest                          # all 116 tests
uv run pytest tests/test_waterfall.py -v
uv run pytest tests/test_api.py -v
uv run pytest --cov=src/analytics      # coverage report
```

All tests run offline — no Snowflake or Anthropic API calls required (mocked).

---

## Troubleshooting

### `zsh: command not found: dbt`
`dbt` is not a global CLI — it lives inside the project's uv-managed virtual environment. Never run `dbt` directly. Always prefix with `uv run`:
```bash
uv run dbt run
uv run dbt test
uv run dbt debug
```
If `uv run dbt` also fails, the package is not installed. Fix it with:
```bash
uv add dbt-snowflake
```

### `dbt` error — `Env var required but not provided: 'SNOWFLAKE_ACCOUNT'`
dbt reads credentials from environment variables, not from `.env` automatically. Source the file before running dbt:
```bash
set -a && source .env && set +a
uv run dbt run
```

### `dbt` error — `Object 'PRICING_DB.BRONZE.RAW_TRANSACTIONS' does not exist`
The Bronze source table is missing — likely dropped by an earlier failed dbt run that attempted `CREATE OR REPLACE VIEW raw_transactions` (which replaces the same-named table). Recreate it and reload:
```bash
# 1. Re-provision the table (run in Snowsight or via Python)
#    Copy the CREATE TABLE block from scripts/setup_snowflake.sql

# 2. Reload the data
uv run python scripts/load_to_snowflake.py

# 3. Re-run dbt
cd dbt && set -a && source ../.env && set +a && uv run dbt run
```

### Backend won't start — `Extra inputs are not permitted`
Your `.env` has a key that Pydantic doesn't recognize. The settings classes use `extra="ignore"` so this should not occur in the current version. If it does, check that you're running `uv run` (not a global Python) so the project's installed version of pydantic-settings is used.

### Browser shows 500 for all `/api/*` calls
The Vite proxy is not reaching the backend. Confirm:
1. The backend is running on port 8000 (`curl http://localhost:8000/api/health` returns 200)
2. `frontend/vite.config.js` has `target: "http://127.0.0.1:8000"` (not `localhost` — Node.js may resolve `localhost` to `::1` IPv6 which uvicorn doesn't bind)

### Snowflake connection error — `[Errno 84] Value too large`
This was a known issue with `cursor.fetchall()` on DECIMAL columns. It is fixed in the current `src/snowflake/connection.py` (uses `cursor.fetch_pandas_all()` / Arrow-based fetch). Run `uv sync` to ensure you have the latest dependencies.

### Chat returns no narrative / very slow
The Anthropic API key in `.env` may be missing or invalid. Check:
```bash
curl https://api.anthropic.com/v1/models \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01"
```
A 200 response confirms the key is valid.

### `npm install` fails with EACCES
```bash
npm install --cache /tmp/npm-cache
```

---

## Project Structure

```
p_pricing_waterfall_agent/
├── READ.md                          # This file
├── CLAUDE.md                        # AI assistant context (domain glossary, build log)
├── pyproject.toml                   # Python dependencies (uv / hatchling)
├── .env                             # Credentials (gitignored)
├── .env.example                     # Credential template
│
├── scripts/
│   ├── seed_sample_data.py          # Generates tests/fixtures/sample_transactions.csv
│   ├── setup_snowflake.sql          # One-time DDL (warehouse, schemas, tables)
│   ├── load_to_snowflake.py         # CSV → Snowflake Bronze loader
│   ├── find_account_id.py           # Discovers Snowflake account identifier
│   └── chat.py                      # SQL-agent REPL — any NL question → SQL → answer
│
├── src/
│   ├── snowflake/
│   │   ├── connection.py            # Singleton connection manager (Arrow-based fetch)
│   │   └── queries.py               # SQL-injection-safe parameterized query templates
│   ├── analytics/
│   │   ├── waterfall.py             # Volume-weighted waterfall computation
│   │   ├── outliers.py              # Z-score peer-group outlier detection
│   │   ├── trends.py                # YoY margin bridge decomposition
│   │   └── narratives.py            # Claude AI narrative generation
│   ├── agent/
│   │   ├── sql_agent.py             # Agentic SQL loop (list_tables/describe_table/run_sql tools)
│   │   ├── intent_parser.py         # NL → structured intent via Claude tool use (API/browser)
│   │   ├── orchestrator.py          # Routes intent to analytics pipeline (API/browser)
│   │   └── response_formatter.py   # Serializes results to JSON (API/browser)
│   └── api/
│       ├── main.py                  # FastAPI app (lifespan, CORS, error handler)
│       ├── routes.py                # /health /chat /waterfall /outliers /trends
│       └── models.py                # Pydantic request/response schemas
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── bronze/src_raw_transactions.sql
│       ├── silver/stg_clean_transactions.sql
│       └── gold/
│           ├── waterfall_fact.sql
│           ├── customer_profitability.sql
│           ├── waterfall_alerts.sql
│           └── schema.yml
│
├── frontend/
│   ├── package.json
│   ├── vite.config.js               # Proxy: /api → http://127.0.0.1:8000
│   ├── tailwind.config.js
│   └── src/
│       ├── App.jsx                  # Split-pane layout, filter state, data loading
│       ├── api.js                   # fetch wrappers for all backend endpoints
│       ├── index.css                # Tailwind base + custom scrollbar
│       └── components/
│           ├── FilterBar.jsx        # Country / PSO / Year / Material dropdowns
│           ├── WaterfallChart.jsx   # Recharts floating-bar waterfall + metric pills
│           ├── CustomerTable.jsx    # Outlier table, severity badges, show-all toggle
│           └── AgentChat.jsx        # NL chat, action badges, metric grid, bridge summary
│
└── tests/
    ├── test_waterfall.py            # 8 known-answer unit tests
    ├── test_outliers.py             # 12 outlier detection tests
    ├── test_trends.py               # 13 margin bridge tests
    ├── test_narratives.py           # 12 narrative generation tests (mocked API)
    ├── test_intent_parser.py        # 14 intent parsing tests
    ├── test_orchestrator.py         # 14 orchestrator routing tests
    ├── test_response_formatter.py   # 12 serialization tests
    ├── test_api.py                  # 31 FastAPI endpoint tests
    └── fixtures/
        └── sample_transactions.csv  # Pre-generated 10K row fixture
```

---

## Key Domain Concepts

| Term | Definition |
|------|-----------|
| Blue/Jobber Price | List price before any discounts |
| Deductions | Off-invoice discounts, allowances, freight |
| Invoice Price | Blue Price − Deductions |
| Bonuses | Rebates, volume incentives |
| Pocket Price | Invoice Price − Bonuses (actual cash received) |
| Standard Cost | Fully loaded manufacturing cost per unit |
| Contribution Margin | Pocket Price − Standard Cost |
| Realization % | Pocket Price / Blue Price × 100 |
| Leakage % | (Blue − Pocket) / Blue × 100 |
| PSO | Primary Steering Organization (business unit) |

## Key Formulas

```
invoice_price       = blue_jobber_price - deductions
pocket_price        = invoice_price - bonuses
contribution_margin = pocket_price - standard_cost
margin_pct          = contribution_margin / pocket_price × 100
deduction_pct       = deductions / blue_jobber_price × 100
bonus_pct           = bonuses / invoice_price × 100
realization_pct     = pocket_price / blue_jobber_price × 100
leakage_pct         = (blue_jobber_price - pocket_price) / blue_jobber_price × 100
```

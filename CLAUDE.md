# Price-to-Margin Waterfall Intelligence Agent

## Project Overview
An AI-powered pricing intelligence agent for the filtration industry that transforms raw transactional data in Snowflake into interactive waterfall analytics, natural language diagnostics, and proactive margin alerts. The agent decomposes the full price journey: Blue/Jobber Price → Deductions → Invoice Price → Bonuses → Pocket Price → Standard Cost → Contribution Margin.

## Data Strategy
This project uses **synthetic generated data** — no real ERP or Snowflake production data is used. The generator (`scripts/seed_sample_data.py`) produces realistic filtration-industry transactions seeded for reproducibility.

**Synthetic data design:**
- 8 materials across 4 categories: Mobile Hydraulics, Industrial Air, Process Filtration, Dust Collection / Engine
- 13 customers across 3 PSOs (Americas, EMEA, APAC) with 4 margin archetypes:
  - `premium` → margin ~40–50% (Tier 1)
  - `healthy` → margin ~28–35% (Tier 2)
  - `low` → margin ~16–22% (Tiers 3–4)
  - `destructive` → margin < 6% (Tiers 4–5)
- Deliberate outlier mix per run: ~5% high-deduction rows, ~3% critical-margin rows, ~1% DQ errors (material_cost > standard_cost)
- Country price multipliers applied (USA 1.0×, Germany 1.15×, Brazil 0.88×, China 0.75×, India 0.72×)
- Default: 10,000 rows, seed=42; override with `--n` and `--seed` flags

**Data pipeline (local dev flow):**
1. `python scripts/seed_sample_data.py` → generates `tests/fixtures/sample_transactions.csv`
2. Run `scripts/setup_snowflake.sql` in Snowsight to provision warehouse + schemas + `RAW_TRANSACTIONS` table
3. `python scripts/load_to_snowflake.py` → uploads CSV to `PRICING_DB.BRONZE.RAW_TRANSACTIONS` via `write_pandas`
4. `dbt run` → populates Silver and Gold layers from Bronze

## Build Progress

### Completed
- [x] **Snowflake DDL** (`scripts/setup_snowflake.sql`) — warehouse (PRICING_WH XS), database (PRICING_DB), three schemas (BRONZE / SILVER / GOLD), `RAW_TRANSACTIONS` table, internal CSV stage
- [x] **Synthetic data generator** (`scripts/seed_sample_data.py`) — full 15-field schema, 4 archetypes, outlier mix, reproducible seed, post-generation validation assertions
- [x] **Snowflake loader** (`scripts/load_to_snowflake.py`) — reads CSV, enforces dtypes, uploads via `write_pandas`, verifies row count; supports `--truncate` for idempotent re-runs
- [x] **dbt Bronze model** (`dbt/models/bronze/raw_transactions.sql`) — source passthrough
- [x] **dbt Silver model** (`dbt/models/silver/stg_clean_transactions.sql`) — deduplication and validation
- [x] **dbt Gold models** — `waterfall_fact.sql` (all derived metrics + clustering), `customer_profitability.sql` (margin tiers), `waterfall_alerts.sql` (anomaly detection)
- [x] **dbt Gold schema tests** (`dbt/models/gold/schema.yml`)
- [x] **Snowflake connection manager** (`src/snowflake/connection.py`) — singleton `SnowflakeConnectionManager`, env-based config via `pydantic-settings`, `execute_query` returns DataFrame
- [x] **Query templates** (`src/snowflake/queries.py`) — `build_waterfall_query` (dynamic filter builder, SQL injection safe), `CUSTOMER_PROFITABILITY_QUERY`, `ACTIVE_ALERTS_QUERY`
- [x] **Waterfall computation engine** (`src/analytics/waterfall.py`) — `compute_waterfall` with volume-weighted averages, `apply_filters`, all 9 derived metrics, zero-qty guard, division-by-zero safe
- [x] **Unit tests for waterfall** (`tests/test_waterfall.py`) — 8 tests with known-answer inputs (unfiltered, country filter, PSO filter, margin math, empty result, zero qty, missing columns, revenue calculation)
- [x] **Sample fixture** (`tests/fixtures/sample_transactions.csv`) — pre-generated 10K rows for offline testing
- [x] **Outlier detection** (`src/analytics/outliers.py`) — z-score per metric within material × country × volume_band peers; direction-aware (low_is_bad / high_is_bad); MIN_PEER_SIZE guard; `OutlierFlag` dataclass + `summarize_outliers`
- [x] **Unit tests for outliers** (`tests/test_outliers.py`) — 12 tests: volume band assignment, clean data no flags, high-deduction flagged, low-margin flagged, missing columns raises, zero qty excluded, small peer group skipped, severity levels, type check, summarize empty, summarize sorted by |z|
- [x] **YoY margin bridge** (`src/analytics/trends.py`) — decomposes total_margin_change into price / deduction / bonus / cost / volume / mix effects; all effects sum to total (within float tolerance); `PeriodMetrics` + `MarginBridge` dataclasses
- [x] **Unit tests for trends** (`tests/test_trends.py`) — 13 tests: correct types, known values, missing year returns None, missing column raises, zero qty excluded, transaction count, bridge math, price-only effect, effects sum, volume effect, negative effects
- [x] **AI narrative generation** (`src/analytics/narratives.py`) — `generate_narrative(waterfall, outliers, bridge)` via `claude-sonnet-4-6`; stable system prompt cached with `cache_control: ephemeral`; top-5 outliers by |z| serialized to JSON payload; `pydantic-settings` for API key
- [x] **Unit tests for narratives** (`tests/test_narratives.py`) — 12 tests: payload serialization, top-5 truncation, correct model used, cache_control present, API key passed, message role, optional params behavior
- [x] **Intent parser** (`src/agent/intent_parser.py`) — `parse_intent(query)` via Claude forced tool use; `ParsedIntent` dataclass (action, WaterfallFilters, base_year, current_year, raw_query); resolves relative time references via dynamic system prompt
- [x] **Analysis orchestrator** (`src/agent/orchestrator.py`) — `run_analysis(df, intent)` routes waterfall / outliers / trends / narrative / full_analysis pipeline; `AnalysisResult` dataclass; graceful error on no-data filters
- [x] **Response formatter** (`src/agent/response_formatter.py`) — `format_response(result)` → JSON-serializable dict with status key; `format_error(message)` for error responses
- [x] **Unit tests for agent layer** (`tests/test_intent_parser.py`, `tests/test_orchestrator.py`, `tests/test_response_formatter.py`) — 40 tests across tool schema validation, filter extraction, pipeline routing, error handling, JSON serialization
- [x] **SQL agent** (`src/agent/sql_agent.py`) — agentic tool-use loop (Chapter 6 pattern from agent.qmd); `SqlAgent.ask(question)` runs list_tables / describe_table / run_sql tools via Claude until a final answer is reached; two backends: DuckDB (offline CSV) and Snowflake GOLD; system prompt prompt-cached; `SqlQueryResult` dataclass
- [x] **Terminal chatbot** (`scripts/chat.py`) — SQL-agent-powered REPL; any NL question → SQL → natural language answer; shows SQL queries executed and timing; Snowflake GOLD fallback to DuckDB offline
- [x] **FastAPI models** (`src/api/models.py`) — `WaterfallModel`, `OutlierModel`, `PeriodModel`, `BridgeModel`, `ChatRequest`, `AnalysisResponse`, `HealthResponse`
- [x] **FastAPI routes** (`src/api/routes.py`) — `GET /api/health`, `POST /api/chat`, `GET /api/waterfall`, `GET /api/outliers`, `GET /api/trends`; unified `AnalysisResponse` envelope with `elapsed_ms`
- [x] **FastAPI app** (`src/api/main.py`) — lifespan data loader (Snowflake → CSV fallback), CORS middleware, global exception handler; `uv run uvicorn src.api.main:app --reload`
- [x] **API tests** (`tests/test_api.py`) — 31 tests: health, chat (mock parse_intent), direct waterfall/outliers/trends, 422 validation, error propagation; 116 total tests all passing

- [x] **React frontend** (`frontend/`) — Vite + React 18 + Recharts + Tailwind CSS; split-pane layout (dashboard left, chat right); build verified
  - `FilterBar.jsx` — country / PSO / year / material dropdowns; Apply + Reset
  - `WaterfallChart.jsx` — floating-bar waterfall (Recharts), metric pills, per-step colour coding
  - `CustomerTable.jsx` — outlier table sorted by |z|, HIGH/MEDIUM severity badges, show-all toggle
  - `AgentChat.jsx` — NL chat with suggestion chips, action badges, metric grid, bridge summary, narrative text, typing indicator; pushes waterfall data to dashboard on response
  - `App.jsx` — split-pane layout, filter-driven `GET /api/*` calls, chat → dashboard bridge, `GET /api/health` status in header

### Up Next
- [ ] CI/CD — GitHub Actions workflow (lint → test → build)
- [ ] Deployment — Dockerfile + docker-compose for local full-stack run

## Agent Architecture — Two Modes

### Terminal chat (`scripts/chat.py`) — SQL Agent
The terminal REPL uses `SqlAgent` (agentic tool-use loop, no fixed pipeline):
```
user question
  → Claude calls list_tables / describe_table / run_sql (multiple turns)
  → SQL executes (DuckDB for CSV, Snowflake GOLD for live)
  → Claude interprets results
  → plain English answer + SQL shown in terminal
```
This is the generic, open-ended path — any question the data can answer gets answered.

### Browser UI (`/api/chat`) — Structured Pipeline
The FastAPI `/api/chat` endpoint still uses the intent parser → orchestrator pipeline:
```
user question
  → parse_intent (forced tool use → structured action + filters)
  → run_analysis (waterfall / outliers / trends / full_analysis)
  → generate_narrative (optional)
  → JSON response driving React charts + metric grid
```
This structured path exists because the React dashboard needs typed data
(WaterfallResult, OutlierFlag[], MarginBridge) to render charts — raw SQL
results cannot drive Recharts directly.

Both modes use `claude-sonnet-4-6` with prompt caching on stable system prompts.

## Tech Stack
- **Data Warehouse:** Snowflake (Enterprise edition)
- **Transformations:** dbt-snowflake (models in `/dbt/`)
- **Backend:** Python 3.11+, FastAPI, Pydantic
- **AI Engine:** Anthropic Claude API (`claude-sonnet-4-6`)
- **Frontend:** React 18, Recharts, Tailwind CSS
- **Visualization:** Power BI (connects to Snowflake Gold layer)
- **Infrastructure:** AWS (ECS Fargate, ElastiCache Redis, CloudWatch)
- **CI/CD:** GitHub Actions (no workflow created yet)
- **SQL Engine (offline):** DuckDB (queries pandas DataFrame in-memory for CSV fallback)
- **Package Management:** uv + hatchling (Python), npm (frontend)

## Domain Terminology
- **Blue/Jobber Price:** List or catalog price before any discounts
- **Deductions:** Off-invoice discounts, allowances, freight deductions
- **Invoice Price (Gross Price):** Blue price minus deductions
- **Bonuses:** Rebates, volume incentives, loyalty payments
- **Pocket Price (Net Price):** Invoice price minus bonuses — the actual cash received
- **Standard Cost:** Fully loaded manufacturing cost per unit
- **Material Cost:** Raw material component of standard cost (subset)
- **Contribution Margin:** Pocket price minus standard cost
- **Price Realization:** Pocket price as % of blue price (measures leakage)
- **PSO:** Primary Steering Organization — the business unit structure
- **Waterfall:** The step-by-step decomposition from list price to margin

## Data Schema (15 Source Fields)
All data lives in Snowflake. The source table is `PRICING_DB.BRONZE.RAW_TRANSACTIONS`.

| # | Field | Type | Role |
|---|-------|------|------|
| 1 | country | VARCHAR | Dimension |
| 2 | year | INTEGER | Time dimension |
| 3 | material | VARCHAR | Product dimension |
| 4 | sales_designation | VARCHAR | Channel dimension |
| 5 | sold_to | VARCHAR | Customer ID |
| 6 | corporate_group | VARCHAR | Parent company grouping |
| 7 | sales_qty | DECIMAL(18,4) | Volume weighting factor |
| 8 | blue_jobber_price | DECIMAL(18,4) | Waterfall start |
| 9 | deductions | DECIMAL(18,4) | Step 1 (subtract) |
| 10 | invoice_price | DECIMAL(18,4) | Subtotal 1 |
| 11 | bonuses | DECIMAL(18,4) | Step 2 (subtract) |
| 12 | pocket_price | DECIMAL(18,4) | Subtotal 2 |
| 13 | pso | VARCHAR | Org structure dimension |
| 14 | standard_cost | DECIMAL(18,4) | Step 3 (subtract) |
| 15 | material_cost | DECIMAL(18,4) | Cost decomposition |

## Key Formulas
```
invoice_price = blue_jobber_price - deductions
pocket_price = invoice_price - bonuses
contribution_margin = pocket_price - standard_cost
margin_pct = contribution_margin / pocket_price * 100
deduction_pct = deductions / blue_jobber_price * 100
bonus_pct = bonuses / invoice_price * 100
realization_pct = pocket_price / blue_jobber_price * 100
leakage_pct = (blue_jobber_price - pocket_price) / blue_jobber_price * 100
conversion_cost = standard_cost - material_cost
```

## Snowflake Schema (Medallion Architecture)
- `PRICING_DB.BRONZE.RAW_TRANSACTIONS` — Raw ERP load, append-only
- `PRICING_DB.SILVER.CLEAN_TRANSACTIONS` — Deduplicated, validated, currency-normalized
- `PRICING_DB.GOLD.WATERFALL_FACT` — Pre-computed waterfall metrics
- `PRICING_DB.GOLD.CUSTOMER_PROFITABILITY` — Aggregated customer margin tiers
- `PRICING_DB.GOLD.WATERFALL_ALERTS` — Proactive anomaly alerts

## Coding Standards
- **Python:** Type hints required on all functions. Use Pydantic models for all request/response schemas. Follow Google Python Style Guide.
- **SQL (dbt):** Use CTEs over subqueries. Explicit column names (no SELECT *). Snake_case for all identifiers. Every model must have a .yml schema file with tests.
- **React:** Functional components with hooks only. No class components. Tailwind for styling.
- **Testing:** Every analytical function needs unit tests with known-answer inputs. Use pytest. Minimum 90% coverage on `src/analytics/`.
- **Git:** Conventional commits (feat:, fix:, docs:, test:). Feature branches off `develop`. PRs require 1 approval.

## File Structure
```
waterfall-agent/
├── CLAUDE.md                    # This file — project context for Claude Code
├── README.md                    # Setup and usage guide
├── pyproject.toml               # Python dependencies (uv / hatchling)
├── .env.example                 # Environment variable template
├── .github/workflows/           # CI/CD (not created yet)
├── Dockerfile                   # Backend container
├── docker-compose.yml           # Local development
├── src/
│   ├── snowflake/
│   │   ├── connection.py        # Connection pool management
│   │   └── queries.py           # Parameterized query templates
│   ├── analytics/
│   │   ├── waterfall.py         # Core waterfall computation
│   │   ├── outliers.py          # Statistical outlier detection
│   │   ├── trends.py            # Period-over-period analysis
│   │   └── narratives.py        # AI narrative generation
│   ├── agent/
│   │   ├── intent_parser.py     # NL query interpretation
│   │   ├── orchestrator.py      # Multi-step analysis coordination
│   │   └── response_formatter.py
│   └── api/
│       ├── main.py              # FastAPI app entry point
│       ├── routes.py            # API endpoints
│       └── models.py            # Pydantic schemas
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── bronze/              # Raw ingestion models
│       ├── silver/              # Cleaned models
│       └── gold/                # Analytics-ready models
├── frontend/
│   └── src/
│       └── components/
│           ├── WaterfallChart.jsx
│           ├── AgentChat.jsx
│           ├── CustomerTable.jsx
│           └── FilterBar.jsx
├── tests/
│   ├── test_waterfall.py
│   ├── test_outliers.py
│   └── test_narratives.py
├── scripts/
│   └── seed_sample_data.py      # Generate sample data for development
└── docs/
    └── architecture.md          # System architecture documentation
```

## Edge Cases to Handle
- Division by zero when pocket_price = 0 or blue_jobber_price = 0
- Negative margins (pocket_price < standard_cost)
- Null cost fields (standard_cost or material_cost missing)
- Zero-quantity transactions (exclude from weighted averages)
- Material cost > standard cost (data quality error — flag, don't crash)
- Currency normalization across countries

## Performance Targets
- Waterfall query response: < 2 seconds for any filter combination
- Agent natural language response: < 10 seconds end-to-end
- Dashboard page load: < 3 seconds
- Cache TTL: 15 minutes for standard queries, 5 minutes for alerts

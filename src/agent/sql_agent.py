"""SQL-based Q&A agent implementing the agentic tool-use loop.

Pattern (from agent.qmd Chapter 6, adapted for native Anthropic SDK):
    user question
    → Claude writes SQL using the schema embedded in the system prompt
    → run_sql executes it (DuckDB offline or Snowflake GOLD)
    → Claude interprets the result rows
    → natural language answer

The full table schema is injected into the system prompt at init time so Claude
never needs discovery tool calls (list_tables / describe_table). Most questions
resolve in exactly ONE run_sql call — two loop iterations total.

Two execution backends selected automatically:
    Snowflake — queries PRICING_DB.GOLD.WATERFALL_FACT, CUSTOMER_PROFITABILITY,
                WATERFALL_ALERTS (pre-computed by dbt, all derived metrics present)
    DuckDB    — queries the pandas DataFrame in-memory; a waterfall_fact view
                with all derived metrics is created automatically
"""

from __future__ import annotations

from dataclasses import dataclass, field

import anthropic
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict

_MODEL = "claude-sonnet-4-6"
_MAX_TURNS = 10         # each SQL call = 1 turn; budget covers retries + final answer
_MAX_RESULT_ROWS = 30


class SqlAgentSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    anthropic_api_key: str


# ── Schema definitions (embedded into system prompt at init time) ─────────────

_DUCKDB_SCHEMA = """\
## Available Tables (DuckDB — offline CSV mode)

### transactions  — raw source data (15 fields)
| Column             | Type    | Description |
|--------------------|---------|-------------|
| country            | VARCHAR | USA, Germany, Brazil, China, India |
| year               | INTEGER | 2024 or 2025 |
| material           | VARCHAR | HYD-001, IND-AIR-001, PROC-001, DUST-001 |
| sales_designation  | VARCHAR | Sales channel designation |
| sold_to            | VARCHAR | Customer ID |
| corporate_group    | VARCHAR | Parent company grouping |
| pso                | VARCHAR | Americas, EMEA, APAC |
| sales_qty          | DOUBLE  | Volume — ALWAYS use as weighting factor |
| blue_jobber_price  | DOUBLE  | List price per unit |
| deductions         | DOUBLE  | Off-invoice discounts |
| invoice_price      | DOUBLE  | blue_jobber_price minus deductions |
| bonuses            | DOUBLE  | Post-invoice rebates |
| pocket_price       | DOUBLE  | Actual cash received |
| standard_cost      | DOUBLE  | Fully loaded manufacturing cost |
| material_cost      | DOUBLE  | Raw material component of standard cost |

### waterfall_fact  — view over transactions with derived metrics (PREFER THIS)
All columns from `transactions` plus:
| Column              | Type   | Formula |
|---------------------|--------|---------|
| contribution_margin | DOUBLE | pocket_price - standard_cost |
| margin_pct          | DOUBLE | (pocket_price - standard_cost) / pocket_price * 100 |
| deduction_pct       | DOUBLE | deductions / blue_jobber_price * 100 |
| bonus_pct           | DOUBLE | bonuses / invoice_price * 100 |
| realization_pct     | DOUBLE | pocket_price / blue_jobber_price * 100 |
| leakage_pct         | DOUBLE | (blue_jobber_price - pocket_price) / blue_jobber_price * 100 |
Already filtered to sales_qty > 0. Use this table for all margin/deduction analysis."""

_SNOWFLAKE_SCHEMA = """\
## Available Tables (Snowflake GOLD — prefix: PRICING_DB.GOLD.<table>)

### WATERFALL_FACT  — pre-computed waterfall metrics per transaction (USE THIS FIRST)
| Column              | Type    | Description |
|---------------------|---------|-------------|
| country             | VARCHAR | USA, Germany, Brazil, China, India |
| year                | INTEGER | 2024 or 2025 |
| material            | VARCHAR | HYD-001, IND-AIR-001, PROC-001, DUST-001 |
| sales_designation   | VARCHAR | Sales channel |
| sold_to             | VARCHAR | Customer ID |
| corporate_group     | VARCHAR | Parent company |
| pso                 | VARCHAR | Americas, EMEA, APAC |
| sales_qty           | DECIMAL | Volume — ALWAYS use as weighting factor |
| blue_jobber_price   | DECIMAL | List price per unit |
| deductions          | DECIMAL | Off-invoice discounts |
| invoice_price       | DECIMAL | blue_jobber_price minus deductions |
| bonuses             | DECIMAL | Post-invoice rebates |
| pocket_price        | DECIMAL | Actual cash received |
| standard_cost       | DECIMAL | Fully loaded manufacturing cost |
| material_cost       | DECIMAL | Raw material component of standard cost |
| contribution_margin | DECIMAL | pocket_price - standard_cost |
| margin_pct          | DECIMAL | contribution_margin / pocket_price * 100 |
| deduction_pct       | DECIMAL | deductions / blue_jobber_price * 100 |
| bonus_pct           | DECIMAL | bonuses / invoice_price * 100 |
| realization_pct     | DECIMAL | pocket_price / blue_jobber_price * 100 |
| leakage_pct         | DECIMAL | (blue_jobber_price - pocket_price) / blue_jobber_price * 100 |

### CUSTOMER_PROFITABILITY  — aggregated customer-level profitability
| Column               | Type    | Description |
|----------------------|---------|-------------|
| sold_to              | VARCHAR | Customer ID |
| corporate_group      | VARCHAR | Parent company |
| country              | VARCHAR | Market geography |
| pso                  | VARCHAR | Business unit |
| year                 | INTEGER | Year |
| total_qty            | DECIMAL | Total sales volume |
| total_pocket_revenue | DECIMAL | Total pocket revenue |
| total_margin_dollars | DECIMAL | Total contribution margin |
| wavg_margin_pct      | DECIMAL | Volume-weighted average margin % |
| wavg_realization_pct | DECIMAL | Volume-weighted average realization % |
| margin_tier          | VARCHAR | Tier 1 Premium / Tier 2 Healthy / Tier 3 Acceptable / Tier 4 Low / Tier 5 Destructive |

### WATERFALL_ALERTS  — active anomaly alerts from dbt
| Column           | Type      | Description |
|------------------|-----------|-------------|
| country          | VARCHAR   | Market geography |
| pso              | VARCHAR   | Business unit |
| year             | INTEGER   | Year |
| alert_type       | VARCHAR   | CRITICAL_MARGIN / LOW_MARGIN / HIGH_DEDUCTIONS / HIGH_BONUSES / LOW_REALIZATION |
| severity         | VARCHAR   | HIGH or MEDIUM |
| avg_margin_pct   | DECIMAL   | Avg margin % in flagged segment |
| avg_deduction_pct| DECIMAL   | Avg deduction % in flagged segment |
| avg_bonus_pct    | DECIMAL   | Avg bonus % in flagged segment |
| detected_at      | TIMESTAMP | When the alert was generated |"""

# ── Stable system prompt template ─────────────────────────────────────────────

_PROMPT_TEMPLATE = """\
You are a pricing intelligence agent for the filtration industry.
Answer every question by writing and running one SQL query, then explain the results
in plain business English — cite actual numbers from the query output.

{schema}

## Pricing Waterfall — Domain Knowledge
Blue Price → (minus Deductions) → Invoice Price → (minus Bonuses) → Pocket Price → (minus Standard Cost) → Contribution Margin

Volume-weighted average rule (CRITICAL — always apply for per-unit metrics):
  SUM(metric * sales_qty) / NULLIF(SUM(sales_qty), 0)

Margin benchmarks:
  > 35 %   Tier 1 Premium     (excellent)
  25–35 %  Tier 2 Healthy     (acceptable)
  15–25 %  Tier 3 Acceptable  (monitor)
   5–15 %  Tier 4 Low         (action required)
   < 5  %  Tier 5 Destructive (urgent intervention)

Terminology: sold_to = customer ID | PSO = Americas / EMEA / APAC | material = product code

## SQL Rules
1. Write ONE query that fully answers the question — do not run multiple exploratory queries.
2. Use waterfall_fact (DuckDB) or PRICING_DB.GOLD.WATERFALL_FACT (Snowflake) for most questions.
3. Default LIMIT 20 unless the user asks for more.
4. NEVER use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE.
5. If the query errors, rewrite it once and retry. After two failed attempts, explain what went wrong.
6. Once you have a successful result set, STOP calling run_sql and give your final answer immediately.
7. Your final answer must cite specific numbers from the query output."""


# ── Single tool: run_sql ──────────────────────────────────────────────────────

_TOOLS: list[dict] = [
    {
        "name": "run_sql",
        "description": (
            "Executes a SELECT SQL query against the pricing database and returns "
            "the result rows as a formatted table. SQL errors are returned as plain "
            "text so you can rewrite and retry."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A valid SELECT (or WITH…SELECT) SQL query.",
                }
            },
            "required": ["query"],
        },
    }
]

# DuckDB DDL run once per connection to expose the waterfall_fact view
_WATERFALL_FACT_DDL = """\
CREATE VIEW waterfall_fact AS
SELECT *,
    pocket_price - standard_cost AS contribution_margin,
    CASE WHEN pocket_price > 0
         THEN (pocket_price - standard_cost) / pocket_price * 100
         ELSE 0.0 END AS margin_pct,
    CASE WHEN blue_jobber_price > 0
         THEN deductions / blue_jobber_price * 100
         ELSE 0.0 END AS deduction_pct,
    CASE WHEN invoice_price > 0
         THEN bonuses / invoice_price * 100
         ELSE 0.0 END AS bonus_pct,
    CASE WHEN blue_jobber_price > 0
         THEN pocket_price / blue_jobber_price * 100
         ELSE 0.0 END AS realization_pct,
    CASE WHEN blue_jobber_price > 0
         THEN (blue_jobber_price - pocket_price) / blue_jobber_price * 100
         ELSE 0.0 END AS leakage_pct
FROM transactions
WHERE sales_qty > 0"""


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class SqlQueryResult:
    """Output from a single SqlAgent.ask() call."""

    answer: str
    sql_calls: list[str] = field(default_factory=list)
    error: str | None = None


# ── Agent ─────────────────────────────────────────────────────────────────────

class SqlAgent:
    """Agentic SQL Q&A over the pricing database.

    Schema is embedded in the system prompt at init time — no discovery tool calls.
    Typical flow: user question → run_sql (1 call) → plain English answer (2 iterations).

    Args:
        df:            Pandas DataFrame for DuckDB / offline mode.
        snowflake_mgr: SnowflakeConnectionManager for live Snowflake queries.
                       One of the two must be provided.
    """

    def __init__(
        self,
        df: pd.DataFrame | None = None,
        snowflake_mgr=None,
    ) -> None:
        if df is None and snowflake_mgr is None:
            raise ValueError("Provide either df (DuckDB mode) or snowflake_mgr (Snowflake mode).")
        self._df = df
        self._mgr = snowflake_mgr
        self._use_snowflake = snowflake_mgr is not None

        schema = _SNOWFLAKE_SCHEMA if self._use_snowflake else _DUCKDB_SCHEMA
        self._system_prompt = _PROMPT_TEMPLATE.format(schema=schema)
        self._client: anthropic.Anthropic | None = None  # lazy — created on first ask()

    # ── SQL execution backends ────────────────────────────────────────────────

    def _exec_snowflake(self, query: str) -> str:
        try:
            df = self._mgr.execute_query(query)
            if df.empty:
                return "Query returned 0 rows."
            return df.head(_MAX_RESULT_ROWS).to_string(index=False)
        except Exception as exc:
            return f"SQL Error: {exc}"

    def _exec_duckdb(self, query: str) -> str:
        try:
            import duckdb
        except ImportError:
            return "DuckDB not installed. Run: uv sync"

        try:
            con = duckdb.connect()
            con.register("transactions", self._df)
            con.execute(_WATERFALL_FACT_DDL)
            result_df = con.execute(query).fetchdf()
            con.close()
            if result_df.empty:
                return "Query returned 0 rows."
            return result_df.head(_MAX_RESULT_ROWS).to_string(index=False)
        except Exception as exc:
            return f"SQL Error: {exc}"

    def _run_sql(self, query: str) -> str:
        q = query.strip()
        first_word = q.split()[0].upper() if q else ""
        if first_word not in ("SELECT", "WITH", "EXPLAIN"):
            return (
                "Error: Only SELECT (or WITH … SELECT) queries are permitted. "
                "Rewrite without DML statements."
            )
        return self._exec_snowflake(q) if self._use_snowflake else self._exec_duckdb(q)

    # ── Agentic loop ──────────────────────────────────────────────────────────

    def ask(self, question: str) -> SqlQueryResult:
        """Answer a natural language question about the pricing database.

        Args:
            question: Free-form user question.

        Returns:
            SqlQueryResult with the plain-English answer and SQL queries executed.
        """
        if self._client is None:
            settings = SqlAgentSettings()
            self._client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        messages: list[dict] = [{"role": "user", "content": question}]
        sql_calls: list[str] = []

        for _ in range(_MAX_TURNS):
            response = self._client.messages.create(
                model=_MODEL,
                max_tokens=4096,
                system=[
                    {
                        "type": "text",
                        "text": self._system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                tools=_TOOLS,
                messages=messages,
            )

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                answer = next(
                    (b.text for b in response.content if hasattr(b, "text") and b.text),
                    "No answer generated.",
                )
                return SqlQueryResult(answer=answer, sql_calls=sql_calls)

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                query = block.input.get("query", "")
                sql_calls.append(query)
                result = self._run_sql(query)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    }
                )

            messages.append({"role": "user", "content": tool_results})

        return SqlQueryResult(
            answer="Could not answer within the allowed steps. Try a more specific question.",
            sql_calls=sql_calls,
            error="max_turns_exceeded",
        )

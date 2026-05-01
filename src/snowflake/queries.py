"""Parameterized Snowflake query templates for the waterfall agent.

All queries use parameterized inputs (%s placeholders) to prevent SQL injection.
Queries are read-only and scoped to the PRICING_DB.GOLD schema.
"""

from dataclasses import dataclass


@dataclass
class WaterfallQueryParams:
    """Parameters for waterfall data retrieval."""

    country: str | None = None
    year: int | None = None
    material: str | None = None
    pso: str | None = None
    corporate_group: str | None = None
    sold_to: str | None = None
    limit: int = 100_000


def build_waterfall_query(params: WaterfallQueryParams) -> tuple[str, list]:
    """Build a parameterized query for waterfall_fact with dynamic filters."""
    where_clauses: list[str] = []
    query_params: list = []

    filter_map = {
        "country": params.country,
        "year": params.year,
        "material": params.material,
        "pso": params.pso,
        "corporate_group": params.corporate_group,
        "sold_to": params.sold_to,
    }

    for col, value in filter_map.items():
        if value is not None:
            where_clauses.append(f"{col} = %s")
            query_params.append(value)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    query = f"""
        SELECT
            country, year, material, sales_designation,
            sold_to, corporate_group, pso, sales_qty,
            blue_jobber_price, deductions, invoice_price,
            bonuses, pocket_price, standard_cost, material_cost,
            contribution_margin, margin_pct, deduction_pct,
            bonus_pct, realization_pct, leakage_pct
        FROM PRICING_DB.GOLD.WATERFALL_FACT
        {where_sql}
        LIMIT {params.limit}
    """

    return query.strip(), query_params


CUSTOMER_PROFITABILITY_QUERY = """
    SELECT sold_to, corporate_group, country, pso, year,
           total_qty, total_pocket_revenue, total_margin_dollars,
           wavg_margin_pct, wavg_realization_pct, margin_tier
    FROM PRICING_DB.GOLD.CUSTOMER_PROFITABILITY
    ORDER BY wavg_margin_pct ASC
"""

ACTIVE_ALERTS_QUERY = """
    SELECT country, pso, year, alert_type, severity,
           avg_margin_pct, avg_deduction_pct, avg_bonus_pct, detected_at
    FROM PRICING_DB.GOLD.WATERFALL_ALERTS
    ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, detected_at DESC
"""

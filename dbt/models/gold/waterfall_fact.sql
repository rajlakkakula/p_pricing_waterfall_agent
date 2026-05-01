{{
    config(
        materialized='table',
        cluster_by=['country', 'pso', 'year']
    )
}}

with clean_transactions as (
    select * from {{ ref('stg_clean_transactions') }}
    where sales_qty > 0
      and blue_jobber_price > 0
),

waterfall_metrics as (
    select
        -- Dimensions
        country,
        year,
        material,
        sales_designation,
        sold_to,
        corporate_group,
        pso,

        -- Base measures
        sales_qty,
        blue_jobber_price,
        deductions,
        invoice_price,
        bonuses,
        pocket_price,
        standard_cost,
        material_cost,

        -- Derived waterfall metrics
        pocket_price - standard_cost as contribution_margin,

        round(
            (pocket_price - standard_cost) / nullif(pocket_price, 0) * 100, 2
        ) as margin_pct,

        round(
            deductions / nullif(blue_jobber_price, 0) * 100, 2
        ) as deduction_pct,

        round(
            bonuses / nullif(invoice_price, 0) * 100, 2
        ) as bonus_pct,

        round(
            pocket_price / nullif(blue_jobber_price, 0) * 100, 2
        ) as realization_pct,

        round(
            (blue_jobber_price - pocket_price) / nullif(blue_jobber_price, 0) * 100, 2
        ) as leakage_pct,

        standard_cost - material_cost as conversion_cost,

        -- Revenue and margin dollars
        pocket_price * sales_qty as pocket_revenue,
        (pocket_price - standard_cost) * sales_qty as margin_dollars

    from clean_transactions
)

select * from waterfall_metrics

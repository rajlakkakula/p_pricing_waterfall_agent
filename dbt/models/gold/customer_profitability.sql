{{
    config(
        materialized='table',
        cluster_by=['pso', 'year']
    )
}}

with waterfall as (
    select * from {{ ref('waterfall_fact') }}
),

customer_agg as (
    select
        sold_to,
        corporate_group,
        country,
        pso,
        year,

        sum(sales_qty) as total_qty,
        sum(pocket_revenue) as total_pocket_revenue,
        sum(margin_dollars) as total_margin_dollars,

        round(
            sum(margin_dollars) / nullif(sum(pocket_revenue), 0) * 100, 2
        ) as wavg_margin_pct,

        round(
            sum(pocket_revenue) / nullif(sum(blue_jobber_price * sales_qty), 0) * 100, 2
        ) as wavg_realization_pct,

        round(
            sum(deductions * sales_qty) / nullif(sum(blue_jobber_price * sales_qty), 0) * 100, 2
        ) as wavg_deduction_pct,

        round(
            sum(bonuses * sales_qty) / nullif(sum(invoice_price * sales_qty), 0) * 100, 2
        ) as wavg_bonus_pct,

        count(*) as transaction_count

    from waterfall
    group by 1, 2, 3, 4, 5
),

tiered as (
    select
        *,
        case
            when wavg_margin_pct > 35 then 'Tier 1 - Premium'
            when wavg_margin_pct > 25 then 'Tier 2 - Healthy'
            when wavg_margin_pct > 15 then 'Tier 3 - Acceptable'
            when wavg_margin_pct > 5  then 'Tier 4 - Low'
            else 'Tier 5 - Destructive'
        end as margin_tier
    from customer_agg
)

select * from tiered

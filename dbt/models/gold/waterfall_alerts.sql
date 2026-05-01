{{
    config(
        materialized='table',
    )
}}

-- Proactive alerts: flagged when metrics breach thresholds
-- This table is consumed by the agent's alerting pipeline

with current_period as (
    select
        country,
        pso,
        year,
        round(avg(deduction_pct), 2) as avg_deduction_pct,
        round(avg(bonus_pct), 2)     as avg_bonus_pct,
        round(avg(margin_pct), 2)    as avg_margin_pct,
        round(avg(realization_pct), 2) as avg_realization_pct,
        count(*) as transaction_count
    from {{ ref('waterfall_fact') }}
    group by 1, 2, 3
),

alerts as (
    select
        country,
        pso,
        year,
        avg_deduction_pct,
        avg_bonus_pct,
        avg_margin_pct,
        avg_realization_pct,
        transaction_count,

        -- Alert type classification
        case
            when avg_margin_pct < 10
                then 'CRITICAL_MARGIN'
            when avg_margin_pct < 20
                then 'LOW_MARGIN'
            when avg_deduction_pct > 18
                then 'HIGH_DEDUCTIONS'
            when avg_bonus_pct > 8
                then 'HIGH_BONUSES'
            when avg_realization_pct < 70
                then 'LOW_REALIZATION'
            else null
        end as alert_type,

        -- Severity
        case
            when avg_margin_pct < 10 then 'HIGH'
            when avg_margin_pct < 20 then 'MEDIUM'
            when avg_deduction_pct > 18 then 'HIGH'
            when avg_bonus_pct > 8 then 'MEDIUM'
            when avg_realization_pct < 70 then 'MEDIUM'
            else null
        end as severity,

        current_timestamp() as detected_at

    from current_period
)

select * from alerts where alert_type is not null

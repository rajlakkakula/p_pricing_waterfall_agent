{{
    config(
        materialized='table',
        cluster_by=['country', 'year']
    )
}}

with source as (
    select * from {{ ref('src_raw_transactions') }}
),

-- Step 1: Deduplicate on natural key
deduped as (
    select *,
        row_number() over (
            partition by material, sold_to, year, country, sales_designation
            order by _loaded_at desc
        ) as row_num
    from source
    where sales_qty is not null
      and sales_qty > 0
      and blue_jobber_price is not null
      and blue_jobber_price > 0
),

-- Step 2: Validate waterfall arithmetic
validated as (
    select
        country,
        year,
        material,
        sales_designation,
        sold_to,
        corporate_group,
        sales_qty,
        blue_jobber_price,
        deductions,
        invoice_price,
        bonuses,
        pocket_price,
        pso,
        standard_cost,
        -- Use coalesce to handle null material_cost
        coalesce(material_cost, 0) as material_cost,

        -- Data quality flags (don't drop bad rows, flag them)
        case
            when abs(invoice_price - (blue_jobber_price - deductions)) > 0.01
            then 'INVOICE_MISMATCH'
            when abs(pocket_price - (invoice_price - bonuses)) > 0.01
            then 'POCKET_MISMATCH'
            when material_cost > standard_cost
            then 'MATERIAL_COST_EXCEEDS_STANDARD'
            when standard_cost <= 0
            then 'ZERO_OR_NEGATIVE_COST'
            when pocket_price < 0
            then 'NEGATIVE_POCKET_PRICE'
            else 'VALID'
        end as data_quality_flag

    from deduped
    where row_num = 1
)

select * from validated

{{ config(materialized='view') }}

-- Bronze layer: raw ingestion view over source table
-- No transformations — this is the exact data as loaded from ERP
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
    material_cost,
    _loaded_at   -- ETL load timestamp
from {{ source('bronze', 'raw_transactions') }}

with source as (
    select * from main.stg_order_line_versions
)
select
    order_id,
    line_id,
    product_name,
    sku,
    quantity,
    unit_amount,
    line_amount,
    snapshot_id
from source

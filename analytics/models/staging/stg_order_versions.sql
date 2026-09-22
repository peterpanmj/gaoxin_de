with source as (
    select * from main.stg_order_versions
)
select
    order_id,
    order_number,
    channel,
    currency,
    status,
    created_at,
    updated_at,
    total_amount,
    payload_json,
    payload_hash,
    snapshot_id
from source

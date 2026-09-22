-- One source version must have one canonical payload, even across snapshots.
select order_id, updated_at
from {{ ref('stg_order_versions') }}
group by order_id, updated_at
having count(distinct payload_hash) > 1

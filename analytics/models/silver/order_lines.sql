select lines.*
from {{ ref('stg_order_line_versions') }} as lines
inner join {{ ref('orders') }} as orders
    on lines.order_id = orders.order_id
    and lines.snapshot_id = orders.snapshot_id
    and lines.payload_hash = orders.payload_hash

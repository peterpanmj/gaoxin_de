select
    cast(orders.created_at as date) as order_date,
    orders.channel,
    orders.currency,
    lines.sku,
    lines.product_name,
    sum(lines.quantity) as units_ordered,
    sum(lines.line_amount) as gross_amount
from {{ ref('order_lines') }} as lines
inner join {{ ref('orders') }} as orders using (order_id)
where orders.status not in ('CANCELED', 'DRAFT')
group by 1, 2, 3, 4, 5

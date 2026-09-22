select
    cast(created_at as date) as order_date,
    channel,
    currency,
    count(*) as order_count,
    sum(total_amount) as gross_amount,
    avg(total_amount) as average_order_value
from {{ ref('orders') }}
where status not in ('CANCELED', 'DRAFT')
group by 1, 2, 3

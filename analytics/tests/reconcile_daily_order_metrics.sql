with expected as (
    select
        cast(created_at as date) as order_date,
        channel,
        currency,
        count(*) as order_count,
        sum(total_amount) as gross_amount
    from {{ ref('orders') }}
    where status not in ('CANCELED', 'DRAFT')
    group by 1, 2, 3
), actual as (
    select order_date, channel, currency, order_count, gross_amount
    from {{ ref('daily_order_metrics') }}
)
select coalesce(expected.order_date, actual.order_date) as order_date
from expected full outer join actual using (order_date, channel, currency)
where expected.order_count is distinct from actual.order_count
   or expected.gross_amount is distinct from actual.gross_amount

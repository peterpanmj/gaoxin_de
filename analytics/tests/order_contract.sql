select order_id
from {{ ref('orders') }}
where updated_at < created_at
   or total_amount < 0
   or status not in ('DRAFT', 'UNCONFIRMED', 'UNFULFILLED', 'PARTIALLY_FULFILLED',
                    'FULFILLED', 'PARTIALLY_RETURNED', 'RETURNED', 'CANCELED',
                    'EXPIRED')

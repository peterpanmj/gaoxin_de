select line_id
from {{ ref('order_lines') }}
where quantity <= 0 or unit_amount < 0 or line_amount < 0

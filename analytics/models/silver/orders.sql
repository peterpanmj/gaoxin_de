with ranked as (
    select *,
        row_number() over (
            partition by order_id
            order by updated_at desc, snapshot_id desc, payload_json desc
        ) as version_rank
    from {{ ref('stg_order_versions') }}
)
select * exclude (version_rank)
from ranked
where version_rank = 1

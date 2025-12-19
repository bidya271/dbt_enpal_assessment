with dupes as (
  select deal_id, funnel_step, count(*) as cnt
  from {{ ref('int_pipedrive_deal_funnel_events') }}
  group by 1,2
  having count(*) > 1
)
select *
from dupes

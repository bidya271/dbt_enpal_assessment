with totals as (
  select month, sum(deals_count) as tot
  from {{ ref('rep_sales_funnel_monthly') }}
  group by 1
),
s as (
  select month, tot,
    lag(tot) over (order by month) as prev_tot,
    case when lag(tot) over (order by month) is null then 1 else tot::float / nullif(lag(tot) over (order by month), 0) end as growth_factor
  from totals
)
select *
from s
where growth_factor > 5  -- adjust threshold as acceptable

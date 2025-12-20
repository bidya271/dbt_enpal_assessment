with events as (
  select
    deal_id,
    funnel_step,
    step_entered_at::timestamptz at time zone 'utc' as event_ts_utc
  from {{ ref('fct_deal_funnel_events') }}
  where step_entered_at is not null
),

-- Bounds for date generation
bounds as (
  select
    coalesce(date_trunc('month', min(event_ts_utc)), date_trunc('month', now() - interval '12 months'))::date as min_month,
    coalesce(date_trunc('month', max(event_ts_utc)), date_trunc('month', now()))::date as max_month
  from events
),

months as (
  select generate_series(min_month, max_month, interval '1 month')::date as month
  from bounds
),

funnel_hierarchy as (
    select * from (values 
        (1, 'Lead Generation'),
        (2, 'Qualified Lead'),
        (3, 'Sales Call 1'), 
        (4, 'Needs Assessment'),
        (5, 'Sales Call 2'),
        (6, 'Proposal/Quote Preparation'),
        (7, 'Negotiation'),
        (8, 'Closing'),
        (9, 'Implementation/Onboarding'),
        (10, 'Follow-up/Customer Success'),
        (11, 'Renewal/Expansion')
    ) as t(step_order, funnel_step)
),

skeleton as (
    select
        m.month,
        fh.funnel_step,
        fh.step_order
    from months m
    cross join funnel_hierarchy fh
),

monthly_agg as (
  select
    date_trunc('month', e.event_ts_utc)::date as month,
    e.funnel_step,
    count(distinct e.deal_id) as deals_count
  from events e
  group by 1, 2
)

select
  s.month,
  'sales_funnel_deals_entered' as kpi_name,
  s.funnel_step,
  coalesce(ma.deals_count, 0) as deals_count
from skeleton s
left join monthly_agg ma
  on s.month = ma.month
  and s.funnel_step = ma.funnel_step
order by s.month desc, s.step_order asc

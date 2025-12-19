-- models/marts/rep_sales_funnel_monthly.sql
-- Purpose: Monthly aggregated funnel report: month, kpi_name, funnel_step, deals_count
-- Source: int_pipedrive_deal_funnel_events (canonical first-entry events)

with events as (
  select
    deal_id,
    funnel_step,
    step_entered_at::timestamptz at time zone 'utc' as event_ts_utc
  from {{ ref('int_pipedrive_deal_funnel_events') }}
  where step_entered_at is not null
),

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

monthly_agg as (
  select
    date_trunc('month', e.event_ts_utc)::date as month,
    e.funnel_step,
    count(distinct e.deal_id) as deals_count
  from events e
  group by 1, 2
)

select
  m.month,
  'sales_funnel_deals_entered' as kpi_name,
  coalesce(ma.funnel_step, 'no_step') as funnel_step,
  coalesce(ma.deals_count, 0) as deals_count
from months m
left join monthly_agg ma
  on ma.month = m.month
order by m.month desc, funnel_step

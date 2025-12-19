-- models/marts/rep_sales_funnel_monthly.sql
-- Purpose: Monthly aggregated funnel report: month, kpi_name, funnel_step, deals_count
-- Uses int_pipedrive_deal_funnel_events as the source of first-entry events.

with events as (

    select
        deal_id,
        funnel_step,
        step_entered_at
    from {{ ref('int_pipedrive_deal_funnel_events') }}

),

monthly_agg as (

    select
        date_trunc('month', step_entered_at)::date as month,
        funnel_step,
        count(distinct deal_id) as deals_count
    from events
    where step_entered_at is not null
    group by 1, 2

)

select
    month,
    'sales_funnel_deals_entered' as kpi_name,
    funnel_step,
    deals_count
from monthly_agg
order by month, funnel_step;
